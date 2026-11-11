package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/coreapi"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

type coreAdmission struct {
	AdmissionToken string    `json:"admission_token"`
	UserID         string    `json:"user_id"`
	EventID        string    `json:"event_id"`
	SlotID         string    `json:"slot_id"`
	MaxTickets     int       `json:"max_tickets"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// admissionBook plays core's POST /v1/core/admission: it keeps the latest token per slot.
type admissionBook struct {
	srv  *httptest.Server
	down atomic.Bool
	mu   sync.Mutex
	recs map[string]coreAdmission
}

func newAdmissionBook(t *testing.T) *admissionBook {
	t.Helper()
	b := &admissionBook{recs: map[string]coreAdmission{}}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var rec coreAdmission
		if r.Method != http.MethodPost || r.URL.Path != "/v1/core/admission" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("X-Service-Key") != "svc" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if json.NewDecoder(r.Body).Decode(&rec) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.mu.Lock()
		b.recs[rec.SlotID] = rec
		b.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"stored":true}`))
	}))
	t.Cleanup(b.srv.Close)
	return b
}

// With core down the user gets 503 and no token; once core is back the retry
// gets the same lease, and the token the user holds is the one core stored.
func TestClaimReturnsOnlyATokenCoreRecorded(t *testing.T) {
	client := testkit.Valkey(t)
	db := testkit.Postgres(t)
	cfg := fastController()
	cfg.HistWindow = time.Minute
	startControllerWith(t, client, db, cfg)
	time.Sleep(300 * time.Millisecond) // subscriptions

	book := newAdmissionBook(t)
	e := admissionEnv{newQueueEnvWith(t, client, queueConfig(time.Minute, time.Minute),
		coreapi.New(book.srv.URL, "svc", 200*time.Millisecond))}
	e.publish(t, "evt_reg", 10)
	d := e.join(t, "evt_reg", "w_reg1")

	book.down.Store(true)
	rec, _ := e.claimUntil(t, "evt_reg", d, 2*time.Second)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "2" ||
		problemCode(t, rec) != api.ErrorCodeADMISSIONUNAVAILABLE {
		t.Fatalf("claim with core down: %d %s", rec.Code, rec.Body)
	}

	book.down.Store(false)
	rec = e.claim("evt_reg", d)
	var adm api.Admission
	if rec.Code != http.StatusOK || json.NewDecoder(rec.Body).Decode(&adm) != nil {
		t.Fatalf("claim with core up: %d %s", rec.Code, rec.Body)
	}
	book.mu.Lock()
	stored, ok := book.recs[adm.SlotId]
	n := len(book.recs)
	book.mu.Unlock()
	if !ok || n != 1 || stored.AdmissionToken != adm.AdmissionToken || stored.SlotID != adm.SlotId ||
		stored.EventID != "evt_reg" || stored.MaxTickets != adm.ReservedUnits || !stored.ExpiresAt.Equal(adm.ExpiresAt) {
		t.Fatalf("core stored %+v (%d slots), user got %+v", stored, n, adm)
	}
}
