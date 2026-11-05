package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/coreapi"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// admissionBook plays core's registerAdmission: it keeps the latest token per slot.
type admissionBook struct {
	srv  *httptest.Server
	down atomic.Bool
	mu   sync.Mutex
	recs map[string]api.AdmissionRecord
}

func newAdmissionBook(t *testing.T) *admissionBook {
	t.Helper()
	b := &admissionBook{recs: map[string]api.AdmissionRecord{}}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var rec api.AdmissionRecord
		if r.Header.Get("Authorization") != "Bearer svc" || json.NewDecoder(r.Body).Decode(&rec) != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		b.mu.Lock()
		b.recs[strings.TrimPrefix(r.URL.Path, "/internal/admissions/")] = rec
		b.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
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
	if !ok || n != 1 || stored.Token != adm.AdmissionToken || stored.Seq != d.seq || stored.DeviceId != "w_reg1" ||
		stored.EventId != "evt_reg" || stored.ReservedUnits != adm.ReservedUnits || !stored.ExpiresAt.Equal(adm.ExpiresAt) {
		t.Fatalf("core stored %+v (%d slots), user got %+v", stored, n, adm)
	}
}
