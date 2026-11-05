package coreapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
)

var rec = dto.AdmissionRecord{
	Token: "tok", EventID: "evt1", DeviceID: "w_1", UserID: "user_1", Seq: 7, SlotID: "s_x", ReservedUnits: 2,
	ExpiresAt: time.Date(2026, 11, 14, 10, 45, 0, 0, time.UTC),
}

// core answers with codes in order, then 204 for every later call.
func core(t *testing.T, codes ...int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := int(calls.Add(1))
		if n <= len(codes) {
			w.WriteHeader(codes[n-1])
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestRegisterPutsTheRecordForItsSlot(t *testing.T) {
	var got api.AdmissionRecord
	var method, path, authz string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, authz = r.Method, r.URL.Path, r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := New(srv.URL+"/v1", "secret", time.Second).Register(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut || path != "/v1/internal/admissions/s_x" || authz != "Bearer secret" {
		t.Fatalf("request = %s %s %q", method, path, authz)
	}
	if got.UserId == nil || *got.UserId != "user_1" {
		t.Fatalf("userId = %v, want user_1", got.UserId)
	}
	got.UserId = nil
	want := api.AdmissionRecord{Token: "tok", EventId: "evt1", DeviceId: "w_1", Seq: 7, ReservedUnits: 2, ExpiresAt: rec.ExpiresAt}
	if got != want {
		t.Fatalf("body = %+v, want %+v", got, want)
	}
}

func TestRegisterRetriesOnceAfterServerError(t *testing.T) {
	srv, calls := core(t, http.StatusServiceUnavailable)
	if err := New(srv.URL, "s", time.Second).Register(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestRegisterUnavailableAfterTwoServerErrors(t *testing.T) {
	srv, calls := core(t, http.StatusBadGateway, http.StatusInternalServerError)
	err := New(srv.URL, "s", time.Second).Register(context.Background(), rec)
	if !errors.Is(err, entity.ErrAdmissionUnavailable) || calls.Load() != 2 {
		t.Fatalf("err = %v, calls = %d", err, calls.Load())
	}
}

func TestRegisterUnavailableWhenCoreIsSlow(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
	}))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	err := New(srv.URL, "s", 50*time.Millisecond).Register(context.Background(), rec)
	if !errors.Is(err, entity.ErrAdmissionUnavailable) {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %s; the timeout was not applied", took)
	}
}

func TestRegisterDoesNotRetryAClientError(t *testing.T) {
	srv, calls := core(t, http.StatusBadRequest)
	err := New(srv.URL, "s", time.Second).Register(context.Background(), rec)
	if err == nil || errors.Is(err, entity.ErrAdmissionUnavailable) || calls.Load() != 1 {
		t.Fatalf("err = %v, calls = %d", err, calls.Load())
	}
}

func TestDisabledRecordsNothing(t *testing.T) {
	if err := (Disabled{}).Register(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
}
