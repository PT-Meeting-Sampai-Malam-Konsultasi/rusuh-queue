package integration

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
)

// M5 verify (ADR-0015): with the controller ticking on real Valkey, a device near
// the front gets retry, the current frontier, frontiers from the tick channel and
// its turn, then the stream ends.
func TestStreamDeliversFrontiersAndTheTurn(t *testing.T) {
	e := newAdmissionEnv(t)
	e.publish(t, "evt_sse", 100)
	ahead := make([]joined, 0, 3)
	for _, id := range []string{"w_s1", "w_s2", "w_s3"} {
		ahead = append(ahead, e.join(t, "evt_sse", id))
	}
	me := ahead[2]

	srv := httptest.NewServer(e.mux)
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/queue/evt_sse/stream", nil)
	req.Header.Set("X-Device-Token", me.devToken)
	req.Header.Set("X-Queue-Token", me.queueToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("status %d, Content-Type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}

	type event struct{ name, data string }
	got := make(chan event, 64)
	go func() {
		defer close(got)
		var ev event
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				got <- ev
				ev = event{}
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				ev.data = strings.TrimPrefix(line, "data: ")
			case strings.HasPrefix(line, "retry: "):
				ev.name = "retry"
			}
		}
	}()

	var frontiers int
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-got:
			if !ok {
				t.Fatalf("stream ended without a turn after %d frontiers", frontiers)
			}
			switch ev.name {
			case "frontier":
				frontiers++
			case "turn":
				var turn api.QueueTurn
				if err := json.Unmarshal([]byte(ev.data), &turn); err != nil || int64(turn.Seq) != me.seq ||
					int64(turn.Served) < me.seq || !turn.TurnExpiresAt.After(turn.ServerNow) {
					t.Fatalf("turn = %q (%v)", ev.data, err)
				}
				if frontiers == 0 {
					t.Fatal("turn before any frontier")
				}
				if _, more := <-got; more {
					t.Fatal("stream went on after the turn")
				}
				return
			}
		case <-deadline:
			t.Fatalf("no turn within 10 s (%d frontiers)", frontiers)
		}
	}
}
