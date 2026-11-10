package http

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"go.uber.org/mock/gomock"

	mock_inbound "github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/_mock/stream/port/inbound"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/api"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/stream/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/auth"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/remotejwks"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

type fakeStream struct {
	initial dto.Frame
	frames  chan dto.Frame
	done    chan struct{}
	seq     int64
}

func (s *fakeStream) Initial() dto.Frame       { return s.initial }
func (s *fakeStream) Frames() <-chan dto.Frame { return s.frames }
func (s *fakeStream) Done() <-chan struct{}    { return s.done }
func (s *fakeStream) Seq() int64               { return s.seq }

func frame(state string, served, ts int64) dto.Frame {
	data := fmt.Sprintf(`{"event":"evt1","state":%q,"served":%d,"streamFrom":100,"ts":%d}`, state, served, ts)
	return dto.Frame{Data: []byte(data), State: state, Served: served, StreamFrom: 100, TS: ts}
}

type rig struct {
	srv     *httptest.Server
	uc      *mock_inbound.MockIStreamUsecase
	headers map[string]string
}

func newRig(t *testing.T, cfg entity.Config) rig {
	ctrl := gomock.NewController(t)
	k, _ := token.GenerateKey("k1")
	keys, _ := token.NewKeyring(k)
	sign := func(kind token.Kind, c token.Claims) string {
		raw, err := keys.Sign(kind, c)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	exp := jwt.NewNumericDate(time.Now().Add(time.Hour))
	dev := sign(token.KindDevice, &token.DeviceClaims{Platform: "web", RegisteredClaims: jwt.RegisteredClaims{
		Subject: "w_1", Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: exp}})
	queue := sign(token.KindQueue, &token.QueueClaims{Event: "evt1", Seq: 150, RegisteredClaims: jwt.RegisteredClaims{
		Subject: "w_1", Audience: jwt.ClaimStrings{token.AudienceQueue}, ExpiresAt: exp}})

	uc := mock_inbound.NewMockIStreamUsecase(ctrl)
	mux := stdhttp.NewServeMux()
	RegisterRoutes(mux, NewStream(uc, auth.New(keys, remotejwks.New("")), cfg))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return rig{srv: srv, uc: uc, headers: map[string]string{"X-Device-Token": dev, "X-Queue-Token": queue}}
}

func (r rig) get(t *testing.T, headers map[string]string) *stdhttp.Response {
	t.Helper()
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, r.srv.URL+"/v1/queue/evt1/stream", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// events reads SSE blocks (separated by a blank line) until the stream ends.
func events(t *testing.T, resp *stdhttp.Response, want int) []string {
	t.Helper()
	got := make(chan []string, 1)
	go func() {
		var blocks []string
		var cur []string
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			if sc.Text() == "" {
				blocks = append(blocks, strings.Join(cur, "\n"))
				cur = nil
				if len(blocks) == want {
					break
				}
				continue
			}
			cur = append(cur, sc.Text())
		}
		got <- blocks
	}()
	select {
	case b := <-got:
		return b
	case <-time.After(2 * time.Second):
		t.Fatalf("fewer than %d events within 2 s", want)
		return nil
	}
}

func fastCfg() entity.Config {
	c := entity.DefaultConfig()
	c.Heartbeat = time.Hour
	return c
}

func TestStreamSendsFrontiersThenTheTurn(t *testing.T) {
	r := newRig(t, fastCfg())
	s := &fakeStream{initial: frame("open", 100, 1), frames: make(chan dto.Frame, 4), done: make(chan struct{}), seq: 150}
	ends, now := time.Date(2026, 11, 13, 10, 45, 0, 0, time.UTC), time.Date(2026, 11, 13, 10, 42, 0, 0, time.UTC)
	r.uc.EXPECT().Open(gomock.Any(), dto.OpenInput{EventID: "evt1", DeviceID: "w_1", TokenSeq: 150}).Return(s, nil)
	r.uc.EXPECT().Turn(gomock.Any(), s).Return(dto.Turn{EventID: "evt1", Seq: 150, Served: 151, TurnExpiresAt: ends, ServerNow: now}, nil)
	r.uc.EXPECT().Close(s)
	s.frames <- frame("open", 140, 2)
	s.frames <- frame("open", 151, 3)

	resp := r.get(t, r.headers)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" ||
		resp.Header.Get("Cache-Control") != "no-store, no-transform" || resp.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
	got := events(t, resp, 5)
	want := []string{
		"retry: 3000",
		"event: frontier\nid: 1\ndata: " + string(frame("open", 100, 1).Data),
		"event: frontier\nid: 2\ndata: " + string(frame("open", 140, 2).Data),
		"event: frontier\nid: 3\ndata: " + string(frame("open", 151, 3).Data),
	}
	for i, w := range want {
		if got[i] != w {
			t.Fatalf("event %d = %q, want %q", i, got[i], w)
		}
	}
	data, ok := strings.CutPrefix(got[4], "event: turn\ndata: ")
	var turn api.QueueTurn
	if !ok || json.Unmarshal([]byte(data), &turn) != nil || turn.Seq != 150 || turn.Served != 151 ||
		!turn.TurnExpiresAt.Equal(ends) || !turn.ServerNow.Equal(now) || turn.Event != "evt1" {
		t.Fatalf("turn event = %q", got[4])
	}
}

func TestStreamAlreadyAtTheTurnSendsItAtOnce(t *testing.T) {
	r := newRig(t, fastCfg())
	s := &fakeStream{initial: frame("open", 150, 1), frames: make(chan dto.Frame), done: make(chan struct{}), seq: 150}
	r.uc.EXPECT().Open(gomock.Any(), gomock.Any()).Return(s, nil)
	r.uc.EXPECT().Turn(gomock.Any(), s).Return(dto.Turn{EventID: "evt1", Seq: 150, Served: 150}, nil)
	r.uc.EXPECT().Close(s)
	got := events(t, r.get(t, r.headers), 3)
	if !strings.HasPrefix(got[2], "event: turn\n") {
		t.Fatalf("events = %q", got)
	}
}

func TestStreamEndsAfterAClosedFrontier(t *testing.T) {
	r := newRig(t, fastCfg())
	s := &fakeStream{initial: frame("open", 100, 1), frames: make(chan dto.Frame, 1), done: make(chan struct{}), seq: 150}
	r.uc.EXPECT().Open(gomock.Any(), gomock.Any()).Return(s, nil)
	r.uc.EXPECT().Close(s)
	s.frames <- frame("closed", 120, 2)
	resp := r.get(t, r.headers)
	got := events(t, resp, 3)
	if !strings.Contains(got[2], `"state":"closed"`) {
		t.Fatalf("events = %q", got)
	}
	if extra := events(t, resp, 1); len(extra) != 0 {
		t.Fatalf("stream went on after closed: %q", extra)
	}
}

func TestStreamSendsHeartbeats(t *testing.T) {
	cfg := fastCfg()
	cfg.Heartbeat = 50 * time.Millisecond
	r := newRig(t, cfg)
	s := &fakeStream{initial: frame("open", 100, 1), frames: make(chan dto.Frame), done: make(chan struct{}), seq: 150}
	r.uc.EXPECT().Open(gomock.Any(), gomock.Any()).Return(s, nil)
	r.uc.EXPECT().Close(s).AnyTimes()
	if got := events(t, r.get(t, r.headers), 3); got[2] != ": hb" {
		t.Fatalf("events = %q", got)
	}
}

func TestStreamEndsWhenTheHubEndsItOrItsLifetimeEnds(t *testing.T) {
	for name, cfg := range map[string]func(*entity.Config, *fakeStream){
		"hub":      func(_ *entity.Config, s *fakeStream) { close(s.done) },
		"lifetime": func(c *entity.Config, _ *fakeStream) { c.Lifetime = 50 * time.Millisecond },
	} {
		c := fastCfg()
		s := &fakeStream{initial: frame("open", 100, 1), frames: make(chan dto.Frame), done: make(chan struct{}), seq: 150}
		cfg(&c, s)
		r := newRig(t, c)
		r.uc.EXPECT().Open(gomock.Any(), gomock.Any()).Return(s, nil)
		r.uc.EXPECT().Close(s)
		resp := r.get(t, r.headers)
		events(t, resp, 2)
		if extra := events(t, resp, 1); len(extra) != 0 {
			t.Fatalf("%s: stream did not end: %q", name, extra)
		}
	}
}

func TestStreamRefusals(t *testing.T) {
	for err, want := range map[error]struct {
		status int
		code   api.ErrorCode
	}{
		entity.ErrTooFar:        {409, api.ErrorCodeTOOFAR},
		entity.ErrFull:          {503, api.ErrorCodeSTREAMFULL},
		entity.ErrDraining:      {503, api.ErrorCodeSTREAMFULL},
		entity.ErrClosed:        {410, api.ErrorCodeQUEUECLOSED},
		entity.ErrMoved:         {410, api.ErrorCodeMOVED},
		entity.ErrMissed:        {410, api.ErrorCodeMISSED},
		entity.ErrPlaceExpired:  {410, api.ErrorCodeEXPIRED},
		entity.ErrUnknownEvent:  {404, api.ErrorCodeNOTFOUND},
		entity.ErrPlaceMismatch: {401, api.ErrorCodeUNAUTHORIZED},
		&entity.RateLimitedError{RetryAfter: time.Second}: {429, api.ErrorCodeRATELIMITED},
		errors.New("boom"): {500, api.ErrorCodeINTERNALERROR},
	} {
		r := newRig(t, fastCfg())
		r.uc.EXPECT().Open(gomock.Any(), gomock.Any()).Return(nil, err)
		resp := r.get(t, r.headers)
		var p struct{ Code api.ErrorCode }
		_ = json.NewDecoder(resp.Body).Decode(&p)
		if resp.StatusCode != want.status || p.Code != want.code {
			t.Errorf("%v: %d %s, want %d %s", err, resp.StatusCode, p.Code, want.status, want.code)
		}
		if want.status == 503 && resp.Header.Get("Retry-After") == "" {
			t.Errorf("%v: 503 without Retry-After", err)
		}
	}
}

func TestStreamNeedsBothTokens(t *testing.T) {
	r := newRig(t, fastCfg())
	if resp := r.get(t, map[string]string{"X-Device-Token": r.headers["X-Device-Token"]}); resp.StatusCode != 401 {
		t.Fatalf("no queue token: %d", resp.StatusCode)
	}
	if resp := r.get(t, nil); resp.StatusCode != 401 {
		t.Fatalf("no tokens: %d", resp.StatusCode)
	}
}
