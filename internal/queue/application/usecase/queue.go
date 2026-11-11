package usecase

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/entity"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/qkeys"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/token"
)

// queueTokenGrace: a queue token lives until the sale ends plus one day (tokens.md).
const queueTokenGrace = 24 * time.Hour

func (s *QueueUsecase) Join(ctx context.Context, in entity.JoinInput) (*entity.Place, error) {
	if !qkeys.ValidEvent(in.EventID) {
		return nil, entity.ErrUnknownEvent
	}
	// Join is idempotent, so a limit per device only stops scripted hammering.
	// No per-IP limit: Indonesian mobile carriers put many users behind one CGNAT IP.
	if r, err := s.limiter.Hit(ctx, s.cfg.JoinPerDevice, in.DeviceID); err != nil {
		slog.WarnContext(ctx, "join rate limit unavailable, allowing", "error", err)
	} else if !r.Allowed {
		return nil, &entity.RateLimitedError{RetryAfter: r.RetryAfter}
	}

	res, err := s.join(ctx, in)
	if err != nil {
		return nil, err
	}

	switch res.Status {
	case dto.JoinNoEvent:
		return nil, entity.ErrUnknownEvent
	case dto.JoinClosed:
		return nil, entity.ErrClosed
	case dto.JoinAccountElsewhere:
		return nil, entity.ErrAccountElsewhere
	case dto.JoinDeviceOtherAccount:
		return nil, entity.ErrDeviceOtherAccount
	case dto.JoinTransferLimit:
		return nil, entity.ErrTransferLimit
	case dto.JoinOK, dto.JoinPre, dto.JoinClaimed:
	default:
		return nil, fmt.Errorf("queue: unknown join status %q", res.Status)
	}

	var seq *int64
	state := "open"
	if res.Status == dto.JoinPre {
		state = "preopen"
	} else {
		seq = &res.Seq
	}
	p, err := s.place(in.EventID, in.DeviceID, state, seq, res.Attached, res.EndsAt, res.Turn)
	if err != nil {
		return nil, err
	}
	p.Claimed = res.Status == dto.JoinClaimed
	return p, nil
}

func (s *QueueUsecase) windows() dto.Windows {
	return dto.Windows{Miss: s.cfg.MissWindow, Login: s.cfg.LoginWindow}
}

// join runs the transfer script first for every logged-in caller: the account's
// place moves here without asking, and the better of the two places is kept
// (D-13, forced since 5 Nov). A guest, or moving turned off, gets a normal join
// (and 409 if the account holds a place elsewhere).
func (s *QueueUsecase) join(ctx context.Context, in entity.JoinInput) (dto.JoinResult, error) {
	k := qkeys.For(in.EventID)
	if in.UserID != "" && s.cfg.TransfersPerEvent > 0 {
		res, err := s.store.Transfer(ctx, k, in.DeviceID, in.UserID, s.windows(), s.cfg.TransfersPerEvent)
		if err != nil || res.Status != dto.JoinNothingToMove {
			if err == nil && (res.Status == dto.JoinOK || res.Status == dto.JoinPre) {
				slog.InfoContext(ctx, "place moved to another device", "event", in.EventID)
			}
			return res, err
		}
	}
	return s.store.Join(ctx, k, in.DeviceID, in.UserID, s.windows())
}

func (s *QueueUsecase) MyPlace(ctx context.Context, eventID, deviceID string, tokenSeq int64) (*entity.Place, error) {
	if !qkeys.ValidEvent(eventID) {
		return nil, entity.ErrUnknownEvent
	}
	v, err := s.store.Place(ctx, qkeys.For(eventID), deviceID, s.windows())
	if err != nil {
		return nil, err
	}
	if !v.Found {
		return nil, entity.ErrUnknownEvent
	}
	if v.State == "closed" {
		return nil, entity.ErrClosed
	}

	switch {
	case v.Value == "pre":
		return s.place(eventID, deviceID, v.State, nil, v.Attached, v.EndsAt, v.Turn)
	case strings.HasPrefix(v.Value, "m:"):
		return nil, entity.ErrMoved
	case v.Value == "" || strings.HasPrefix(v.Value, "x:"):
		return nil, entity.ErrPlaceExpired
	case v.Missed:
		return nil, entity.ErrMissed
	}
	seq, err := strconv.ParseInt(v.Value, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("queue: bad place %q: %w", v.Value, err)
	}
	// A token from before the draw carries seq 0; any other seq must match.
	if tokenSeq != 0 && tokenSeq != seq {
		return nil, entity.ErrPlaceMismatch
	}
	return s.place(eventID, deviceID, v.State, &seq, v.Attached, v.EndsAt, v.Turn)
}

func (s *QueueUsecase) Leave(ctx context.Context, eventID, deviceID string) error {
	if !qkeys.ValidEvent(eventID) {
		return entity.ErrUnknownEvent
	}
	found, err := s.store.Leave(ctx, qkeys.For(eventID), deviceID)
	if err != nil {
		return err
	}
	if !found {
		return entity.ErrUnknownEvent
	}
	return nil
}

func (s *QueueUsecase) Frontier(ctx context.Context, eventID string) ([]byte, error) {
	if !qkeys.ValidEvent(eventID) {
		return nil, entity.ErrUnknownEvent
	}
	b, err := s.store.Frontier(ctx, qkeys.For(eventID))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, entity.ErrUnknownEvent
	}
	return b, nil
}

func (s *QueueUsecase) Claim(ctx context.Context, eventID, deviceID, userID string, tokenSeq int64) (*entity.Admission, error) {
	if !qkeys.ValidEvent(eventID) {
		return nil, entity.ErrUnknownEvent
	}
	if userID == "" && s.cfg.ClaimRequiresLogin {
		return nil, entity.ErrLoginRequired
	}
	if r, err := s.limiter.Hit(ctx, s.cfg.ClaimPerDevice, deviceID); err != nil {
		slog.WarnContext(ctx, "claim rate limit unavailable, allowing", "error", err)
	} else if !r.Allowed {
		return nil, &entity.RateLimitedError{RetryAfter: r.RetryAfter}
	}

	if tokenSeq == 0 {
		// A queue token from before the draw carries seq 0; claim with the number the draw gave.
		v, err := s.store.Place(ctx, qkeys.For(eventID), deviceID, s.windows())
		if err != nil {
			return nil, err
		}
		tokenSeq = placeSeq(v.Value)
	}
	slot, err := newSlotID()
	if err != nil {
		return nil, err
	}
	res, err := s.store.Claim(ctx, qkeys.For(eventID), dto.ClaimInput{
		DeviceID: deviceID, UserID: userID, TokenSeq: tokenSeq, SlotID: slot,
		OrderWindow: s.cfg.OrderWindow, Windows: s.windows(),
	})
	if err != nil {
		return nil, err
	}
	switch res.Status {
	case dto.ClaimOK:
	case dto.ClaimNoEvent:
		return nil, entity.ErrUnknownEvent
	case dto.ClaimClosed:
		return nil, entity.ErrClosed
	case dto.ClaimExpired:
		return nil, s.expiredOrMoved(ctx, eventID, deviceID)
	case dto.ClaimNotYet:
		return nil, entity.ErrNotYet
	case dto.ClaimMissed:
		return nil, entity.ErrMissed
	case dto.ClaimFull:
		return nil, entity.ErrFull
	case dto.ClaimNoStock:
		return nil, entity.ErrNoStock
	case dto.ClaimDeviceOtherAccount:
		return nil, entity.ErrDeviceOtherAccount
	case dto.ClaimAccountElsewhere:
		return nil, entity.ErrAccountElsewhere
	default:
		return nil, fmt.Errorf("queue: unknown claim status %q", res.Status)
	}

	// exp is the lease end in Valkey time, so token and lease end together (FR-17, FR-19).
	raw, err := s.signer.Sign(token.KindAdmission, &token.AdmissionClaims{
		Event:    eventID,
		Seq:      tokenSeq,
		Slot:     res.SlotID,
		Reserved: res.Units,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   deviceID,
			Audience:  jwt.ClaimStrings{token.AudienceCore},
			ExpiresAt: jwt.NewNumericDate(res.EndsAt),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("queue: sign admission: %w", err)
	}
	// core accepts only a token it has stored, so the user must not get one core missed.
	if err := s.admits.Register(ctx, dto.AdmissionRecord{
		Token: raw, EventID: eventID, DeviceID: deviceID, UserID: userID, Seq: tokenSeq,
		SlotID: res.SlotID, ReservedUnits: res.Units, ExpiresAt: res.EndsAt.Add(s.cfg.CorePaymentWindow),
	}); err != nil {
		return nil, err
	}
	return &entity.Admission{
		Token:         raw,
		SlotID:        res.SlotID,
		ReservedUnits: res.Units,
		LoginRequired: !res.Attached,
		ExpiresAt:     res.EndsAt,
		ServerNow:     res.ServerNow,
	}, nil
}

// expiredOrMoved tells a moved place (MOVED) from one that ended (EXPIRED) after
// a refused claim. Only refusals pay for this extra read.
func (s *QueueUsecase) expiredOrMoved(ctx context.Context, eventID, deviceID string) error {
	v, err := s.store.Place(ctx, qkeys.For(eventID), deviceID, s.windows())
	if err == nil && strings.HasPrefix(v.Value, "m:") {
		return entity.ErrMoved
	}
	return entity.ErrPlaceExpired
}

// placeSeq reads the seq of a numbered ("57") or used ("x:57") place; 0 otherwise.
func placeSeq(v string) int64 {
	n, err := strconv.ParseInt(strings.TrimPrefix(v, "x:"), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// newSlotID returns "s_" + 16 random bytes; core stores it under a unique
// index, so one admission makes at most one booking (tokens.md).
func newSlotID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("queue: slot id: %w", err)
	}
	return "s_" + base64.RawURLEncoding.EncodeToString(b), nil
}

func (s *QueueUsecase) place(eventID, deviceID, state string, seq *int64, attached bool, endsAt time.Time, t dto.Turn) (*entity.Place, error) {
	var tokenSeq int64
	if seq != nil {
		tokenSeq = *seq
	}
	exp := endsAt.Add(queueTokenGrace)
	if endsAt.IsZero() || exp.Before(time.Now()) {
		exp = time.Now().Add(queueTokenGrace)
	}
	raw, err := s.signer.Sign(token.KindQueue, &token.QueueClaims{
		Event: eventID,
		Seq:   tokenSeq,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   deviceID,
			Audience:  jwt.ClaimStrings{token.AudienceQueue},
			ExpiresAt: jwt.NewNumericDate(exp),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("queue: sign: %w", err)
	}
	p := &entity.Place{
		EventID: eventID, State: state, Seq: seq, QueueToken: raw, AccountAttached: attached,
		Served: t.Served, OpensAt: t.OpensAt, ServerNow: t.ServerNow,
	}
	if !t.TurnEnds.IsZero() {
		p.TurnExpiresAt = &t.TurnEnds
	}
	return p, nil
}
