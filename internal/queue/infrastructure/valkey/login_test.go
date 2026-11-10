package valkey_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/domain/dto"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/queue/infrastructure/valkey"
	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/internal/testkit"
)

// ADR-0021 with short windows: 200 ms for a device logged in before its turn, 1 s otherwise.
var wins = dto.Windows{Miss: 200 * time.Millisecond, Login: time.Second}

func claimAs(device, user string, seq int64, slot string) dto.ClaimInput {
	return dto.ClaimInput{DeviceID: device, UserID: user, TokenSeq: seq, SlotID: slot, OrderWindow: 5 * time.Minute, Windows: wins}
}

func TestGuestTurnLastsTheLoginWindow(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_win", testkit.EventOpts{})

	_, _ = s.Join(ctx, k, "w_guest", "", wins)        // seq 1
	_, _ = s.Join(ctx, k, "w_member", "user_m", wins) // seq 2, logged in before the turn
	testkit.Tick(t, client, k, testkit.TickArgs{})
	time.Sleep(400 * time.Millisecond)

	guest, _ := s.Place(ctx, k, "w_guest", wins)
	if guest.Missed || guest.TurnEnds.Sub(guest.ServerNow) < 400*time.Millisecond {
		t.Fatalf("guest after 400 ms = %+v, want still in its 1 s turn", guest)
	}
	if member, _ := s.Place(ctx, k, "w_member", wins); !member.Missed {
		t.Fatalf("member after 400 ms = %+v, want missed (200 ms turn)", member)
	}
	if r, _ := s.Claim(ctx, k, claimAs("w_member", "user_m", 2, "s_m")); r.Status != dto.ClaimMissed {
		t.Fatalf("member claim = %s, want MISSED", r.Status)
	}
	r, err := s.Claim(ctx, k, claimAs("w_guest", "user_g", 1, "s_g"))
	if err != nil || r.Status != dto.ClaimOK || !r.Attached {
		t.Fatalf("guest claim with a login = %+v, %v; want OK and attached", r, err)
	}
}

func TestLoginAfterTheTurnKeepsTheLoginWindow(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_late", testkit.EventOpts{})

	_, _ = s.Join(ctx, k, "w_late", "", wins)
	testkit.Tick(t, client, k, testkit.TickArgs{})
	time.Sleep(100 * time.Millisecond)
	if r, _ := s.Join(ctx, k, "w_late", "user_l", wins); r.Status != dto.JoinOK || r.Seq != 1 {
		t.Fatalf("login after the turn = %+v, want the same place", r)
	}
	time.Sleep(300 * time.Millisecond)
	if r, _ := s.Claim(ctx, k, claimAs("w_late", "user_l", 1, "s_l")); r.Status != dto.ClaimOK {
		t.Fatalf("claim 400 ms after the turn = %s, want OK (logged in after the turn: 1 s)", r.Status)
	}
}

// A move keeps the source device's login time, so it never buys the longer turn.
func TestMoveKeepsTheLoginTime(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_mvwin", testkit.EventOpts{})

	_, _ = s.Join(ctx, k, "w_a", "user_x", wins)
	testkit.Tick(t, client, k, testkit.TickArgs{})
	if r, _ := s.Transfer(ctx, k, "w_b", "user_x", wins, 1); r.Status != dto.JoinOK || r.Seq != 1 {
		t.Fatalf("move = %+v", r)
	}
	time.Sleep(400 * time.Millisecond)
	if v, _ := s.Place(ctx, k, "w_b", wins); !v.Missed {
		t.Fatalf("moved place after 400 ms = %+v, want missed (200 ms turn)", v)
	}
}

func TestClaimWithAUser(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_cu", testkit.EventOpts{})

	_, _ = s.Join(ctx, k, "w_mine", "user_1", wins)   // seq 1
	_, _ = s.Join(ctx, k, "w_spare", "", wins)        // seq 2
	_, _ = s.Join(ctx, k, "w_theirs", "user_2", wins) // seq 3
	testkit.Tick(t, client, k, testkit.TickArgs{})

	if r, _ := s.Claim(ctx, k, claimAs("w_theirs", "user_1", 3, "s_1")); r.Status != dto.ClaimDeviceOtherAccount {
		t.Fatalf("claim on another account's device = %s, want DEVICE_OTHER_ACCOUNT", r.Status)
	}
	if r, _ := s.Claim(ctx, k, claimAs("w_spare", "user_1", 2, "s_2")); r.Status != dto.ClaimAccountElsewhere {
		t.Fatalf("claim of a spare guest place while the account waits elsewhere = %s, want ACCOUNT_ELSEWHERE", r.Status)
	}
	if r, _ := s.Claim(ctx, k, claimAs("w_mine", "user_1", 1, "s_3")); r.Status != dto.ClaimOK {
		t.Fatalf("own place = %s", r.Status)
	}
	if r, _ := s.Claim(ctx, k, claimAs("w_spare", "user_1", 2, "s_4")); r.Status != dto.ClaimAccountElsewhere {
		t.Fatalf("second claim for the account = %s, want ACCOUNT_ELSEWHERE", r.Status)
	}
}

func TestClaimedLeaseResumesOnAnotherDevice(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_resume", testkit.EventOpts{Available: 10})

	_, _ = s.Join(ctx, k, "w_phone", "user_r", wins)
	testkit.Tick(t, client, k, testkit.TickArgs{})
	first, _ := s.Claim(ctx, k, claimAs("w_phone", "user_r", 1, "s_r"))
	if first.Status != dto.ClaimOK {
		t.Fatalf("claim on the phone = %s", first.Status)
	}
	unreserved := testkit.Int(t, client, k.Unreserved)

	moved, err := s.Transfer(ctx, k, "w_laptop", "user_r", wins, 1)
	if err != nil || moved.Status != dto.JoinClaimed || moved.Seq != 1 || !moved.Attached {
		t.Fatalf("login on the laptop = %+v, %v; want CLAIMED seq 1", moved, err)
	}
	again, _ := s.Claim(ctx, k, claimAs("w_laptop", "user_r", 1, "s_new"))
	if again.Status != dto.ClaimOK || again.SlotID != "s_r" || !again.EndsAt.Equal(first.EndsAt) || again.Units != first.Units {
		t.Fatalf("claim on the laptop = %+v, want the phone's slot %s and lease end", again, "s_r")
	}
	if u := testkit.Int(t, client, k.Unreserved); u != unreserved {
		t.Fatalf("unreserved = %d after the resume, want %d (no new units)", u, unreserved)
	}
	if v, _ := s.Place(ctx, k, "w_phone", wins); v.Value != "m:1" {
		t.Fatalf("phone place = %q, want m:1", v.Value)
	}
	if r, _ := s.Claim(ctx, k, claimAs("w_phone", "user_r", 1, "s_x")); r.Status != dto.ClaimExpired {
		t.Fatalf("phone claim after the move = %s, want EXPIRED (the usecase reports MOVED)", r.Status)
	}
	if r, _ := s.Transfer(ctx, k, "w_tablet", "user_r", wins, 1); r.Status != dto.JoinTransferLimit {
		t.Fatalf("second move = %s, want LIMIT", r.Status)
	}
}

func TestHeldLeaseDoesNotMove(t *testing.T) {
	client := testkit.Valkey(t)
	s := valkey.New(client)
	ctx := context.Background()
	k := testkit.Event(t, client, "evt_held", testkit.EventOpts{Available: 10})

	_, _ = s.Join(ctx, k, "w_phone", "user_h", wins)
	testkit.Tick(t, client, k, testkit.TickArgs{})
	if r, _ := s.Claim(ctx, k, claimAs("w_phone", "user_h", 1, "s_h")); r.Status != dto.ClaimOK {
		t.Fatalf("claim = %s", r.Status)
	}
	// booking.held marks the lease held; the status field is what matters here.
	raw, err := client.Do(ctx, client.B().Hget().Key(k.Leases).Field("s_h").Build()).ToString()
	if err != nil || !strings.Contains(raw, "|reserved|") {
		t.Fatalf("lease = %q, %v", raw, err)
	}
	hset(t, client, k.Leases, "s_h", strings.Replace(raw, "|reserved|", "|held|", 1))

	if r, _ := s.Transfer(ctx, k, "w_laptop", "user_h", wins, 1); r.Status != dto.JoinNothingToMove {
		t.Fatalf("move of a held lease = %s, want NOTHING (core resumes by account)", r.Status)
	}
}
