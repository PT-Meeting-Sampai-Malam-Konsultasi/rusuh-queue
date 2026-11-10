package entity

import (
	"errors"
	"testing"
)

func TestPatchApply(t *testing.T) {
	base := DefaultSettings("evt1")
	got, err := Patch{
		State: Some("closed"), MaxSlots: Some(int64(10)), RateCap: Some(int64(3)), StreamFrom: Some(int64(0)),
	}.Apply(base)
	if err != nil || got != (Settings{EventID: "evt1", MaxSlots: 10, RateCap: 3, StreamFrom: 0, State: "closed"}) {
		t.Fatalf("settings = %+v, err = %v", got, err)
	}
	if got, err := (Patch{}).Apply(base); err != nil || got != base {
		t.Fatalf("empty patch changed settings: %+v, %v", got, err)
	}

	for name, p := range map[string]Patch{
		"state":       {State: Some("frozen")},
		"slots":       {MaxSlots: Some(int64(0))},
		"slots huge":  {MaxSlots: Some(int64(1) << 31)},
		"rate":        {RateCap: Some(int64(0))},
		"stream from": {StreamFrom: Some(int64(-1))},
	} {
		if _, err := p.Apply(base); !errors.Is(err, ErrInvalidSettings) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
