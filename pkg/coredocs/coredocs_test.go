package coredocs

import (
	"errors"
	"testing"
)

func TestParseSale(t *testing.T) {
	good := `{"v":1,"eventId":"evt1","status":"published","saleOpensAt":"2026-11-13T10:00:00Z","endsAt":"2026-11-13T22:00:00Z"}`
	if _, err := ParseSale([]byte(good), "evt1"); err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"wrong version":     `{"v":2,"eventId":"evt1","status":"published","saleOpensAt":"2026-11-13T10:00:00Z","endsAt":"2026-11-13T22:00:00Z"}`,
		"other event":       `{"v":1,"eventId":"evt2","status":"published","saleOpensAt":"2026-11-13T10:00:00Z","endsAt":"2026-11-13T22:00:00Z"}`,
		"unknown status":    `{"v":1,"eventId":"evt1","status":"draft","saleOpensAt":"2026-11-13T10:00:00Z","endsAt":"2026-11-13T22:00:00Z"}`,
		"ends before opens": `{"v":1,"eventId":"evt1","status":"published","saleOpensAt":"2026-11-13T10:00:00Z","endsAt":"2026-11-13T09:00:00Z"}`,
		"no open time":      `{"v":1,"eventId":"evt1","status":"published","endsAt":"2026-11-13T09:00:00Z"}`,
		"not json":          `{`,
	} {
		if _, err := ParseSale([]byte(doc), "evt1"); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestParseStock(t *testing.T) {
	if s, err := ParseStock([]byte(`{"v":1,"eventId":"evt1","available":0,"version":3}`), "evt1"); err != nil || s.Available != 0 {
		t.Fatalf("zero stock is valid: %+v %v", s, err)
	}
	for name, doc := range map[string]string{
		"negative":    `{"v":1,"eventId":"evt1","available":-1}`,
		"other event": `{"v":1,"eventId":"evt2","available":1}`,
		"no version":  `{"eventId":"evt1","available":1}`,
	} {
		if _, err := ParseStock([]byte(doc), "evt1"); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
