package qkeys

import (
	"reflect"
	"strings"
	"testing"
)

func TestEveryEventKeySharesTheHashTag(t *testing.T) {
	k := For("evt_2026")
	v := reflect.ValueOf(k)
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		if name == "ID" {
			continue
		}
		key := v.Field(i).String()
		if !strings.Contains(key, "{evt_2026}") {
			t.Errorf("%s = %q has no {evt_2026} hash tag", name, key)
		}
	}
}

func TestValidEvent(t *testing.T) {
	for _, ok := range []string{"evt1", "evt_2026-konser", strings.Repeat("a", 64)} {
		if !ValidEvent(ok) {
			t.Errorf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"", "a}b", "a{b", "a:b", "a b", strings.Repeat("a", 65), "../x"} {
		if ValidEvent(bad) {
			t.Errorf("%q should be invalid", bad)
		}
	}
}
