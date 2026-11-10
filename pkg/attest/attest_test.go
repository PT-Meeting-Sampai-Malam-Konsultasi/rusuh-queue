package attest

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestSandbox(t *testing.T) {
	ctx := context.Background()
	id := "install-0123456789"
	if err := (Sandbox{}).Verify(ctx, "ios", id, "att"); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][3]string{
		"web":            {"web", id, "att"},
		"short id":       {"android", "abc", "att"},
		"no attestation": {"android", id, ""},
		"huge":           {"android", id, strings.Repeat("a", 16<<10+1)},
	} {
		if err := (Sandbox{}).Verify(ctx, args[0], args[1], args[2]); !errors.Is(err, ErrRejected) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
