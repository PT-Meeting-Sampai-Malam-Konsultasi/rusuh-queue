// Package attest checks mobile app attestation (Play Integrity on Android,
// App Attest on iOS). For 13 Nov only the sandbox verifier exists (D-6): it
// checks the shape of the request and leaves abuse control to rate limits.
// Real verifiers are a stretch goal and plug in behind the same interface.
package attest

import (
	"context"
	"errors"
	"regexp"
)

var ErrRejected = errors.New("attest: attestation rejected")

// installID is the random id the app keeps in Keychain / Keystore (UUID or similar).
var installID = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)

// Sandbox accepts any non-empty attestation for a well-formed install id.
type Sandbox struct{}

func (Sandbox) Verify(_ context.Context, platform, id, attestation string) error {
	if platform != "android" && platform != "ios" {
		return ErrRejected
	}
	if !installID.MatchString(id) || attestation == "" || len(attestation) > 16<<10 {
		return ErrRejected
	}
	return nil
}
