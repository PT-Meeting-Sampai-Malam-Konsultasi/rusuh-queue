package valkeyx

import (
	"crypto/tls"
	"testing"
)

// TestClientOptionTLS verifies Valkey client TLS configuration is enabled only on request.
func TestClientOptionTLS(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		option := clientOption("valkey.example:6379", "secret", true)
		if option.TLSConfig == nil {
			t.Fatal("TLSConfig = nil, want TLS configuration")
		}
		if option.TLSConfig.MinVersion != tls.VersionTLS12 {
			t.Errorf("TLSConfig.MinVersion = %d, want %d", option.TLSConfig.MinVersion, tls.VersionTLS12)
		}
		if option.TLSConfig.InsecureSkipVerify {
			t.Error("TLSConfig.InsecureSkipVerify = true, want false")
		}
	})

	t.Run("disabled", func(t *testing.T) {
		option := clientOption("localhost:6379", "", false)
		if option.TLSConfig != nil {
			t.Errorf("TLSConfig = %#v, want nil", option.TLSConfig)
		}
	})
}
