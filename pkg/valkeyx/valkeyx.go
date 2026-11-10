// Package valkeyx builds the shared Valkey client and loads the rusuh_q
// function library (lua/rusuh_q.lua).
package valkeyx

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/lua"
)

// Connect opens a client, checks it with PING and loads the function library.
func Connect(ctx context.Context, addr, password string, tlsEnabled bool) (valkey.Client, error) {
	client, err := valkey.NewClient(clientOption(addr, password, tlsEnabled))
	if err != nil {
		return nil, fmt.Errorf("valkey connect %s: %w", addr, err)
	}
	if err := Ping(ctx, client); err != nil {
		client.Close()
		return nil, err
	}
	if err := LoadFunctions(ctx, client); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

// clientOption builds a Valkey client option with TLS certificate verification
// enabled when requested.
func clientOption(addr, password string, tlsEnabled bool) valkey.ClientOption {
	option := valkey.ClientOption{
		InitAddress: []string{addr},
		Password:    password,
		// Queue scripts must not wait on a slow Valkey (Solution Architecture: 50 ms budget);
		// per-call deadlines come from the request context.
		ConnWriteTimeout: 2 * time.Second,
	}
	if tlsEnabled {
		option.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return option
}

// Ping returns an error when Valkey does not answer PING.
func Ping(ctx context.Context, client valkey.Client) error {
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("valkey ping: %w", err)
	}
	return nil
}

// LoadFunctions installs (or replaces) the rusuh_q library.
func LoadFunctions(ctx context.Context, client valkey.Client) error {
	cmd := client.B().FunctionLoad().Replace().FunctionCode(lua.Library).Build()
	if err := client.Do(ctx, cmd).Error(); err != nil {
		return fmt.Errorf("valkey function load rusuh_q: %w", err)
	}
	return nil
}

// FCall runs a library function; keys and args follow the function's comment in rusuh_q.lua.
func FCall(ctx context.Context, client valkey.Client, fn string, keys []string, args ...string) valkey.ValkeyResult {
	return client.Do(ctx, client.B().Fcall().Function(fn).Numkeys(int64(len(keys))).Key(keys...).Arg(args...).Build())
}
