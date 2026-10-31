// Package testkit starts real dependencies for integration tests with
// Testcontainers. Tests skip (not fail) when Docker is not available.
//
// One container per dependency per test binary: each call returns a fresh
// client on a flushed database. Tests inside a package run one at a time
// (no t.Parallel), so flushing at the start of a test is safe. Ryuk removes
// the containers when the test binary exits.
package testkit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/valkey-io/valkey-go"

	"github.com/PT-Meeting-Sampai-Malam-Konsultasi/rusuh-queue/pkg/valkeyx"
)

// ValkeyImage matches docker-compose.yml and the lab (ElastiCache-like: no AOF, no RDB).
const ValkeyImage = "valkey/valkey:8-alpine"

var (
	valkeyOnce sync.Once
	valkeyAddr string
	valkeyErr  error
)

// Valkey returns a client to the shared Valkey with every key removed and the
// rusuh_q library loaded.
func Valkey(t *testing.T) valkey.Client {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	valkeyOnce.Do(func() {
		valkeyAddr, valkeyErr = startReachable(ctx, testcontainers.ContainerRequest{
			Image:        ValkeyImage,
			Cmd:          []string{"valkey-server", "--appendonly", "no", "--save", ""},
			ExposedPorts: []string{"6379/tcp"},
			// Log only: wait.ForListeningPort's host-side check times out on WSL Docker.
			WaitingFor: wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
		}, "6379/tcp", func(addr string) error {
			c, err := valkeyx.Connect(ctx, addr, "")
			if err == nil {
				c.Close()
			}
			return err
		})
	})
	if valkeyErr != nil {
		t.Fatalf("start valkey: %v", valkeyErr)
	}

	client, err := valkeyx.Connect(ctx, valkeyAddr, "")
	if err != nil {
		t.Fatalf("connect valkey: %v", err)
	}
	t.Cleanup(client.Close)
	if err := client.Do(ctx, client.B().Flushall().Build()).Error(); err != nil {
		t.Fatalf("flushall: %v", err)
	}
	return client
}

func retry(within time.Duration, f func() error) error {
	deadline := time.Now().Add(within)
	for {
		err := f()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// startReachable starts a container and checks it can be reached from the
// host. Docker on WSL sometimes wires a container's network wrongly when many
// start at once (healthy inside, unreachable outside); such a container is
// dropped and replaced, up to three times.
func startReachable(ctx context.Context, req testcontainers.ContainerRequest, port string, probe func(addr string) error) (string, error) {
	var lastErr error
	for range 3 {
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
		if err != nil {
			lastErr = err
			continue
		}
		addr, err := c.PortEndpoint(ctx, port, "")
		if err == nil {
			if err = retry(10*time.Second, func() error { return probe(addr) }); err == nil {
				return addr, nil
			}
		}
		lastErr = err
		_ = c.Terminate(ctx)
	}
	return "", lastErr
}
