package provider

import (
	"context"
	"time"
)

// Provider is a minimal byte store with TTLs.
// Implementations must be safe for concurrent use.
type Provider interface {
	// Get returns (value, true, nil) on hit; (nil, false, nil) on miss.
	// If an IO/remote error happens, return (nil, false, err).
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Set stores value with the given TTL. Implementations may ignore cost if unsupported.
	// Returns ok=false when store rejected the write under pressure.
	Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (ok bool, err error)

	// Del removes a key (best-effort).
	Del(ctx context.Context, key string) error

	// Close releases resources.
	Close(ctx context.Context) error
}
