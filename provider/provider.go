// Package provider defines the storage abstraction used by cascache.
//
// Implementations MUST be byte-for-byte transparent: Get must return exactly the
// same []byte that was previously passed to Set for a key (no prepended/appended
// metadata, no re-encoding, no mutation). If a store performs internal transforms
// (e.g., compression), they MUST be fully reversed so that the bytes returned by
// Get are identical to the bytes provided to Set.
//
// Important: the keyspaces "single:<ns>:" and "bulk:<ns>:" are owned by cascache.
// External code MUST NOT write values under these prefixes. Foreign writes may be
// treated as corruption by strict wire-format validation and deleted.
package provider

import (
	"context"
	"time"
)

// Provider is a minimal byte store with TTLs.
// Must be safe for concurrent use and must be byte-for-byte
// transparent: Get must return exactly the []byte previously passed to Set for
// the same key. Implementations must not prepend/append metadata, transcode, or
// otherwise mutate values.
type Provider interface {
	// Get returns (value, true, nil) on hit; (nil, false, nil) on miss.
	// If an IO/remote error happens, return (nil, false, err).
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Set stores value with the given TTL. May ignore cost if unsupported.
	// Returns ok=false when the store rejected the write under pressure.
	Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (ok bool, err error)

	// Del removes a key (best-effort).
	Del(ctx context.Context, key string) error

	// Close releases resources.
	Close(ctx context.Context) error
}
