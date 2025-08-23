package genstore

import (
	"context"
	"time"
)

// GenStore abstracts where generations live.
// Use LocalGenStore (default) for in-process gens, or RedisGenStore for distributed gens.
type GenStore interface {
	// Snapshot returns the current generation; missing => 0.
	Snapshot(ctx context.Context, storageKey string) (uint64, error)
	// SnapshotMany returns gens for many keys; missing => 0.
	SnapshotMany(ctx context.Context, storageKeys []string) (map[string]uint64, error)
	// Bump atomically increments and returns the new generation.
	Bump(ctx context.Context, storageKey string) (uint64, error)
	// Cleanup prunes old metadata if applicable (no-op for Redis).
	Cleanup(retention time.Duration)
	// Close releases resources (no-op ok).
	Close(context.Context) error
}
