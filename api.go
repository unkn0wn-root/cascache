package cascache

import (
	"context"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	gen "github.com/unkn0wn-root/cascache/genstore"
	pr "github.com/unkn0wn-root/cascache/provider"
)

type SetCostFunc func(key string, raw []byte, isBulk bool, bulkCount int) int64

// Cache is an alias for CAS so callers can write cascache.Cache[V] if preferred.
type Cache[V any] = CAS[V]

// CAS is the provider-agnostic cache interface with compare-and-swap safety
// via per-key generations. V is the caller's value type
// serialization is handled by the configured Codec[V].
type CAS[V any] interface {
	Enabled() bool
	Close(context.Context) error

	// Single
	Get(ctx context.Context, key string) (v V, ok bool, err error)
	SetWithGen(ctx context.Context, key string, value V, observedGen uint64, ttl time.Duration) error
	Invalidate(ctx context.Context, key string) error

	// Bulk (order-agnostic return; use your own ordering by keys slice)
	GetBulk(ctx context.Context, keys []string) (values map[string]V, missing []string, err error)
	SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error

	// Error-aware generation snapshots for CAS writes.
	// These return an error so the caller can decide whether to proceed.
	TrySnapshotGen(ctx context.Context, key string) (uint64, error)
	TrySnapshotGens(ctx context.Context, keys []string) (map[string]uint64, error)

	// Best-effort generation snapshots.
	// Failures are reported through Hooks and the generation falls back to zero.
	SnapshotGen(ctx context.Context, key string) uint64
	SnapshotGens(ctx context.Context, keys []string) map[string]uint64
}

// Options configures the CAS cache.
// Namespace, Provider, and Codec are required.
type Options[V any] struct {
	// Required
	Namespace string // logical namespace to avoid collisions. e.g. "user", "profile", "order"
	Provider  pr.Provider
	Codec     c.Codec[V]

	// Optional
	DefaultTTL      time.Duration // singles; 0 => 10m
	BulkTTL         time.Duration // bulks; 0 => 10m
	CleanupInterval time.Duration // 0 => 1h
	GenRetention    time.Duration // 0 => 30d
	Disabled        bool          // default false (enabled)
	ComputeSetCost  SetCostFunc   // default 1
	GenStore        gen.GenStore  // nil => LocalGenStore (in-process)
	DisableBulk     bool          // default false => bulk enabled
	Hooks           Hooks         // high-signal events for metrics/telemetry/logging
}

func New[V any](opts Options[V]) (CAS[V], error) {
	return newCache[V](opts)
}
