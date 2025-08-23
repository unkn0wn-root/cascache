package cascache

import (
	"context"
	"time"
)

// CAS is the high-level, provider-agnostic cache API with CAS safety via per-key generations.
// V is the caller's value type. Serialization is handled by a pluggable Codec[V].
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

	// Generation snapshots (for CAS)
	SnapshotGen(key string) uint64
	SnapshotGens(keys []string) map[string]uint64
}

// Options tune the behavior of the generic CAS cache.
// Only Namespace and Provider are required; others have sensible defaults.
type Options[V any] struct {
	// Required
	Namespace string // logical namespace to avoid collisions. e.g. "user", "profile", "order"
	Provider  Provider
	Codec     Codec[V]

	// Optional
	Logger          Logger                                                         // if nil, logging is disabled
	DefaultTTL      time.Duration                                                  // default single entry TTL (if SetWithGen ttl == 0)
	BulkTTL         time.Duration                                                  // default bulk entry TTL (if SetBulkWithGens ttl == 0)
	CleanupInterval time.Duration                                                  // sweep interval for stale generation metadata; default 1h
	GenRetention    time.Duration                                                  // prune generations not bumped for this long; default 30d
	Enabled         bool                                                           // default true
	ComputeSetCost  func(key string, raw []byte, isBulk bool, bulkCount int) int64 // default returns 1
}

func New[V any](opts Options[V]) (CAS[V], error) {
	return newCache[V](opts)
}
