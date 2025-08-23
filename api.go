package cascache

import (
	"context"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	gen "github.com/unkn0wn-root/cascache/genstore"
	pr "github.com/unkn0wn-root/cascache/provider"
)

type SetCostFunc func(key string, raw []byte, isBulk bool, bulkCount int) int64

type Cache[V any] = CAS[V] // just and alias -> cascache.Cache[User] or cascache.CAS[User]

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
	Provider  pr.Provider
	Codec     c.Codec[V]

	Logger          Logger        // if nil, NopLogger is used
	DefaultTTL      time.Duration // singles; 0 => 10m
	BulkTTL         time.Duration // bulks; 0 => 10m
	CleanupInterval time.Duration // 0 => 1h
	GenRetention    time.Duration // 0 => 30d
	Disabled        bool          // default false (enabled)
	ComputeSetCost  SetCostFunc   // default 1
	GenStore        gen.GenStore  // nil => LocalGenStore (in-process)
	DisableBulk     bool          // default false => bulk enabled
}

func New[V any](opts Options[V]) (CAS[V], error) {
	return newCache[V](opts)
}
