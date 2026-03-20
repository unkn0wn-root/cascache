package cascache

import (
	"context"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	gen "github.com/unkn0wn-root/cascache/genstore"
	pr "github.com/unkn0wn-root/cascache/provider"
)

type SetCostFunc func(key string, raw []byte, isBulk bool, bulkCount int) int64

// ReadGuardFunc can veto serving a decoded cache hit for a single logical key.
// It is intended for critical paths that need an authoritative source check
// before a cached value may be returned.
//
// Return allow=false to reject the entry as stale or unsafe. Any returned error
// is treated conservatively as a rejection and the caller receives a miss.
type ReadGuardFunc[V any] func(ctx context.Context, key string, value V) (allow bool, err error)

// BulkReadGuardFunc is the batch form of ReadGuardFunc for validated bulk hits.
// The input map contains only the requested logical keys that survived wire and
// generation checks. Return a set of rejected logical keys; if the set is not
// empty the entire bulk entry is rejected and the cache falls back to singles.
//
// Returning an error is treated conservatively as a bulk rejection.
type BulkReadGuardFunc[V any] func(ctx context.Context, values map[string]V) (rejected map[string]struct{}, err error)

// BulkSeedMode controls whether a successful bulk read validated
// members as individual single-key entries.
// The zero/default value is BulkSeedOff so bulk hits stay read-only unless
// the caller explicitly opts into warming singles.
type BulkSeedMode uint8

const (
	// BulkSeedOff returns the bulk hit "as-is" and does not write singles.
	BulkSeedOff BulkSeedMode = iota

	// BulkSeedAll seeds singles after the bulk has already passed
	// generation validation. No additional per-key generation lookup is done.
	BulkSeedAll

	// BulkSeedIfMissing seeds singles only when the provider supports
	// conditional add/set-if-absent with native backend support or
	// provider-level atomic coordination. Cache construction fails if the
	// provider does not implement Adder.
	BulkSeedIfMissing
)

// BulkWriteSeedMode controls how a successful SetBulkWithGens
// write individual single-key entries.
//
// The zero/default value is BulkWriteSeedStrict, which routes each single
// through SetWithGen again so the post-bulk seeding path preserves the same
// per-key CAS recheck as standalone writes. Higher-throughput systems can opt
// into BulkWriteSeedFast to reuse the validated bulk payloads directly, or
// BulkWriteSeedOff to skip success path single seeding entirely.
type BulkWriteSeedMode uint8

const (
	// BulkWriteSeedStrict seeds singles through SetWithGen after the bulk write
	// has succeeded. Stricter CAS semantics of rechecking
	// each key's generation immediately before the single write lands.
	BulkWriteSeedStrict BulkWriteSeedMode = iota

	// BulkWriteSeedFast seeds singles directly from the validated bulk payloads
	// without a second generation lookup. This is faster but can allow stale
	// singles to land if a generation changes between batch validation and the
	// single writes.
	BulkWriteSeedFast

	// BulkWriteSeedOff skips single seeding after a successful bulk write. The
	// bulk entry is still written, and fallback paths that skip or reject the
	// bulk continue to seed singles best-effort through the checked CAS path.
	BulkWriteSeedOff
)

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

	// Bulk (order-agnostic return. Use your own ordering by keys slice)
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
	Namespace string // logical namespace to isolate the keyspace
	Provider  pr.Provider
	Codec     c.Codec[V]

	DefaultTTL      time.Duration // singles; 0 => 10m
	BulkTTL         time.Duration // bulks; 0 => 10m
	CleanupInterval time.Duration // 0 => 1h
	GenRetention    time.Duration // 0 => 30d
	Disabled        bool          // default false (enabled)
	ComputeSetCost  SetCostFunc   // default 1
	GenStore        gen.GenStore  // nil => LocalGenStore (in-process)
	DisableBulk     bool          // default false => bulk enabled
	ReadGuard       ReadGuardFunc[V]
	BulkReadGuard   BulkReadGuardFunc[V]
	// BulkSeed controls single-entry warming after successful GetBulk hits
	// only. It does not affect how successful SetBulkWithGens writes seed
	// singles; that behavior is controlled separately by BulkWriteSeed.
	// BulkSeedIfMissing requires a provider with native or provider-level
	// atomic add-if-missing support.
	BulkSeed BulkSeedMode
	// BulkWriteSeed controls single-entry materialization after a successful
	// SetBulkWithGens bulk write only. The default is BulkWriteSeedStrict,
	// which re-enters SetWithGen per key to preserve stricter CAS semantics.
	// It does not affect fallback single seeding when the bulk write is
	// skipped, rejected, or bulk mode is disabled.
	BulkWriteSeed BulkWriteSeedMode
	Hooks         Hooks
}

func New[V any](opts Options[V]) (CAS[V], error) {
	return newCache(opts)
}
