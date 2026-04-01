package cascache

import (
	"context"
	"time"

	c "github.com/unkn0wn-root/cascache/v3/codec"
	pr "github.com/unkn0wn-root/cascache/v3/provider"
	"github.com/unkn0wn-root/cascache/v3/version"
)

// WriteOutcome describes what happened during a versioned write attempt.
type WriteOutcome string

const (
	WriteOutcomeStored           WriteOutcome = "stored"
	WriteOutcomeVersionMismatch  WriteOutcome = "version_mismatch"
	WriteOutcomeSnapshotError    WriteOutcome = "snapshot_error"
	WriteOutcomeProviderRejected WriteOutcome = "provider_rejected"
	WriteOutcomeDisabled         WriteOutcome = "disabled"
)

type WriteResult struct {
	Outcome WriteOutcome
}

// Stored reports whether the write landed in the provider.
func (r WriteResult) Stored() bool {
	return r.Outcome == WriteOutcomeStored
}

// BatchWriteResult describes the result of a versioned write through the batch
// entry path.
type BatchWriteResult struct {
	Outcome       WriteOutcome
	SeededSingles bool
}

// Stored reports whether the batch entry landed in the provider.
func (r BatchWriteResult) Stored() bool {
	return r.Outcome == WriteOutcomeStored
}

// VersionedValue is the caller-facing unit for versioned multi-key writes.
type VersionedValue[V any] struct {
	Key     string
	Value   V
	Version Version
}

// BatchReadSeedMode controls whether a successful batch read validated
// members as individual single-key entries.
// The zero/default value is BatchReadSeedOff so batch hits stay read-only unless
// the caller explicitly opts into warming singles.
type BatchReadSeedMode uint8

const (
	BatchReadSeedOff BatchReadSeedMode = iota
	BatchReadSeedAll
	BatchReadSeedIfMissing
)

// BatchWriteSeedMode controls how a successful SetIfVersions
// write individual single-key entries.
//
// The zero/default value is BatchWriteSeedStrict, which routes each single
// through SetIfVersion again so the post-batch seeding path preserves the same
// per-key CAS recheck as standalone writes. Higher-throughput systems can opt
// into BatchWriteSeedFast to reuse the validated batch payloads directly, or
// BatchWriteSeedOff to skip success path single seeding entirely.
type BatchWriteSeedMode uint8

const (
	BatchWriteSeedStrict BatchWriteSeedMode = iota
	BatchWriteSeedFast
	BatchWriteSeedOff
)

// KeyWriter is an optional backend-native fast path for single-key
// compare-and-write operations.
// versionKey identifies the canonical authoritative version state tracked by
// the configured VersionStore. valueKey identifies the provider storage key for the encoded single
// value entry. Implementations are responsible for coordinating those two keys
// so SetIfVersion preserves the same freshness contract as the generic cache
// path.
type KeyWriter interface {
	// payload is the codec-encoded caller value, not the final wire envelope.
	// Implementations are responsible for writing a value stamped with the
	// authoritative fence that actually won the compare/init step.
	SetIfVersion(ctx context.Context, versionKey version.CacheKey, valueKey string, expected version.Snapshot, payload []byte, ttl time.Duration) (stored bool, err error)
}

// KeyInvalidator is an optional backend-native fast path for single-key
// invalidation.
// versionKey identifies the canonical authoritative version state tracked by
// the configured VersionStore. valueKey identifies the provider storage key for the encoded single
// value entry. Implementations are responsible for coordinating those two keys
// so Invalidate preserves the same contract as the generic cache
// path.
type KeyInvalidator interface {
	Invalidate(ctx context.Context, versionKey version.CacheKey, valueKey string) error
}

type KeyMutator interface {
	KeyWriter
	KeyInvalidator
}

// ReadGuardFunc can veto serving a decoded cache hit for a single logical key.
// It is intended for critical paths that need an authoritative source check
// before a cached value may be returned.
// Return allow=false to reject the entry as stale or unsafe. Any returned error
// is treated conservatively as a rejection and the caller receives a miss.
type ReadGuardFunc[V any] func(ctx context.Context, key string, value V) (allow bool, err error)

// BatchReadGuardFunc is the batch form of ReadGuardFunc for validated GetMany hits.
// The input map contains only the requested logical keys that survived wire and
// fence checks. Return the keys that failed validation. Any non-empty
// result deletes the stored batch entry because batch values are stored as a
// single blob.
//
// If ReadGuard is also configured, GetMany rechecks the rejected keys through
// per-key fallback reads. Otherwise, rejected keys are treated as misses for
// that GetMany call so they cannot be served back from seeded singles.
type BatchReadGuardFunc[V any] func(ctx context.Context, values map[string]V) (rejected map[string]struct{}, err error)

// SetCostFunc computes the provider cost used for a cache write.
// The returned value is passed through to Provider.Set (and Adder.Add when
// applicable). Providers that use admission or weighted eviction can interpret
// it as entry weight; providers that ignore cost may discard it.
// key is the provider storage key cascache is writing. raw is the fully encoded
// wire value that will be stored. isBatch reports whether the write targets the
// grouped batch-entry path rather than a single-key entry. memberCount is 1 for
// single writes and the number of logical keys contained in a batch entry.
type SetCostFunc func(key string, raw []byte, isBatch bool, memberCount int) int64

// Cache is an alias for CAS so callers can write cascache.Cache[V] if preferred.
type Cache[V any] = CAS[V]

// CAS is the provider-agnostic cache interface with compare-and-swap safety
// via per-key version fences. V is the caller's value type
// serialization is handled by the configured Codec[V].
type CAS[V any] interface {
	Enabled() bool
	Close(context.Context) error

	// Single
	Get(ctx context.Context, key string) (v V, ok bool, err error)
	SnapshotVersion(ctx context.Context, key string) (Version, error)
	SetIfVersion(ctx context.Context, key string, value V, version Version) (WriteResult, error)
	SetIfVersionWithTTL(ctx context.Context, key string, value V, version Version, ttl time.Duration) (WriteResult, error)
	Invalidate(ctx context.Context, key string) error

	// Batch (order-agnostic return. Use your own ordering by keys slice)
	GetMany(ctx context.Context, keys []string) (values map[string]V, missing []string, err error)
	SnapshotVersions(ctx context.Context, keys []string) (map[string]Version, error)
	SetIfVersions(ctx context.Context, items []VersionedValue[V]) (BatchWriteResult, error)
	SetIfVersionsWithTTL(ctx context.Context, items []VersionedValue[V], ttl time.Duration) (BatchWriteResult, error)
}

// Options configures the CAS cache.
// Namespace, Provider, and Codec are required.
type Options[V any] struct {
	Namespace string // logical namespace to isolate the keyspace
	Provider  pr.Provider
	Codec     c.Codec[V]

	DefaultTTL     time.Duration // singles; 0 => 10m
	BatchTTL       time.Duration // batches; 0 => 10m
	Disabled       bool          // default false (enabled)
	ComputeSetCost SetCostFunc   // default 1
	VersionStore   version.Store // nil => LocalStore (in-process)
	KeyWriter      KeyWriter
	KeyInvalidator KeyInvalidator
	DisableBatch   bool // default false => batch enabled
	ReadGuard      ReadGuardFunc[V]
	BatchReadGuard BatchReadGuardFunc[V]
	BatchReadSeed  BatchReadSeedMode
	BatchWriteSeed BatchWriteSeedMode
	Hooks          Hooks
}

func New[V any](opts Options[V]) (CAS[V], error) {
	return newCache(opts)
}
