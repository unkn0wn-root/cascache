package cascache

import (
	"context"
	"errors"
	"fmt"
	"time"

	c "github.com/unkn0wn-root/cascache/v3/codec"
	keyutil "github.com/unkn0wn-root/cascache/v3/internal/keys"
	pr "github.com/unkn0wn-root/cascache/v3/provider"
	"github.com/unkn0wn-root/cascache/v3/version"
)

type cache[V any] struct {
	ns       string
	space    keyutil.Keyspace
	provider pr.Provider
	codec    c.Codec[V]

	// Hooks receives operational notifications such as self-heals and
	// version-store errors.
	hooks   Hooks
	enabled bool // When false, reads miss and writes are dropped.

	defaultTTL time.Duration
	batchTTL   time.Duration

	computeSetCost SetCostFunc
	versionStore   version.Store
	adder          pr.Adder
	readGuard      ReadGuardFunc[V]
	batchReadGuard BatchReadGuardFunc[V]
	keyReader      KeyReader
	keyWriter      KeyWriter
	keyInvalidator KeyInvalidator

	// When false, reads fall back to per-key lookups and batch writes are skipped.
	batchEnabled   bool
	batchSeed      BatchReadSeedMode
	batchWriteSeed BatchWriteSeedMode
}

func newCache[V any](opts Options[V]) (*cache[V], error) {
	if opts.Provider == nil {
		return nil, fmt.Errorf("provider is required")
	}
	if opts.Codec == nil {
		return nil, fmt.Errorf("codec is required")
	}
	if opts.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}

	c := &cache[V]{
		ns:       opts.Namespace,
		space:    keyutil.NewKeyspace(opts.Namespace),
		provider: opts.Provider,
		codec:    opts.Codec,
		enabled:  !opts.Disabled,
	}

	c.hooks = coalesce[Hooks](opts.Hooks, NopHooks{})
	c.defaultTTL = coalesce(opts.DefaultTTL, 10*time.Minute)
	c.batchTTL = coalesce(opts.BatchTTL, 10*time.Minute)

	if opts.ComputeSetCost != nil {
		c.computeSetCost = opts.ComputeSetCost
	} else {
		c.computeSetCost = func(_ string, _ []byte, _ bool, _ int) int64 { return 1 }
	}

	if opts.VersionStore != nil {
		c.versionStore = opts.VersionStore
	} else {
		c.versionStore = version.NewLocal()
	}

	c.readGuard = opts.ReadGuard
	c.batchReadGuard = opts.BatchReadGuard
	c.batchEnabled = !opts.DisableBatch

	if adder, ok := opts.Provider.(pr.Adder); ok {
		c.adder = adder
	}
	if c.batchEnabled && opts.BatchReadSeed == BatchReadSeedIfMissing {
		if c.adder == nil {
			return nil, ErrBatchReadSeedNeedsAdder
		}
	}

	c.batchSeed = opts.BatchReadSeed
	c.batchWriteSeed = opts.BatchWriteSeed
	if c.batchEnabled && isLocalVersionStore(c.versionStore) {
		c.hooks.LocalVersionStoreWithBatch()
	}

	c.keyReader = opts.KeyReader
	c.keyWriter = opts.KeyWriter
	c.keyInvalidator = opts.KeyInvalidator

	return c, nil
}

// Enabled reports whether the cache is active.
// When disabled, every Get returns a miss and every write is silently dropped.
func (c *cache[V]) Enabled() bool { return c.enabled }

// Close shuts down the version store then the provider.
func (c *cache[V]) Close(ctx context.Context) error {
	var errs []error
	if c.versionStore != nil {
		errs = append(errs, c.versionStore.Close(ctx))
	}
	if c.provider != nil {
		errs = append(errs, c.provider.Close(ctx))
	}
	return errors.Join(errs...)
}

// loadSnapshot reads authoritative version state for one key. This is the
// single point where per-key version-store errors are translated into hook calls,
// so every caller gets consistent observability without duplicating that logic.
func (c *cache[V]) loadSnapshot(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, error) {
	snap, err := c.versionStore.Snapshot(ctx, cacheKey)
	if err != nil {
		c.hooks.VersionSnapshotError(1, err)
		return version.Snapshot{}, err
	}
	return snap, nil
}

func (c *cache[V]) createSnapshot(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, bool, error) {
	snap, created, err := c.versionStore.CreateIfMissing(ctx, cacheKey)
	if err != nil {
		c.hooks.VersionCreateError(cacheKey, err)
		return version.Snapshot{}, false, err
	}
	return snap, created, nil
}

// checkSnapshot returns the authoritative snapshot for a single-key write.
// ok reports whether the caller's observed version still allows the write.
func (c *cache[V]) checkSnapshot(
	ctx context.Context,
	cacheKey version.CacheKey,
	observed Version,
) (version.Snapshot, bool, error) {
	if observed.IsMissing() {
		return c.createSnapshot(ctx, cacheKey)
	}

	snap, err := c.loadSnapshot(ctx, cacheKey)
	if err != nil {
		return version.Snapshot{}, false, err
	}
	if !snapshotMatchesVersion(snap, observed) {
		return version.Snapshot{}, false, nil
	}
	return snap, true, nil
}

// versionKey maps one logical key to the canonical version-store key used
// for all single-key freshness checks.
func (c *cache[V]) versionKey(key string) version.CacheKey {
	return toVersionCacheKey(c.space.SingleCacheKey(key))
}

// advanceVersion advances authoritative version state for a canonical single-key
// identity and reports any failure through hooks. This is the write-side
// counterpart of loadSnapshot.
func (c *cache[V]) advanceVersion(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, error) {
	s, err := c.versionStore.Advance(ctx, cacheKey)
	if err != nil {
		c.hooks.VersionAdvanceError(cacheKey, err)
		return version.Snapshot{}, err
	}
	return s, nil
}

// refreshVersion keeps an already matched non-missing version record alive
// before writing its value. Stores without expiring authoritative metadata do
// not need this step and are treated as successfully refreshed.
// refreshed=false means the record disappeared between compare and write so
// callers must treat the write as a version mismatch.
func (c *cache[V]) refreshVersion(ctx context.Context, cacheKey version.CacheKey) (bool, error) {
	refresher, ok := c.versionStore.(version.Refresher)
	if !ok {
		return true, nil
	}
	refreshed, err := refresher.Refresh(ctx, cacheKey)
	if err != nil {
		c.hooks.VersionSnapshotError(1, err)
		return false, err
	}
	return refreshed, nil
}

// toVersionCacheKey bridges the internal key builder and the public VersionStore API.
// The VersionStore boundary is intentionally typed so custom implementations cannot
// silently keep treating canonical identities as plain logical strings.
func toVersionCacheKey(cacheKey keyutil.CacheKey) version.CacheKey {
	return version.NewCacheKey(cacheKey.String())
}

func isLocalVersionStore(store version.Store) bool {
	_, ok := store.(*version.LocalStore)
	return ok
}

func snapshotMatchesVersion(snap version.Snapshot, version Version) bool {
	if version.IsMissing() {
		return !snap.Exists
	}
	if !snap.Exists {
		return false
	}
	return snap.Fence.Equal(version.fence)
}

func coalesce[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
