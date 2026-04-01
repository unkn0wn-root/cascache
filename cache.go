package cascache

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
	"github.com/unkn0wn-root/cascache/internal/wire"
	pr "github.com/unkn0wn-root/cascache/provider"
	"github.com/unkn0wn-root/cascache/version"
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

	c.keyWriter = opts.KeyWriter
	c.keyInvalidator = opts.KeyInvalidator

	return c, nil
}

// Enabled reports whether the cache is active.
// When disabled, every Get returns a miss and every write is silently dropped.
func (c *cache[V]) Enabled() bool { return c.enabled }

// Close shuts down the version store then the provider.
func (c *cache[V]) Close(ctx context.Context) error {
	if c.versionStore != nil {
		_ = c.versionStore.Close(ctx)
	}
	if c.provider != nil {
		return c.provider.Close(ctx)
	}
	return nil
}

// Get looks up a single key and returns its value only if the cached entry is
// still usable.
//
// The read passes through three validation gates:
//
//  1. Wire: the raw bytes must parse as a valid single-entry frame.
//  2. Version: the embedded fence must match the current
//     authoritative fence in the version store.
//  3. Codec: the payload must decode with the configured codec.
//
// Failures in (1), (2), and (3) delete the provider entry and return a miss.
// If the current authoritative fence cannot be loaded, Get returns a miss without
// serving or deleting the cached value. Provider read failures are returned
// as *OpError with OpGet.
func (c *cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if !c.enabled {
		return zero, false, nil
	}

	sk := c.singleKeys(key)
	storageKey := sk.Value.String()
	raw, ok, err := c.provider.Get(ctx, storageKey)
	if err != nil {
		return zero, false, opError(OpGet, key, err)
	}
	if !ok {
		return zero, false, nil
	}

	dfence, payload, err := wire.DecodeSingle(raw)
	if err != nil {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonCorrupt)
		return zero, false, nil
	}

	snap, err := c.loadSnapshot(ctx, toVersionCacheKey(sk.Cache))
	if err != nil {
		return zero, false, nil
	}
	if !snap.Exists {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonVersionMissing)
		return zero, false, nil
	}
	if !dfence.Equal(snap.Fence) {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonVersionMismatch)
		return zero, false, nil
	}

	v, err := c.codec.Decode(payload)
	if err != nil {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonValueDecode)
		return zero, false, nil
	}
	if guardReason := c.guardSingleRead(ctx, key, v); guardReason != "" {
		c.selfHealSingle(ctx, storageKey, guardReason)
		return zero, false, nil
	}
	return v, true, nil
}

// SnapshotVersion returns the current version for one logical key.
func (c *cache[V]) SnapshotVersion(ctx context.Context, key string) (Version, error) {
	snap, err := c.loadSnapshot(ctx, c.versionKey(key))
	if err != nil {
		return Version{}, opError(OpSnapshot, key, err)
	}
	return versionFromSnapshot(snap), nil
}

// SetIfVersion writes a value only when the current version still matches the
// caller's observed version using the cache's configured default TTL.
func (c *cache[V]) SetIfVersion(
	ctx context.Context,
	key string,
	value V,
	version Version,
) (WriteResult, error) {
	return c.SetIfVersionWithTTL(ctx, key, value, version, c.defaultTTL)
}

// SetIfVersionWithTTL writes a value only when the current version still
// matches the caller's observed version. Generic backends perform a final
// version read immediately before the provider write. When KeyWriter is
// configured, the backend-native implementation collapses compare and write
// into one operation.
func (c *cache[V]) SetIfVersionWithTTL(
	ctx context.Context,
	key string,
	value V,
	version Version,
	ttl time.Duration,
) (WriteResult, error) {
	if !c.enabled {
		return WriteResult{Outcome: WriteOutcomeDisabled}, nil
	}
	if ttl == 0 {
		ttl = c.defaultTTL
	}

	payload, err := c.codec.Encode(value)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}

	sk := c.singleKeys(key)
	ckey := toVersionCacheKey(sk.Cache)

	if c.keyWriter != nil {
		s, kerr := c.keyWriter.SetIfVersion(
			ctx,
			ckey,
			sk.Value.String(),
			version.snapshot(),
			payload,
			ttl,
		)
		if kerr != nil {
			return WriteResult{}, opError(OpSet, key, kerr)
		}
		if !s {
			return WriteResult{Outcome: WriteOutcomeVersionMismatch}, nil
		}
		return WriteResult{Outcome: WriteOutcomeStored}, nil
	}

	snap, ok, err := c.checkSnapshot(ctx, ckey, version)
	if err != nil {
		return WriteResult{Outcome: WriteOutcomeSnapshotError}, opError(OpSnapshot, key, err)
	}
	if !ok {
		return WriteResult{Outcome: WriteOutcomeVersionMismatch}, nil
	}

	sw, err := c.buildSingleWrite(sk, snap.Fence, payload)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}

	s, err := c.setSingle(ctx, sw, ttl)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}
	if !s {
		return WriteResult{Outcome: WriteOutcomeProviderRejected}, nil
	}
	return WriteResult{Outcome: WriteOutcomeStored}, nil
}

// Invalidate marks a key as stale so that future readers will not serve it.
// Backends perform two operations in a specific order:
//
//  1. Advance the authoritative fence in the version store, which makes every
//     existing cached entry for this key stale because its embedded fence
//     will no longer match.
//
//  2. Delete the single entry from the provider as a courtesy so the stale
//     bytes do not linger and waste memory.
//
// The order matters. Advancing first ensures that even if the delete fails,
// readers will see the fence mismatch and self-heal on the next read.
// If we deleted first and the advance then failed, a batch entry could reseed
// the single with stale data and the fence check would still pass.
func (c *cache[V]) Invalidate(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}

	sk := c.singleKeys(key)
	if c.keyInvalidator != nil {
		if err := c.keyInvalidator.Invalidate(
			ctx,
			toVersionCacheKey(sk.Cache),
			sk.Value.String(),
		); err != nil {
			c.hooks.InvalidateOutage(key, err, nil)
			return &InvalidateError{
				Key:        key,
				AdvanceErr: opError(OpInvalidate, key, err),
			}
		}
		return nil
	}

	_, bErr := c.advanceVersion(ctx, toVersionCacheKey(sk.Cache))
	delErr := c.provider.Del(ctx, sk.Value.String())

	if bErr != nil {
		c.hooks.InvalidateOutage(key, bErr, delErr)
		return &InvalidateError{
			Key:        key,
			AdvanceErr: opError(OpInvalidate, key, bErr),
			DelErr:     opError(OpInvalidate, key, delErr),
		}
	}
	return nil
}

// GetMany retrieves multiple keys in one call, trying the most efficient
// path first and falling back as needed:
//
//  1. Look up the combined batch entry whose storage key is derived from the
//     sorted, deduplicated set of requested keys. If it exists, is valid on
//     the wire level, and every member passes the authoritative fence check, decode
//     only the requested items and return them.
//
//     Successful batch hits may also materialize the validated members as
//     individual single entries depending on BatchReadSeed. This warming is
//     optional and best-effort.
//
//  2. If the batch entry is missing, corrupt, stale, or contains items that
//     fail to decode, delete it from the provider to prevent
//     repeated failures on subsequent reads.
//
//  3. Fall back to per-key reads via Get. Results are memoized internally so
//     duplicate keys in the input do not cause duplicate lookups. If
//     BatchReadGuard rejects specific members and no ReadGuard is configured,
//     those rejected members are reported as misses for this call instead of
//     being served back from seeded singles. If BatchReadGuard errors and no
//     ReadGuard is configured, the entire batch fails closed to misses.
//
// When batch mode is disabled (DisableBatch option), step 1 is skipped
// entirely and we go straight to per-key reads.
func (c *cache[V]) GetMany(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	missing := make([]string, 0, len(keys))

	if !c.enabled {
		missing = append(missing, keys...)
		return out, missing, nil
	}

	if len(keys) == 0 {
		return out, nil, nil
	}

	if !c.batchEnabled {
		m, err := c.readSingles(ctx, keys, out)
		return out, m, err
	}

	us := sortedUnique(keys)
	hit, ok, err := c.loadBatchHit(ctx, us)
	if err != nil {
		return out, missing, opError(OpGetMany, "", err)
	}
	if !ok {
		missing, err = c.readSingles(ctx, keys, out)
		return out, missing, err
	}

	plan := c.buildBatchReadPlan(ctx, hit.values)
	if plan.action != batchReadServeAll {
		c.rejectBatch(ctx, hit.storageKey, len(us), plan.reason)
	}
	missing, err = c.applyBatchReadPlan(ctx, keys, us, hit, plan, out)
	return out, missing, err
}

// SnapshotVersions returns the current version for each unique logical key.
func (c *cache[V]) SnapshotVersions(ctx context.Context, keys []string) (map[string]Version, error) {
	keys = sortedUnique(keys)
	ss, err := c.loadSnapshots(ctx, keys)
	if err != nil {
		return nil, err
	}

	out := make(map[string]Version, len(keys))
	for i, key := range keys {
		out[key] = versionFromSnapshot(ss[i])
	}
	return out, nil
}

// SetIfVersions writes one batch entry when every version still matches using
// the cache's configured default single and batch TTLs.
func (c *cache[V]) SetIfVersions(
	ctx context.Context,
	items []VersionedValue[V],
) (BatchWriteResult, error) {
	return c.SetIfVersionsWithTTL(ctx, items, 0)
}

// SetIfVersionsWithTTL writes one batch entry when every version still
// matches. If the batch write is skipped or rejected, the cache falls back to
// checked single writes and reports that through the result.
func (c *cache[V]) SetIfVersionsWithTTL(
	ctx context.Context,
	items []VersionedValue[V],
	ttl time.Duration,
) (BatchWriteResult, error) {
	if !c.enabled {
		return BatchWriteResult{Outcome: WriteOutcomeDisabled}, nil
	}

	if len(items) == 0 {
		return BatchWriteResult{Outcome: WriteOutcomeStored}, nil
	}

	ws, ks, err := prepBatchWrite(items)
	if err != nil {
		return BatchWriteResult{}, err
	}

	sttl := ttl
	if sttl == 0 {
		sttl = c.defaultTTL
	}
	if !c.batchEnabled {
		return c.fallbackSet(ctx, WriteOutcomeDisabled, ws, sttl)
	}

	ss, err := c.loadSnapshots(ctx, ks)
	if err != nil {
		return c.fallbackSet(ctx, WriteOutcomeSnapshotError, ws, sttl)
	}
	for i := range ws {
		if !snapshotMatchesVersion(ss[i], ws[i].obs) {
			return c.fallbackSet(ctx, WriteOutcomeVersionMismatch, ws, sttl)
		}
	}
	for i := range ws {
		if !ws[i].obs.IsMissing() {
			continue
		}

		snap, created, createErr := c.createSnapshot(ctx, c.versionKey(ws[i].key))
		if createErr != nil {
			return c.fallbackSet(ctx, WriteOutcomeSnapshotError, ws, sttl)
		}
		if !created {
			return c.fallbackSet(ctx, WriteOutcomeVersionMismatch, ws, sttl)
		}
		ws[i].obs = versionFromSnapshot(snap)
	}

	bttl := ttl
	if bttl == 0 {
		bttl = c.batchTTL
	}

	wires := make([]wire.BatchItem, 0, len(ws))
	for _, w := range ws {
		payload, eErr := c.codec.Encode(w.val)
		if eErr != nil {
			return BatchWriteResult{}, opError(OpSetIfVersions, w.key, eErr)
		}
		wires = append(wires, wire.BatchItem{
			Key:     w.key,
			Fence:   w.obs.fence,
			Payload: payload,
		})
	}

	wireb, err := wire.EncodeBatch(wires)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}

	bk, err := c.batchKeySorted(ks)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}

	ok, err := c.provider.Set(
		ctx,
		bk.String(),
		wireb,
		c.computeSetCost(bk.String(), wireb, true, len(ws)),
		bttl,
	)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}
	if !ok {
		c.hooks.ProviderSetRejected(bk.String(), true)
		return c.fallbackSet(ctx, WriteOutcomeProviderRejected, ws, sttl)
	}

	if err := c.seedAfterBatch(ctx, ws, wires, sttl); err != nil {
		return BatchWriteResult{Outcome: WriteOutcomeStored}, err
	}

	return BatchWriteResult{Outcome: WriteOutcomeStored}, nil
}

// fallbackSet records that the batch path did not land and retries the write
// through checked single-key writes.
func (c *cache[V]) fallbackSet(
	ctx context.Context,
	outcome WriteOutcome,
	items []batchWriteItem[V],
	ttl time.Duration,
) (BatchWriteResult, error) {
	return BatchWriteResult{
		Outcome:       outcome,
		SeededSingles: true,
	}, c.seedSingles(ctx, items, ttl)
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

// loadBatch reads authoritative state for multiple canonical keys in one batch call.
func (c *cache[V]) loadBatch(ctx context.Context, cacheKeys []version.CacheKey) (map[version.CacheKey]version.Snapshot, error) {
	if len(cacheKeys) == 0 {
		return map[version.CacheKey]version.Snapshot{}, nil
	}

	snaps, err := c.versionStore.SnapshotMany(ctx, cacheKeys)
	if err != nil {
		c.hooks.VersionSnapshotError(len(cacheKeys), err)
		return nil, err
	}
	return snaps, nil
}

// loadSnapshots returns authoritative state for keys in the same order.
// keys must already be sorted and deduplicated.
func (c *cache[V]) loadSnapshots(ctx context.Context, keys []string) ([]version.Snapshot, error) {
	if len(keys) == 0 {
		return []version.Snapshot{}, nil
	}

	ck := c.versionKeys(keys)
	m, err := c.loadBatch(ctx, ck)
	if err == nil {
		out := make([]version.Snapshot, len(ck))
		for i, k := range ck {
			out[i] = m[k]
		}
		return out, nil
	}
	return c.loadFallback(ctx, keys, ck)
}

// versionKey maps one logical key to the canonical version-store key used
// for all single-key freshness checks.
func (c *cache[V]) versionKey(key string) version.CacheKey {
	return toVersionCacheKey(c.space.SingleCacheKey(key))
}

// versionKeys is the slice form of versionKey for callers that already
// have a sorted, deduplicated logical key set.
func (c *cache[V]) versionKeys(keys []string) []version.CacheKey {
	ck := make([]version.CacheKey, len(keys))
	for i, k := range keys {
		ck[i] = c.versionKey(k)
	}
	return ck
}

// loadFallback performs per-key snapshot reads after a batch snapshot fails.
// Unreadable keys are mapped to missing snapshots and returned errors are
// wrapped per logical key so strict callers can fail the whole snapshot.
func (c *cache[V]) loadFallback(
	ctx context.Context,
	keys []string,
	cacheKeys []version.CacheKey,
) ([]version.Snapshot, error) {
	out := make([]version.Snapshot, len(keys))
	errs := make([]error, 0, len(keys))
	for i, k := range keys {
		s, err := c.loadSnapshot(ctx, cacheKeys[i])
		if err != nil {
			out[i] = version.Snapshot{}
			errs = append(errs, &OpError{Op: OpSnapshot, Key: k, Err: err})
			continue
		}
		out[i] = s
	}
	return out, errors.Join(errs...)
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

type batchHit[V any] struct {
	storageKey string
	items      map[string]wire.BatchItem
	values     map[string]V
}

type batchWriteItem[V any] struct {
	key string
	val V
	obs Version
}

type batchReadAction uint8

const (
	batchReadServeAll batchReadAction = iota
	batchReadServeAcceptedMissRejected
	batchReadServeAcceptedRefetchRejected
	batchReadFallbackSingles
	batchReadMissAll
)

type batchReadPlan struct {
	action   batchReadAction
	reason   BatchRejectReason
	rejected map[string]struct{}
}

type batchProjection struct {
	missing      []string
	fallbackKeys []string
}

// batchRejectReason reports whether a stored batch entry can serve the requested
// keys. For each requested key it verifies two things: the key must be present
// in the batch, and its stored fence must match the current authoritative fence
// in the version store.
//
// Extra keys present in the batch but not in the requested set are ignored. A
// non-empty return means the entry should be rejected for this read.
func (c *cache[V]) batchRejectReason(
	ctx context.Context,
	sortedRequested []string,
	items []wire.BatchItem,
) (BatchRejectReason, error) {
	return c.batchRejectByKey(ctx, sortedRequested, indexBatch(items))
}

func (c *cache[V]) batchRejectByKey(
	ctx context.Context,
	sortedRequested []string,
	items map[string]wire.BatchItem,
) (BatchRejectReason, error) {
	ss, err := c.loadSnapshots(ctx, sortedRequested)
	if err != nil {
		return "", err
	}

	for i, k := range sortedRequested {
		it, ok := items[k]
		if !ok {
			return BatchRejectReasonIncompleteBatch, nil
		}
		snap := ss[i]
		if !snap.Exists {
			return BatchRejectReasonVersionMissing, nil
		}
		if !it.Fence.Equal(snap.Fence) {
			return BatchRejectReasonVersionMismatch, nil
		}
	}
	return "", nil
}

// loadBatchHit reads, validates, and decodes a batch entry for one unique key
// set. Corrupt, stale, or undecodable entries are self-healed here so callers
// can treat a false hit as a normal fallback-to-singles condition.
func (c *cache[V]) loadBatchHit(ctx context.Context, sortedRequested []string) (batchHit[V], bool, error) {
	bk, err := c.batchKeySorted(sortedRequested)
	if err != nil {
		return batchHit[V]{}, false, err
	}
	sk := bk.String()

	raw, ok, err := c.provider.Get(ctx, sk)
	if err != nil {
		return batchHit[V]{}, false, err
	}
	if !ok {
		return batchHit[V]{}, false, nil
	}

	it, err := wire.DecodeBatch(raw)
	if err != nil {
		c.rejectBatch(ctx, sk, len(sortedRequested), BatchRejectReasonDecodeError)
		return batchHit[V]{}, false, nil
	}

	bm := indexBatch(it)
	reason, err := c.batchRejectByKey(ctx, sortedRequested, bm)
	if err != nil {
		return batchHit[V]{}, false, nil
	}
	if reason != "" {
		c.rejectBatch(ctx, sk, len(sortedRequested), reason)
		return batchHit[V]{}, false, nil
	}

	dec, err := c.decodeBatch(sortedRequested, bm)
	if err != nil {
		c.rejectBatch(ctx, sk, len(sortedRequested), BatchRejectReasonValueDecode)
		return batchHit[V]{}, false, nil
	}

	return batchHit[V]{
		storageKey: sk,
		items:      bm,
		values:     dec,
	}, true, nil
}

// buildBatchReadPlan translates guard outcomes into one internal action so
// GetMany can keep policy decisions separate from result projection and I/O.
func (c *cache[V]) buildBatchReadPlan(ctx context.Context, values map[string]V) batchReadPlan {
	guard := c.guardBatchRead(ctx, values)
	if guard.allowed() {
		return batchReadPlan{action: batchReadServeAll}
	}

	plan := batchReadPlan{
		reason:   guard.reason,
		rejected: guard.rejected,
	}
	switch {
	case c.batchReadGuard != nil && len(guard.rejected) != 0 && c.readGuard == nil:
		plan.action = batchReadServeAcceptedMissRejected
	case c.batchReadGuard != nil && len(guard.rejected) != 0 && c.readGuard != nil:
		plan.action = batchReadServeAcceptedRefetchRejected
	case c.batchReadGuard != nil && c.readGuard == nil:
		plan.action = batchReadMissAll
	default:
		plan.action = batchReadFallbackSingles
	}
	return plan
}

// applyBatchReadPlan materializes a previously chosen batch-read action back
// onto the caller's requested key order, including warming or single-key
// fallback when the plan requires it.
func (c *cache[V]) applyBatchReadPlan(
	ctx context.Context,
	keys, sortedRequested []string,
	hit batchHit[V],
	plan batchReadPlan,
	out map[string]V,
) ([]string, error) {
	switch plan.action {
	case batchReadServeAll:
		p := projectBatchValues(keys, hit.values, nil, out, false)
		c.seedBatchRead(ctx, sortedRequested, hit.items)
		return p.missing, nil
	case batchReadServeAcceptedMissRejected:
		p := projectBatchValues(keys, hit.values, plan.rejected, out, false)
		return p.missing, nil
	case batchReadServeAcceptedRefetchRejected:
		p := projectBatchValues(keys, hit.values, plan.rejected, out, true)
		m, err := c.readSingles(ctx, p.fallbackKeys, out)
		return append(p.missing, m...), err
	case batchReadMissAll:
		return slices.Clone(keys), nil
	case batchReadFallbackSingles:
	}
	return c.readSingles(ctx, keys, out)
}

// projectBatchValues maps decoded batch members back to the caller's original
// request shape. Rejected keys are either reported as misses immediately or
// collected for later single-key fallback, depending on the caller's plan.
func projectBatchValues[V any](
	keys []string,
	values map[string]V,
	rejected map[string]struct{},
	out map[string]V,
	fallbackRejected bool,
) batchProjection {
	pr := batchProjection{
		missing: make([]string, 0, len(keys)),
	}
	if fallbackRejected {
		pr.fallbackKeys = make([]string, 0, len(keys))
	}

	for _, k := range keys {
		if _, ok := rejected[k]; ok {
			if fallbackRejected {
				pr.fallbackKeys = append(pr.fallbackKeys, k)
			} else {
				pr.missing = append(pr.missing, k)
			}
			continue
		}

		v, ok := values[k]
		if !ok {
			pr.missing = append(pr.missing, k)
			continue
		}
		out[k] = v
	}
	return pr
}

// rejectBatch deletes an unusable batch blob and emits the matching
// operational hook so repeated reads do not keep reprocessing bad data.
func (c *cache[V]) rejectBatch(
	ctx context.Context,
	storageKey string,
	requested int,
	reason BatchRejectReason,
) {
	_ = c.provider.Del(ctx, storageKey)
	c.hooks.BatchRejected(c.ns, requested, reason)
}

// seedBatchRead warms single-key entries from an already validated batch hit
// according to the configured BatchReadSeed policy.
func (c *cache[V]) seedBatchRead(
	ctx context.Context,
	sortedRequested []string,
	items map[string]wire.BatchItem,
) {
	switch c.batchSeed {
	case BatchReadSeedAll:
		_ = c.seedBatch(ctx, sortedRequested, items, c.defaultTTL)
	case BatchReadSeedIfMissing:
		_ = c.seedBatchIfMissing(ctx, sortedRequested, items, c.defaultTTL)
	}
}

// readSingles reads each unique key exactly once via Get and maps the
// results back onto the caller's original key list. This avoids redundant
// provider and version-store round-trips when the input has duplicates.
//
// Hits are written into out. Keys that were not found (including entries
// that were self-healed during Get) appear in the returned missing slice,
// preserving the caller's original order and duplicates.
// self-heal events inside Get produce misses, not errors.
func (c *cache[V]) readSingles(ctx context.Context, keys []string, out map[string]V) ([]string, error) {
	type res struct {
		v   V
		ok  bool
		err error
	}

	us := sortedUnique(keys)
	tmp := make(map[string]res, len(us))
	for _, k := range us {
		v, ok, err := c.Get(ctx, k)
		tmp[k] = res{v: v, ok: ok, err: err}
	}

	m := make([]string, 0, len(keys))
	for _, k := range keys { // preserve caller order and duplicates
		r := tmp[k]
		if r.ok {
			out[k] = r.v
		} else {
			m = append(m, k)
		}
	}

	var errs []error
	for _, k := range us {
		if err := tmp[k].err; err != nil {
			errs = append(errs, err)
		}
	}
	return m, errors.Join(errs...)
}

// singleKeys translates a caller-facing logical key into both the canonical
// single-key identity used by the version store and the provider value key.
// Example: logical key "42" in namespace "user" becomes:
//   - Cache: "s:4:user:42"
//   - Value: "cas:v:{<hash>}:s:4:user:42"
//
// The {<hash>} is a deterministic prefix derived from the cache key.
func (c *cache[V]) singleKeys(userKey string) keyutil.Single {
	return c.space.Single(userKey)
}

// batchKeySorted builds the provider storage key for a batch entry from a set
// of sorted member keys. The key is a hash of the members, so the same set
// always maps to the same storage key regardless of input order.
// The sortedKeys slice must be sorted in ascending order and deduplicated.
func (c *cache[V]) batchKeySorted(sortedKeys []string) (keyutil.ValueKey, error) {
	return c.space.BatchValueSorted(sortedKeys)
}

type batchReadGuardResult struct {
	reason   BatchRejectReason
	rejected map[string]struct{}
}

func (r batchReadGuardResult) allowed() bool { return r.reason == "" }

// guardSingleRead applies the configured single-key read guard and translates
// its result into the self-heal reason Get should record on rejection.
func (c *cache[V]) guardSingleRead(ctx context.Context, key string, value V) SelfHealReason {
	if c.readGuard == nil {
		return ""
	}

	ok, err := c.readGuard(ctx, key, value)
	if err != nil {
		return SelfHealReasonReadGuardError
	}
	if !ok {
		return SelfHealReasonReadGuardReject
	}
	return ""
}

// guardBatchRead applies the authoritative validation configured for batch hits.
// It prefers BatchReadGuard when present, validates any reported rejected keys,
// and otherwise falls back to per-member ReadGuard checks.
func (c *cache[V]) guardBatchRead(ctx context.Context, values map[string]V) batchReadGuardResult {
	if len(values) == 0 {
		return batchReadGuardResult{}
	}

	if c.batchReadGuard != nil {
		gv := make(map[string]V, len(values))
		maps.Copy(gv, values)
		rejected, err := c.batchReadGuard(ctx, gv)
		if err != nil {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
		}
		if len(rejected) == 0 {
			return batchReadGuardResult{}
		}

		r := make(map[string]struct{}, len(rejected))
		for k := range rejected {
			if _, ok := values[k]; !ok {
				// A guard may reject only members that were actually validated.
				return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
			}
			r[k] = struct{}{}
		}
		return batchReadGuardResult{
			reason:   BatchRejectReasonReadGuardReject,
			rejected: r,
		}
	}

	if c.readGuard == nil {
		return batchReadGuardResult{}
	}

	for k, v := range values {
		ok, err := c.readGuard(ctx, k, v)
		if err != nil {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
		}
		if !ok {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardReject}
		}
	}
	return batchReadGuardResult{}
}

// decodeBatch decodes only the batch items that the caller requested.
// If any requested item fails to decode the method returns an error and no
// partial results. A decode failure on a requested item means the batch
// entry is not trustworthy and the caller should delete it and fall back
// to per-key reads.
func (c *cache[V]) decodeBatch(requested []string, items map[string]wire.BatchItem) (map[string]V, error) {
	bk := make(map[string]V, len(requested))
	for _, k := range requested {
		it, ok := items[k]
		if !ok {
			return nil, fmt.Errorf("missing batch item %q", k)
		}
		v, err := c.codec.Decode(it.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode batch item %q: %w", k, err)
		}
		bk[k] = v
	}
	return bk, nil
}

// seedBatch materializes validated batch members as single key entries.
// It assumes the caller already established that each member fence is safe to
// serve, so it does not re-check the version store.
func (c *cache[V]) seedBatch(
	ctx context.Context,
	requested []string,
	items map[string]wire.BatchItem,
	ttl time.Duration,
) error {
	if len(requested) == 0 || len(items) == 0 {
		return nil
	}

	var errs []error
	for _, k := range requested {
		it, ok := items[k]
		if !ok {
			continue
		}
		if err := c.writeSingle(ctx, k, it.Fence, it.Payload, ttl); err != nil {
			errs = append(errs, &OpError{Op: OpSet, Key: k, Err: err})
		}
	}
	return errors.Join(errs...)
}

// seedAfterBatch materializes singles after a successful batch
// write according to BatchWriteSeed. Unknown enum values default to Strict so
// the safest behavior wins if a caller passes an out-of-range mode.
func (c *cache[V]) seedAfterBatch(
	ctx context.Context,
	items []batchWriteItem[V],
	wireItems []wire.BatchItem,
	ttl time.Duration,
) error {
	switch c.batchWriteSeed {
	case BatchWriteSeedOff:
		return nil
	case BatchWriteSeedFast:
		return c.seedFromBatch(ctx, wireItems, ttl)
	case BatchWriteSeedStrict:
		fallthrough
	default:
		return c.seedSingles(ctx, items, ttl)
	}
}

// seedBatchIfMissing is the conditional variant of seedBatch. It only
// inserts a single when the provider reports that the key is currently absent.
func (c *cache[V]) seedBatchIfMissing(
	ctx context.Context,
	requested []string,
	items map[string]wire.BatchItem,
	ttl time.Duration,
) error {
	if len(requested) == 0 || len(items) == 0 {
		return nil
	}

	var errs []error
	for _, k := range requested {
		it, ok := items[k]
		if !ok {
			continue
		}
		if err := c.addSingle(ctx, k, it.Fence, it.Payload, ttl); err != nil {
			errs = append(errs, &OpError{Op: OpAdd, Key: k, Err: err})
		}
	}
	return errors.Join(errs...)
}

func (c *cache[V]) seedFromBatch(
	ctx context.Context,
	items []wire.BatchItem,
	ttl time.Duration,
) error {
	if len(items) == 0 {
		return nil
	}

	var errs []error
	for _, it := range items {
		if err := c.writeSingle(ctx, it.Key, it.Fence, it.Payload, ttl); err != nil {
			errs = append(errs, &OpError{Op: OpSet, Key: it.Key, Err: err})
		}
	}
	return errors.Join(errs...)
}

type singleWrite struct {
	storageKey string
	wire       []byte
	cost       int64
}

// buildSingleWrite builds the provider payload and admission metadata for a
// single entry write from an already encoded value payload.
func (c *cache[V]) buildSingleWrite(
	sk keyutil.Single,
	fence version.Fence,
	payload []byte,
) (singleWrite, error) {
	wireb, err := wire.EncodeSingle(fence, payload)
	if err != nil {
		return singleWrite{}, err
	}

	sKey := sk.Value.String()
	return singleWrite{
		storageKey: sKey,
		wire:       wireb,
		cost:       c.computeSetCost(sKey, wireb, false, 1),
	}, nil
}

// selfHealSingle deletes one unusable single entry and emits the matching
// hook reason. Read paths call this after conservative validation failures.
func (c *cache[V]) selfHealSingle(ctx context.Context, storageKey string, reason SelfHealReason) {
	_ = c.provider.Del(ctx, storageKey)
	c.hooks.SelfHealSingle(storageKey, reason)
}

// setSingle executes a prepared single-entry provider Set and reports
// admission rejection through hooks without treating it as an error.
func (c *cache[V]) setSingle(ctx context.Context, sw singleWrite, ttl time.Duration) (bool, error) {
	ok, err := c.provider.Set(ctx, sw.storageKey, sw.wire, sw.cost, ttl)
	if err != nil {
		return false, err
	}
	if !ok {
		c.hooks.ProviderSetRejected(sw.storageKey, false)
	}
	return ok, nil
}

// writeSingle encodes one single entry frame from an already validated batch
// member and stores it through the normal provider Set path.
func (c *cache[V]) writeSingle(
	ctx context.Context,
	key string,
	fence version.Fence,
	payload []byte,
	ttl time.Duration,
) error {
	sk := c.singleKeys(key)
	sw, err := c.buildSingleWrite(sk, fence, payload)
	if err != nil {
		return err
	}
	_, err = c.setSingle(ctx, sw, ttl)
	return err
}

// addSingle encodes one single-entry frame from an already validated batch
// member and inserts it only if the provider reports the key as missing.
func (c *cache[V]) addSingle(
	ctx context.Context,
	key string,
	fence version.Fence,
	payload []byte,
	ttl time.Duration,
) error {
	sk := c.singleKeys(key)
	sw, err := c.buildSingleWrite(sk, fence, payload)
	if err != nil {
		return err
	}
	_, err = c.adder.Add(ctx, sw.storageKey, sw.wire, sw.cost, ttl)
	return err
}

// seedSingles writes each item as an individual single entry via SetIfVersion.
//
//   - When batch mode is disabled, singles are the only cache shape available.
//   - As a fallback when a batch write is skipped (version mismatch, provider
//     rejection, version-store error), it ensures that non-stale keys can still
//     be cached individually.
//
// Each call to SetIfVersionWithTTL performs its own version check, so a stale
// item in the slice is simply skipped without writing bad data.
func (c *cache[V]) seedSingles(
	ctx context.Context,
	items []batchWriteItem[V],
	ttl time.Duration,
) error {
	var errs []error
	for _, it := range items {
		if _, err := c.SetIfVersionWithTTL(ctx, it.key, it.val, it.obs, ttl); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// toVersionCacheKey bridges the internal key builder and the public VersionStore API.
// The VersionStore boundary is intentionally typed so custom implementations cannot
// silently keep treating canonical identities as plain logical strings.
func toVersionCacheKey(cacheKey keyutil.CacheKey) version.CacheKey {
	return version.NewCacheKey(cacheKey.String())
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

func indexBatch(items []wire.BatchItem) map[string]wire.BatchItem {
	bk := make(map[string]wire.BatchItem, len(items))
	for _, it := range items {
		bk[it.Key] = it // duplicates in stored items: last wins
	}
	return bk
}

func prepBatchWrite[V any](items []VersionedValue[V]) ([]batchWriteItem[V], []string, error) {
	ws := make([]batchWriteItem[V], len(items))
	for i, it := range items {
		ws[i] = batchWriteItem[V]{
			key: it.Key,
			val: it.Value,
			obs: it.Version,
		}
	}

	slices.SortFunc(ws, func(a, b batchWriteItem[V]) int { return cmp.Compare(a.key, b.key) })

	ks := make([]string, len(ws))
	for i, it := range ws {
		if i > 0 && it.key == ws[i-1].key {
			return nil, nil, fmt.Errorf("duplicate batch item key %q", it.key)
		}
		ks[i] = it.key
	}
	return ws, ks, nil
}

func sortedUnique(keys []string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	out := slices.Clone(keys)
	slices.Sort(out)
	return slices.Compact(out)
}

func isLocalVersionStore(store version.Store) bool {
	_, ok := store.(*version.LocalStore)
	return ok
}
