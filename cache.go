package cascache

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	gen "github.com/unkn0wn-root/cascache/genstore"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
	"github.com/unkn0wn-root/cascache/internal/wire"
	pr "github.com/unkn0wn-root/cascache/provider"
)

const (
	// defaultGenRetention is how long a local generation entry is kept after
	// its last bump before cleanup may prune it. Pruning is safe because a
	// missing generation snapshots as zero and stale higher-generation entries
	// are rejected on read.
	defaultGenRetention = 30 * 24 * time.Hour

	// defaultSweep is how often the local gen store scans for expired
	// generation entries.
	defaultSweep = time.Hour
)

type cache[V any] struct {
	ns       string           // Namespace is reported to hooks and docs.
	space    keyutil.Keyspace // Key builder for the versioned provider/genstore keyspace.
	provider pr.Provider      // Provider stores raw wire-encoded bytes with a TTL.
	codec    c.Codec[V]
	// Hooks receives operational notifications such as self-heals and
	// generation-store errors.
	hooks   Hooks
	enabled bool // When false, reads miss and writes are dropped.

	defaultTTL    time.Duration
	bulkTTL       time.Duration
	sweepInterval time.Duration
	genRetention  time.Duration

	computeSetCost SetCostFunc // computeSetCost influence admission in cost-aware providers.
	gen            gen.GenStore
	bulkEnabled    bool // When false, reads fall back to per-key lookups and bulk writes are skipped.
}

func newCache[V any](opts Options[V]) (*cache[V], error) {
	if opts.Provider == nil {
		return nil, fmt.Errorf("cascache: provider is required")
	}
	if opts.Codec == nil {
		return nil, fmt.Errorf("cascache: codec is required")
	}
	if opts.Namespace == "" {
		return nil, fmt.Errorf("cascache: namespace is required")
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
	c.bulkTTL = coalesce(opts.BulkTTL, 10*time.Minute)
	c.sweepInterval = coalesce(opts.CleanupInterval, defaultSweep)
	c.genRetention = coalesce(opts.GenRetention, defaultGenRetention)

	if opts.ComputeSetCost != nil {
		c.computeSetCost = opts.ComputeSetCost
	} else {
		c.computeSetCost = func(_ string, _ []byte, _ bool, _ int) int64 { return 1 }
	}

	if opts.GenStore != nil {
		c.gen = opts.GenStore
	} else {
		c.gen = gen.NewLocalGenStore(c.sweepInterval, c.genRetention)
	}

	c.bulkEnabled = !opts.DisableBulk
	if c.bulkEnabled && isLocalGenStore(c.gen) {
		c.hooks.LocalGenWithBulk()
	}

	return c, nil
}

// Enabled reports whether the cache is active. When disabled, every Get
// returns a miss and every write is silently dropped.
func (c *cache[V]) Enabled() bool { return c.enabled }

// Close shuts down the gen store best-effort, then the provider.
// Ownership and shutdown semantics are defined by those components.
func (c *cache[V]) Close(ctx context.Context) error {
	if c.gen != nil {
		_ = c.gen.Close(ctx)
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
//  1. Wire integrity: the raw bytes must parse as a valid single-entry frame.
//  2. Generation freshness: the embedded generation must match the current
//     generation in the gen store.
//  3. Codec decode: the payload must decode with the configured codec.
//
// Failures in (1), (2), and (3) delete the provider entry and return a miss.
// If the current generation cannot be loaded, Get returns a miss without
// serving or deleting the cached value.
func (c *cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if !c.enabled {
		return zero, false, nil
	}

	sk := c.singleKeys(key)
	raw, ok, err := c.provider.Get(ctx, sk.Value.String())
	if err != nil || !ok {
		return zero, false, err
	}

	dgen, payload, err := wire.DecodeSingle(raw)
	if err != nil {
		_ = c.provider.Del(ctx, sk.Value.String()) // self-heal corrupt
		c.hooks.SelfHealSingle(sk.Value.String(), SelfHealReasonCorrupt)
		return zero, false, nil
	}

	currentGen, err := c.loadGen(ctx, toGenStoreKey(sk.Cache))
	if err != nil {
		return zero, false, nil
	}
	if dgen != currentGen {
		_ = c.provider.Del(ctx, sk.Value.String())
		c.hooks.SelfHealSingle(sk.Value.String(), SelfHealReasonGenMismatch)
		return zero, false, nil
	}

	v, err := c.codec.Decode(payload)
	if err != nil {
		_ = c.provider.Del(ctx, sk.Value.String()) // self-heal
		c.hooks.SelfHealSingle(sk.Value.String(), SelfHealReasonValueDecode)
		return zero, false, nil
	}
	return v, true, nil
}

// SetWithGen writes a value only if the key's current generation still
// matches what the caller observed before fetching the value from source.
// This is the core CAS guard against stale writes.
//
//	gen, err := cache.TrySnapshotGen(ctx, key)   // 1. observe
//	val       := fetchFromDB(key)                // 2. read source
//	if err == nil {
//	    cache.SetWithGen(ctx, key, val, gen, 0)  // 3. write if unchanged
//	}
//
// If another goroutine or replica calls Invalidate between steps 1 and 3,
// the generation advances and SetWithGen silently drops the write.
//
// There is still a window between the generation check and the provider
// write. If an invalidation happens in that window, the stale write can land,
// but Get will reject it on the next read because the stored generation no
// longer matches.
//
// When the gen store is unreachable the write is skipped entirely. We
// prefer extra source reads over risking stale data in the cache.
func (c *cache[V]) SetWithGen(ctx context.Context, key string, value V, observedGen uint64, ttl time.Duration) error {
	if !c.enabled {
		return nil
	}

	if ttl == 0 {
		ttl = c.defaultTTL
	}

	sk := c.singleKeys(key)
	currentGen, err := c.loadGen(ctx, toGenStoreKey(sk.Cache))
	if err != nil || currentGen != observedGen {
		return nil // generation moved; skip stale write
	}

	payload, err := c.codec.Encode(value)
	if err != nil {
		return err
	}

	wireb, err := wire.EncodeSingle(observedGen, payload)
	if err != nil {
		return err
	}
	ok, err := c.provider.Set(ctx, sk.Value.String(), wireb, c.computeSetCost(sk.Value.String(), wireb, false, 1), ttl)
	if err != nil {
		return err
	}
	if !ok {
		c.hooks.ProviderSetRejected(sk.Value.String(), false)
	}
	return nil
}

// Invalidate marks a key as stale so that future readers will not serve it.
//
// It performs two operations in a specific order:
//
//  1. Bump the generation in the gen store, which makes every existing
//     cached entry for this key stale because their embedded generation
//     will no longer match.
//
//  2. Delete the single entry from the provider as a courtesy so the stale
//     bytes do not linger and waste memory.
//
// The order matters. Bumping first ensures that even if the delete fails,
// readers will see the generation mismatch and self-heal on the next read.
// If we deleted first and the bump then failed, a bulk entry could reseed
// the single with stale data and the gen check would still pass.
//
// An error is returned only when both the bump and the delete fail, which
// typically means a full backend outage. Partial failures are absorbed:
//
//   - Bump succeeds, delete fails: readers self-heal on the next read.
//   - Bump fails, delete succeeds: the single is gone, but a bulk entry
//     could reseed it with stale data until the gen store recovers, the bulk
//     entry is evicted, or a later successful bump invalidates it.
func (c *cache[V]) Invalidate(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}

	sk := c.singleKeys(key)
	_, bumpErr := c.bumpGen(ctx, toGenStoreKey(sk.Cache))
	delErr := c.provider.Del(ctx, sk.Value.String())

	// Only surface the coupled failure (likely full outage).
	if bumpErr != nil && delErr != nil {
		c.hooks.InvalidateOutage(key, bumpErr, delErr)
		return &InvalidateError{Key: key, BumpErr: bumpErr, DelErr: delErr}
	}
	return nil
}

// GetBulk retrieves multiple keys in one call, trying the most efficient
// path first and falling back as needed:
//
//  1. Look up the combined bulk entry whose storage key is derived from the
//     sorted, deduplicated set of requested keys. If it exists, is valid on
//     the wire level, and every member passes the generation check, decode
//     only the requested items and return them. Each member is also seeded
//     as an individual single entry (best-effort) so that future single-key
//     Gets do not need the bulk.
//
//  2. If the bulk entry is missing, corrupt, stale, or contains items that
//     fail to decode, delete it from the provider to free memory and prevent
//     repeated failures on subsequent reads.
//
//  3. Fall back to per-key reads via Get. Results are memoized internally so
//     duplicate keys in the input do not cause duplicate lookups.
//
// When bulk mode is disabled (DisableBulk option), step 1 is skipped
// entirely and we go straight to per-key reads.
//
// The missing slice preserves the caller's original order and may contain
// duplicates if the same missing key was requested more than once. The
// returned error aggregates provider-level errors from the per-key
// fallback; self-heal events are not treated as errors.
func (c *cache[V]) GetBulk(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	missing := make([]string, 0, len(keys))

	if !c.enabled {
		missing = append(missing, keys...)
		return out, missing, nil
	}
	if len(keys) == 0 {
		return out, nil, nil
	}

	if !c.bulkEnabled {
		m, err := c.memoizedSingles(ctx, keys, out)
		return out, m, err
	}

	us := uniqSorted(keys)
	bk, err := c.bulkKeySorted(us)
	if err != nil {
		return out, missing, err
	}

	if raw, ok, gErr := c.provider.Get(ctx, bk.String()); gErr == nil && ok {
		items, dErr := wire.DecodeBulk(raw)
		if dErr == nil && c.bulkValid(ctx, us, items) {
			decodedByKey, rErr := c.decodeRequestedBulkItems(us, items)
			if rErr == nil {
				for _, k := range keys {
					if item, ok := decodedByKey[k]; ok {
						out[k] = item.value
					} else {
						missing = append(missing, k)
					}
				}
				for _, k := range us { // warm once per unique
					if item, ok := decodedByKey[k]; ok {
						_ = c.SetWithGen(ctx, k, item.value, item.gen, c.defaultTTL)
					}
				}
				return out, missing, nil
			}
			_ = c.provider.Del(ctx, bk.String()) // self-heal undecodable bulk payloads
			c.hooks.BulkRejected(c.ns, len(us), BulkRejectReasonValueDecode)
		} else {
			_ = c.provider.Del(ctx, bk.String()) // self-heal
			reason := BulkRejectReasonInvalidOrStale
			if dErr != nil {
				reason = BulkRejectReasonDecodeError
			}
			c.hooks.BulkRejected(c.ns, len(us), reason)
		}
	}

	// fallback to per-key reads.
	missing, err = c.memoizedSingles(ctx, keys, out)
	return out, missing, err
}

// SetBulkWithGens writes a combined bulk entry that bundles multiple keys
// into a single provider value. Each member is also seeded as an individual
// single entry so that future single-key Gets can serve them directly.
//
// Two checks run before anything is written:
//
//  1. Every item key must have a corresponding entry in observedGens. A
//     missing entry is a programming error and causes an immediate return
//     with MissingObservedGensError. Nothing is written.
//
//  2. Every observed generation must still match the current generation in
//     the gen store. If any key was invalidated between the caller’s
//     snapshot and this call, the bulk write is abandoned because a single
//     stale member would poison the entire entry. We still seed each key
//     as an individual single through seedSingles, which runs its own
//     per-key generation check and therefore never writes stale data.
//
// When the gen store is unreachable for the batch snapshot, the bulk is
// skipped and we fall back to seeding singles for the same reason.
//
// When bulk mode is disabled (DisableBulk option), only singles are seeded.
//
// The bulk wire entry is encoded with keys in sorted order so that the
// same set of keys always produces the same storage key regardless of map
// iteration order.
func (c *cache[V]) SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error {
	if !c.enabled || len(items) == 0 {
		return nil
	}
	if missing := c.missingObservedGenKeys(items, observedGens); len(missing) != 0 {
		c.hooks.BulkRejected(c.ns, len(items), BulkRejectReasonMissingObservedGen)
		return newMissingObservedGensError(missing)
	}
	if !c.bulkEnabled {
		return c.seedSingles(ctx, items, observedGens, ttl)
	}

	if ttl == 0 {
		ttl = c.bulkTTL
	}

	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	currentGens, err := c.snapshotSortedUniqueGens(ctx, keys)
	if err != nil {
		c.hooks.BulkRejected(c.ns, len(items), BulkRejectReasonGenSnapshotError)
		return c.seedSingles(ctx, items, observedGens, c.defaultTTL)
	}

	for _, k := range keys {
		if currentGens[k] != observedGens[k] {
			c.hooks.BulkRejected(c.ns, len(items), BulkRejectReasonGenMismatch)
			return c.seedSingles(ctx, items, observedGens, c.defaultTTL)
		}
	}

	wireItems := make([]wire.BulkItem, 0, len(items))
	for _, k := range keys {
		payload, eErr := c.codec.Encode(items[k])
		if eErr != nil {
			return eErr
		}
		wireItems = append(wireItems, wire.BulkItem{
			Key:     k,
			Gen:     observedGens[k],
			Payload: payload,
		})
	}

	wireb, err := wire.EncodeBulk(wireItems)
	if err != nil {
		return err
	}

	bk, err := c.bulkKeySorted(keys)
	if err != nil {
		return err
	}
	ok, err := c.provider.Set(ctx, bk.String(), wireb, c.computeSetCost(bk.String(), wireb, true, len(items)), ttl)
	if err != nil {
		return err
	}
	if !ok {
		c.hooks.ProviderSetRejected(bk.String(), true)
		return c.seedSingles(ctx, items, observedGens, c.defaultTTL)
	}

	_ = c.seedSingles(ctx, items, observedGens, c.defaultTTL)
	return nil
}

// TrySnapshotGen returns the current generation counter for a single key,
// surfacing any gen store error so the caller can decide how to react
// (skip the CAS write, log, increment a metric, etc.).
//
// A generation of zero for a key that has never been bumped is a valid
// value, not an error.
func (c *cache[V]) TrySnapshotGen(ctx context.Context, key string) (uint64, error) {
	return c.loadGen(ctx, toGenStoreKey(c.space.SingleCacheKey(key)))
}

// TrySnapshotGens returns the current generation for every unique logical key.
// Duplicates in the input are coalesced in the result map.
//
// It first attempts a batch read (SnapshotMany) for efficiency. If the batch
// fails it falls back to reading each key individually. An error is returned
// only if at least one key still cannot be read after the fallback.
//
// On error the map is nil rather than partial. This is an all-or-nothing
// contract: the caller either gets a complete snapshot suitable for use with
// SetBulkWithGens, or gets an error and should not attempt the bulk write.
func (c *cache[V]) TrySnapshotGens(ctx context.Context, keys []string) (map[string]uint64, error) {
	out, err := c.snapshotGens(ctx, keys)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SnapshotGen is the best-effort variant of TrySnapshotGen. If the gen
// store is unreachable it returns zero and reports the failure through
// Hooks. A zero result will cause any subsequent SetWithGen to re-check
// the gen store at write time and skip the write if it is still down,
// so no stale data can be introduced.
func (c *cache[V]) SnapshotGen(ctx context.Context, key string) uint64 {
	g, err := c.loadGen(ctx, toGenStoreKey(c.space.SingleCacheKey(key)))
	if err != nil {
		return 0
	}
	return g
}

// SnapshotGens is the best-effort variant of TrySnapshotGens. It always
// returns a map (never nil). Keys whose generation could not be read are
// mapped to zero and failures are reported through Hooks. Duplicate keys
// in the input are coalesced in the result map.
func (c *cache[V]) SnapshotGens(ctx context.Context, keys []string) map[string]uint64 {
	out, _ := c.snapshotGens(ctx, keys)
	return out
}

// loadGen reads a single generation from the gen store. This is the single
// point where per-key gen store errors are translated into hook calls, so
// every caller gets consistent observability without duplicating that logic.
func (c *cache[V]) loadGen(ctx context.Context, cacheKey gen.CacheKey) (uint64, error) {
	g, err := c.gen.Snapshot(ctx, cacheKey)
	if err != nil {
		c.hooks.GenSnapshotError(1, err)
		return 0, err
	}
	return g, nil
}

// loadGens reads generations for multiple keys in a single batch call
// (SnapshotMany). This is much cheaper than N individual calls when the gen
// store is backed by Redis (one MGET round-trip instead of N GETs).
func (c *cache[V]) loadGens(ctx context.Context, cacheKeys []gen.CacheKey) (map[gen.CacheKey]uint64, error) {
	if len(cacheKeys) == 0 {
		return map[gen.CacheKey]uint64{}, nil
	}
	gens, err := c.gen.SnapshotMany(ctx, cacheKeys)
	if err != nil {
		c.hooks.GenSnapshotError(len(cacheKeys), err)
		return nil, err
	}
	return gens, nil
}

// snapshotGens deduplicates the caller's keys before reading generations.
// It is the shared entry point for both SnapshotGens and TrySnapshotGens.
func (c *cache[V]) snapshotGens(ctx context.Context, keys []string) (map[string]uint64, error) {
	us := uniqSorted(keys)
	return c.snapshotSortedUniqueGens(ctx, us)
}

// snapshotSortedUniqueGens reads the current generation for each key,
// expecting the input to be already sorted and deduplicated.
//
// It first tries a batch read via loadGens. If the batch fails (e.g. a
// Redis MGET error) it falls back to reading each key individually. This
// matters because some gen store implementations may support individual
// reads but not batch reads, or the batch may fail transiently while
// individual calls still succeed.
//
// The returned map always has an entry for every input key. Keys that
// could not be read even after the per-key fallback are mapped to zero.
func (c *cache[V]) snapshotSortedUniqueGens(ctx context.Context, keys []string) (map[string]uint64, error) {
	if len(keys) == 0 {
		return map[string]uint64{}, nil
	}

	cacheKeys := make([]gen.CacheKey, len(keys))
	for i, k := range keys {
		cacheKeys[i] = toGenStoreKey(c.space.SingleCacheKey(k))
	}

	m, err := c.loadGens(ctx, cacheKeys)
	if err == nil {
		out := make(map[string]uint64, len(keys))
		for i, k := range keys {
			out[k] = m[cacheKeys[i]]
		}
		return out, nil
	}

	out := make(map[string]uint64, len(keys))
	errs := make([]error, 0, len(keys))
	for i, k := range keys {
		g, loadErr := c.loadGen(ctx, cacheKeys[i])
		if loadErr != nil {
			out[k] = 0
			errs = append(errs, fmt.Errorf("snapshot %q: %w", k, loadErr))
			continue
		}
		out[k] = g
	}
	return out, errors.Join(errs...)
}

// bumpGen increments the generation for a canonical single-key identity and reports any
// failure through hooks. This is the write-side counterpart of loadGen.
func (c *cache[V]) bumpGen(ctx context.Context, cacheKey gen.CacheKey) (uint64, error) {
	g, err := c.gen.Bump(ctx, cacheKey)
	if err != nil {
		c.hooks.GenBumpError(cacheKey, err)
		return 0, err
	}
	return g, nil
}

// bulkValid checks whether a stored bulk entry can serve the requested keys.
// For each requested key it verifies two things: the key must be present in
// the bulk, and its stored generation must match the current generation in
// the gen store. If any member fails either check the entire bulk is
// considered invalid because we cannot serve partial results from it.
//
// Extra keys present in the bulk but not in the requested set are ignored.
// The sortedRequested slice must already be sorted and deduplicated.
func (c *cache[V]) bulkValid(ctx context.Context, sortedRequested []string, items []wire.BulkItem) bool {
	itemGen := make(map[string]uint64, len(items))
	for _, it := range items {
		itemGen[it.Key] = it.Gen // duplicates in stored items: last wins
	}

	gens, err := c.snapshotSortedUniqueGens(ctx, sortedRequested)
	if err != nil {
		return false
	}

	for _, k := range sortedRequested {
		g, ok := itemGen[k]
		if !ok {
			return false // missing member in bulk
		}
		if g != gens[k] {
			return false // stale member
		}
	}
	return true
}

// memoizedSingles reads each unique key exactly once via Get and maps the
// results back onto the caller's original key list. This avoids redundant
// provider and gen store round-trips when the input has duplicates.
//
// Hits are written into out. Keys that were not found (including entries
// that were self-healed during Get) appear in the returned missing slice,
// preserving the caller's original order and duplicates.
// self-heal events inside Get produce misses, not errors.
func (c *cache[V]) memoizedSingles(ctx context.Context, keys []string, out map[string]V) ([]string, error) {
	type res struct {
		v   V
		ok  bool
		err error
	}

	us := uniqSorted(keys)
	tmp := make(map[string]res, len(us))
	for _, k := range us {
		v, ok, err := c.Get(ctx, k)
		tmp[k] = res{v: v, ok: ok, err: err}
	}

	missing := make([]string, 0, len(keys))
	for _, k := range keys { // preserve caller order and duplicates
		r := tmp[k]
		if r.ok {
			out[k] = r.v
		} else {
			missing = append(missing, k)
		}
	}

	errs := make([]error, 0, len(us))
	for _, k := range us {
		if err := tmp[k].err; err != nil {
			errs = append(errs, fmt.Errorf("get %q: %w", k, err))
		}
	}
	return missing, errors.Join(errs...)
}

// singleKeys translates a caller-facing logical key into both the canonical
// single-key identity used by the gen store and the provider value key.
// Example: logical key "42" in namespace "user" becomes:
//   - Cache: "s:4:user:42"
//   - Value: "cas:v1:val:s:4:user:42"
func (c *cache[V]) singleKeys(userKey string) keyutil.Single {
	return c.space.Single(userKey)
}

// toGenStoreKey bridges the internal key builder and the public GenStore API.
// The GenStore boundary is intentionally typed so custom implementations cannot
// silently keep treating canonical identities as plain logical strings.
func toGenStoreKey(cacheKey keyutil.CacheKey) gen.CacheKey {
	return gen.NewCacheKey(cacheKey.String())
}

// bulkKeySorted builds the provider storage key for a bulk entry from a set
// of sorted member keys. The key is a hash of the members, so the same set
// always maps to the same storage key regardless of input order.
// The sortedKeys slice must be sorted in ascending order.
// An error is returned only for impossible key sizes that would make the
// framing ambiguous or overflow platform memory limits.
func (c *cache[V]) bulkKeySorted(sortedKeys []string) (keyutil.ValueKey, error) {
	return c.space.BulkValueSorted(sortedKeys)
}

func uniqSorted(keys []string) []string {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}

	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func isLocalGenStore(gs gen.GenStore) bool {
	_, ok := gs.(*gen.LocalGenStore)
	return ok
}

// missingObservedGenKeys returns item keys for which the caller did not
// provide an observed generation.
func (c *cache[V]) missingObservedGenKeys(items map[string]V, observedGens map[string]uint64) []string {
	missing := make([]string, 0, len(items))
	for k := range items {
		if _, ok := observedGens[k]; !ok {
			missing = append(missing, k)
		}
	}
	return missing
}

// decodedBulkItem pairs a decoded value with the generation it was stored
// under. Both are needed because after reading a bulk entry we seed each
// member as an individual single via SetWithGen, which requires the
// generation for its CAS check.
type decodedBulkItem[V any] struct {
	value V
	gen   uint64
}

// decodeRequestedBulkItems decodes only the bulk items that the caller
// requested. Unrequested extras are ignored by design.
//
// If any requested item fails to decode the method returns an error and no
// partial results. A decode failure on a requested item means the bulk
// entry is not trustworthy and the caller should delete it and fall back
// to per-key reads.
func (c *cache[V]) decodeRequestedBulkItems(requested []string, items []wire.BulkItem) (map[string]decodedBulkItem[V], error) {
	wanted := make(map[string]struct{}, len(requested))
	for _, k := range requested {
		wanted[k] = struct{}{}
	}

	decodedByKey := make(map[string]decodedBulkItem[V], len(requested))
	for _, it := range items {
		if _, ok := wanted[it.Key]; !ok {
			continue
		}
		v, err := c.codec.Decode(it.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode bulk item %q: %w", it.Key, err)
		}
		decodedByKey[it.Key] = decodedBulkItem[V]{value: v, gen: it.Gen}
	}
	return decodedByKey, nil
}

// seedSingles writes each item as an individual single entry via SetWithGen.
// It serves two purposes:
//
//   - After a successful bulk write, it warms individual keys so that future
//     single-key Gets hit the provider directly without needing the bulk.
//   - As a fallback when the bulk write is skipped (gen mismatch, provider
//     rejection, gen store error), it ensures that non-stale keys can still
//     be cached individually.
//
// Each call to SetWithGen performs its own generation check, so a stale
// item in the map is simply skipped without writing bad data.
func (c *cache[V]) seedSingles(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error {
	errs := make([]error, 0, len(items))
	for k, v := range items {
		obs, ok := observedGens[k]
		if !ok {
			continue
		}
		if err := c.SetWithGen(ctx, k, v, obs, ttl); err != nil {
			errs = append(errs, fmt.Errorf("set %q: %w", k, err))
		}
	}
	return errors.Join(errs...)
}
