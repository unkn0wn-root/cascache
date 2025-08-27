package cascache

import (
	"context"
	"fmt"
	"sort"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	gen "github.com/unkn0wn-root/cascache/genstore"
	"github.com/unkn0wn-root/cascache/internal/util"
	"github.com/unkn0wn-root/cascache/internal/wire"
	pr "github.com/unkn0wn-root/cascache/provider"
)

const (
	defaultGenRetention = 30 * 24 * time.Hour
	defaultSweep        = time.Hour
)

type cache[V any] struct {
	ns             string
	provider       pr.Provider
	codec          c.Codec[V]
	hooks          Hooks
	enabled        bool
	defaultTTL     time.Duration
	bulkTTL        time.Duration
	sweepInterval  time.Duration
	genRetention   time.Duration
	computeSetCost SetCostFunc
	gen            gen.GenStore
	bulkEnabled    bool
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
		provider: opts.Provider,
		codec:    opts.Codec,
		enabled:  !opts.Disabled,
	}

	// defaults
	c.hooks = coalesce[Hooks](opts.Hooks, NopHooks{})
	c.defaultTTL = coalesce[time.Duration](opts.DefaultTTL, 10*time.Minute)
	c.bulkTTL = coalesce[time.Duration](opts.BulkTTL, 10*time.Minute)
	c.sweepInterval = coalesce[time.Duration](opts.CleanupInterval, defaultSweep)
	c.genRetention = coalesce[time.Duration](opts.GenRetention, defaultGenRetention)

	if opts.ComputeSetCost != nil {
		c.computeSetCost = opts.ComputeSetCost
	} else {
		c.computeSetCost = func(_ string, _ []byte, _ bool, _ int) int64 { return 1 }
	}

	if opts.GenStore != nil {
		c.gen = opts.GenStore
	} else {
		// default to local (in-process) gen store
		c.gen = gen.NewLocalGenStore(c.sweepInterval, c.genRetention)
	}

	c.bulkEnabled = !opts.DisableBulk
	if c.bulkEnabled && isLocalGenStore(c.gen) {
		c.hooks.LocalGenWithBulk()
	}

	return c, nil
}

// Enabled reports whether the cache is enabled
func (c *cache[V]) Enabled() bool { return c.enabled }

// Close flushes resources for the GenStore and Provider
func (c *cache[V]) Close(ctx context.Context) error {
	// Close gen store first (best effort)
	if c.gen != nil {
		_ = c.gen.Close(ctx)
	}
	if c.provider != nil {
		return c.provider.Close(ctx)
	}
	return nil
}

// Get returns the value for key if present and not stale, performing
// read-side generation validation and self-healing on corruption.
func (c *cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if !c.enabled {
		return zero, false, nil
	}

	k := c.singleKey(key)
	raw, ok, err := c.provider.Get(ctx, k)
	if err != nil || !ok {
		return zero, false, err
	}

	dgen, payload, err := wire.DecodeSingle(raw)
	if err != nil {
		_ = c.provider.Del(ctx, k) // self-heal corrupt
		c.hooks.SelfHealSingle(k, "corrupt")
		return zero, false, nil
	}

	// validate generation
	if dgen != c.snapshotGen(ctx, k) {
		_ = c.provider.Del(ctx, k)
		c.hooks.SelfHealSingle(k, "gen_mismatch")
		return zero, false, nil
	}

	v, err := c.codec.Decode(payload)
	if err != nil {
		_ = c.provider.Del(ctx, k) // self-heal
		c.hooks.SelfHealSingle(k, "value_decode")
		return zero, false, nil
	}
	return v, true, nil
}

// SetWithGen writes value using CAS: the write is accepted only if the
// current generation equals observedGen.
func (c *cache[V]) SetWithGen(ctx context.Context, key string, value V, observedGen uint64, ttl time.Duration) error {
	if !c.enabled {
		return nil
	}

	if ttl == 0 {
		ttl = c.defaultTTL
	}

	k := c.singleKey(key)
	if c.snapshotGen(ctx, k) != observedGen {
		// generation moved; skip stale write
		return nil
	}

	payload, err := c.codec.Encode(value)
	if err != nil {
		return err
	}

	wireb := wire.EncodeSingle(observedGen, payload)
	ok, err := c.provider.Set(ctx, k, wireb, c.computeSetCost(k, wireb, false, 1), ttl)
	if err != nil {
		return err
	}
	if !ok {
		c.hooks.ProviderSetRejected(k, false)
	}
	return nil
}

// Invalidate bumps the per-key generation and best-effort deletes the single entry.
// Ordering matters: we bump first so that even if the delete fails, readers will
// observe a gen mismatch and self-heal.
//
// Failure modes:
//  1. Bump OK, Delete FAILS  → Safe. Reads will see gen≠dgen and delete on read. (no error returned)
//  2. Bump FAILS, Delete OK  → Partial. Single is gone, but bulks may reseed the single
//     until the gen-store recovers or TTL expires. (no error returned)
//  3. Bump FAILS, Delete FAILS → Likely full backend outage (e.g., Redis cluster down).
//     We return an error so callers can react.
//
// Note: If Provider and GenStore share a Redis cluster, case (3) is the coupled failure.
// TTLs still bound staleness; a later successful bump invalidates bulks.
func (c *cache[V]) Invalidate(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}

	k := c.singleKey(key)
	_, bumpErr := c.bumpGen(ctx, k)
	delErr := c.provider.Del(ctx, k)

	// Only surface the coupled failure (likely full outage).
	if bumpErr != nil && delErr != nil {
		c.hooks.InvalidateOutage(key, bumpErr, delErr)
		return &InvalidateError{Key: key, BumpErr: bumpErr, DelErr: delErr}
	}
	return nil
}

// GetBulk returns cached values for keys using a bulk entry if valid,
// otherwise falls back to single reads. Missing keys are returned separately.
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

	// Bulk disabled -> singles with memoization
	if !c.bulkEnabled {
		missing = c.memoizedSingles(ctx, keys, out)
		return out, missing, nil
	}

	us := uniqSorted(keys)
	bk := c.bulkKeySorted(us)

	if raw, ok, err := c.provider.Get(ctx, bk); err == nil && ok {
		items, err := wire.DecodeBulk(raw)
		if err == nil && c.bulkValid(ctx, us, items) {
			byKey := make(map[string]V, len(items))
			genByKey := make(map[string]uint64, len(items))
			for _, it := range items {
				v, err := c.codec.Decode(it.Payload)
				if err != nil {
					continue
				}
				byKey[it.Key] = v
				genByKey[it.Key] = it.Gen
			}
			for _, k := range keys {
				if v, ok := byKey[k]; ok {
					out[k] = v
				} else {
					missing = append(missing, k)
				}
			}
			for _, k := range us { // warm once per unique
				if v, ok := byKey[k]; ok {
					_ = c.SetWithGen(ctx, k, v, genByKey[k], c.defaultTTL)
				}
			}
			return out, missing, nil
		}
		_ = c.provider.Del(ctx, bk) // self-heal
		reason := "invalid_or_stale"
		if err != nil {
			reason = "decode_error"
		}
		c.hooks.BulkRejected(c.ns, len(us), reason)
	}

	// Fallback: singles with memoization
	missing = c.memoizedSingles(ctx, keys, out)
	return out, missing, nil
}

// SetBulkWithGens writes a bulk entry using CAS across all members.
// If any member’s observed gen mismatches, it seeds singles instead.
func (c *cache[V]) SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error {
	if !c.enabled || len(items) == 0 {
		return nil
	}
	if !c.bulkEnabled {
		for k, v := range items {
			if obs, ok := observedGens[k]; ok {
				_ = c.SetWithGen(ctx, k, v, obs, ttl)
			}
		}
		return nil
	}

	if ttl == 0 {
		ttl = c.bulkTTL
	}

	// verify all observed gens still current
	for k := range items {
		kk := c.singleKey(k)
		obs, ok := observedGens[k]
		if !ok || c.snapshotGen(ctx, kk) != obs {
			// skip bulk; seed singles instead (use default single TTL)
			c.hooks.BulkRejected(c.ns, len(items), "gen_mismatch")
			for kk2, v := range items {
				if obs2, ok := observedGens[kk2]; ok {
					_ = c.SetWithGen(ctx, kk2, v, obs2, c.defaultTTL)
				}
			}
			return nil
		}
	}

	// encode all into wire bulk (deterministic key order)
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	wireItems := make([]wire.BulkItem, 0, len(items))
	for _, k := range keys {
		payload, err := c.codec.Encode(items[k])
		if err != nil {
			return err
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

	// Use sorted keys for bulk key too
	bk := c.bulkKeySorted(keys)
	ok, err := c.provider.Set(ctx, bk, wireb, c.computeSetCost(bk, wireb, true, len(items)), ttl)
	if err != nil {
		return err
	}
	if !ok {
		c.hooks.ProviderSetRejected(bk, true)
		for k, v := range items {
			_ = c.SetWithGen(ctx, k, v, observedGens[k], c.defaultTTL)
		}
		return nil
	}

	// also seed singles best-effort
	for k, v := range items {
		_ = c.SetWithGen(ctx, k, v, observedGens[k], c.defaultTTL)
	}
	return nil
}

// SnapshotGen returns the current generation for key.
func (c *cache[V]) SnapshotGen(key string) uint64 {
	return c.snapshotGen(context.Background(), c.singleKey(key))
}

// SnapshotGens returns current generations for multiple keys.
func (c *cache[V]) SnapshotGens(keys []string) map[string]uint64 {
	storage := make([]string, len(keys))
	for i, k := range keys {
		storage[i] = c.singleKey(k)
	}

	m, err := c.gen.SnapshotMany(context.Background(), storage)
	if err != nil {
		// fallback one by one
		out := make(map[string]uint64, len(keys))
		for _, k := range keys {
			out[k] = c.SnapshotGen(k)
		}
		return out
	}

	out := make(map[string]uint64, len(keys))
	for i, k := range keys {
		out[k] = m[storage[i]]
	}
	return out
}

func (c *cache[V]) snapshotGen(ctx context.Context, storageKey string) uint64 {
	g, err := c.gen.Snapshot(ctx, storageKey)
	if err != nil {
		// Conservative: treat as 0 so CAS writes will skip; reads will self-heal
		c.hooks.GenSnapshotError(1, err)
		return 0
	}
	return g
}

func (c *cache[V]) bumpGen(ctx context.Context, storageKey string) (uint64, error) {
	g, err := c.gen.Bump(ctx, storageKey)
	if err != nil {
		c.hooks.GenBumpError(storageKey, err)
		return 0, err
	}
	return g, nil
}

// - Every requested key must exist in the bulk and be fresh (gen equal to current).
// - Extras in the bulk are ignored.
// - The input slice is the already-sorted (but not deduplicated) requested keys.
func (c *cache[V]) bulkValid(ctx context.Context, sortedRequested []string, items []wire.BulkItem) bool {
	itemGen := make(map[string]uint64, len(items))
	for _, it := range items {
		itemGen[it.Key] = it.Gen // duplicates in stored items: last wins
	}

	storage := make([]string, len(sortedRequested))
	for i, k := range sortedRequested {
		storage[i] = c.singleKey(k)
	}

	gens, err := c.gen.SnapshotMany(ctx, storage)
	if err != nil {
		c.hooks.GenSnapshotError(len(sortedRequested), err)
		return false
	}

	for i, k := range sortedRequested {
		g, ok := itemGen[k]
		if !ok {
			return false // missing member in bulk
		}
		sk := storage[i]
		if g != gens[sk] {
			return false // stale member
		}
	}
	return true
}

// memoizedSingles does at most one Get per unique key,
// fills 'out', and returns 'missing' preserving caller order & duplicates.
func (c *cache[V]) memoizedSingles(ctx context.Context, keys []string, out map[string]V) []string {
	type res struct {
		v  V
		ok bool
	}

	us := uniqSorted(keys) // unique set for memoization
	tmp := make(map[string]res, len(us))
	for _, k := range us {
		v, ok, _ := c.Get(ctx, k) // ignore err → treat as miss
		tmp[k] = res{v: v, ok: ok}
	}

	missing := make([]string, 0, len(keys))
	for _, k := range keys { // preserve caller order & duplicates
		r := tmp[k]
		if r.ok {
			out[k] = r.v
		} else {
			missing = append(missing, k)
		}
	}
	return missing
}

// singleKey returns the storage key for a logical key within the namespace.
func (c *cache[V]) singleKey(userKey string) string {
	// isolate by namespace
	return "single:" + c.ns + ":" + userKey
}

// bulkKeySorted builds the bulk storage key from sorted logical keys.
func (c *cache[V]) bulkKeySorted(sortedKeys []string) string {
	// sortedKeys must be sorted ascending
	return util.BulkKeySorted("bulk:"+c.ns, sortedKeys)
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
