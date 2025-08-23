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
	log            Logger
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
	c.log = coalesce[Logger](opts.Logger, NopLogger{})
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
		// default to in-process generations with periodic cleanup
		c.gen = gen.NewLocalGenStore(c.sweepInterval, c.genRetention)
	}

	c.bulkEnabled = !opts.DisableBulk
	if c.bulkEnabled && isLocalGenStore(c.gen) {
		c.log.Warn("bulk enabled with local generations; stale bulks possible in multi-replica deployments", nil)
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
	gen, payload, err := wire.DecodeSingle(raw)
	if err != nil {
		_ = c.provider.Del(ctx, k) // self-heal corrupt
		return zero, false, nil
	}
	// validate generation
	if gen != c.snapshotGen(k) {
		_ = c.provider.Del(ctx, k)
		return zero, false, nil
	}
	v, err := c.codec.Decode(payload)
	if err != nil {
		_ = c.provider.Del(ctx, k) // self-heal
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
	if c.snapshotGen(k) != observedGen {
		// generation moved; skip stale write
		c.log.Debug("SetWithGen skipped (gen mismatch)", Fields{"key": key, "obs": observedGen})
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
		c.log.Debug("SetWithGen rejected by provider (pressure)", Fields{"key": key})
	}
	return nil
}

// Invalidate bumps the generation for key and deletes the single-entry value.
func (c *cache[V]) Invalidate(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}
	k := c.singleKey(key)
	newGen := c.bumpGen(k)
	_ = c.provider.Del(ctx, k)
	c.log.Debug("invalidated key (bumped gen + cleared single)", Fields{"key": key, "newGen": newGen})
	return nil
}

// GetBulk returns cached values for keys using a bulk entry if valid,
// otherwise falls back to single reads. Missing keys are returned separately.
func (c *cache[V]) GetBulk(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	if !c.enabled {
		missing := append([]string(nil), keys...)
		return out, missing, nil
	}
	if len(keys) == 0 {
		return out, nil, nil
	}

	// Bulk disabled -> use singles.
	if !c.bulkEnabled {
		var missing []string
		for _, k := range keys {
			if v, ok, _ := c.Get(ctx, k); ok {
				out[k] = v
			} else {
				missing = append(missing, k)
			}
		}
		return out, missing, nil
	}

	// sort a copy once; reuse for both bulk key and deterministic decode-order mapping
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)

	bulkKey := c.bulkKeySorted(sorted)
	if raw, ok, err := c.provider.Get(ctx, bulkKey); err == nil && ok {
		items, err := wire.DecodeBulk(raw)
		if err == nil && c.bulkValid(items) {
			byKey := make(map[string]V, len(items))
			genByKey := make(map[string]uint64, len(items))
			for _, it := range items {
				val, err := c.codec.Decode(it.Payload)
				if err != nil {
					continue
				}
				byKey[it.Key] = val
				genByKey[it.Key] = it.Gen
			}
			var missing []string
			for _, k := range keys {
				if v, ok := byKey[k]; ok {
					out[k] = v
					// opportunistic single warmup (CAS-protected)
					_ = c.SetWithGen(ctx, k, v, genByKey[k], c.defaultTTL)
				} else {
					missing = append(missing, k)
				}
			}
			return out, missing, nil
		}
		// stale or corrupt bulk; drop
		_ = c.provider.Del(ctx, bulkKey)
	}

	// Fallback: try singles
	var missing []string
	for _, k := range keys {
		if v, ok, _ := c.Get(ctx, k); ok {
			out[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	return out, missing, nil
}

// SetBulkWithGens writes a bulk entry using CAS across all members.
// If any member’s observed gen mismatches, it seeds singles instead.
func (c *cache[V]) SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error {
	if !c.enabled || len(items) == 0 {
		return nil
	}

	if !c.bulkEnabled {
		sttl := ttl
		if sttl == 0 {
			sttl = c.defaultTTL
		}
		for k, v := range items {
			if obs, ok := observedGens[k]; ok {
				_ = c.SetWithGen(ctx, k, v, obs, sttl)
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
		if !ok || c.snapshotGen(kk) != obs {
			// skip bulk; seed singles instead (use default single TTL)
			c.log.Debug("SetBulkWithGens skipped (gen mismatch)", Fields{"key": k})
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
	wireb := wire.EncodeBulk(wireItems)

	// Use sorted keys for bulk key too
	bk := c.bulkKeySorted(keys)
	ok, err := c.provider.Set(ctx, bk, wireb, c.computeSetCost(bk, wireb, true, len(items)), ttl)
	if err != nil {
		return err
	}
	if !ok {
		c.log.Debug("bulk Set rejected; seeding singles", Fields{"bulkKey": bk})
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
	k := c.singleKey(key)
	return c.snapshotGen(k)
}

// SnapshotGens returns current generations for multiple keys.
func (c *cache[V]) SnapshotGens(keys []string) map[string]uint64 {
	storage := make([]string, len(keys))
	for i, k := range keys {
		storage[i] = c.singleKey(k)
	}
	m, err := c.gen.SnapshotMany(context.Background(), storage)
	if err != nil {
		// conservative fallback: one by one
		out := make(map[string]uint64, len(keys))
		for _, k := range keys {
			out[k] = c.SnapshotGen(k)
		}
		return out
	}
	out := make(map[string]uint64, len(keys))
	for _, k := range keys {
		out[k] = m[c.singleKey(k)]
	}
	return out
}

func (c *cache[V]) snapshotGen(storageKey string) uint64 {
	g, err := c.gen.Snapshot(context.Background(), storageKey)
	if err != nil {
		// Conservative: treat as 0 so CAS writes will skip; reads will self-heal
		c.log.Warn("gen snapshot error", Fields{"key": storageKey, "err": err})
		return 0
	}
	return g
}

func (c *cache[V]) bumpGen(storageKey string) uint64 {
	g, err := c.gen.Bump(context.Background(), storageKey)
	if err != nil {
		c.log.Error("gen bump error", Fields{"key": storageKey, "err": err})
		return 0
	}
	return g
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

// bulkValid validates that each bulk item’s generation equals the current generation.
func (c *cache[V]) bulkValid(items []wire.BulkItem) bool {
	for _, it := range items {
		if it.Gen != c.snapshotGen(c.singleKey(it.Key)) {
			return false
		}
	}
	return true
}

func isLocalGenStore(gs gen.GenStore) bool {
	_, ok := gs.(*gen.LocalGenStore)
	return ok
}
