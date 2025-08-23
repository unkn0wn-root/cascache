package cascache

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/yourorg/cascache/internal/util"
	"github.com/yourorg/cascache/internal/wire"
)

const (
	defaultGenRetention = 30 * 24 * time.Hour
	defaultSweep        = time.Hour
)

type genEntry struct {
	Gen       uint64
	UpdatedAt time.Time // set only on bumps
}

type cache[V any] struct {
	ns       string
	provider Provider
	codec    Codec[V]
	log      Logger

	enabled bool

	defaultTTL     time.Duration
	bulkTTL        time.Duration
	sweepInterval  time.Duration
	genRetention   time.Duration
	computeSetCost SetCostFunc

	// generation map (in-memory only; missing treated as gen=0)
	genMu sync.RWMutex
	gens  map[string]genEntry

	// background cleanup
	ticker    *time.Ticker
	stopCh    chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
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
		gens:     make(map[string]genEntry),
	}

	defaultCost := SetCostFunc(func(_ string, _ []byte, _ bool, _ int) int64 { return 1 })
	if opts.ComputeSetCost != nil {
		c.computeSetCost = opts.ComputeSetCost
	} else {
		c.computeSetCost = defaultCost
	}

	c.log = coalesce[Logger](opts.Logger, NopLogger{})
	c.defaultTTL = coalesce[time.Duration](opts.DefaultTTL, 10*time.Minute)
	c.bulkTTL = coalesce[time.Duration](opts.BulkTTL, 10*time.Minute)
	c.sweepInterval = coalesce[time.Duration](opts.CleanupInterval, defaultSweep)
	c.genRetention = coalesce[time.Duration](opts.GenRetention, defaultGenRetention)

	c.enabled = !opts.Disabled

	if c.enabled {
		c.ticker = time.NewTicker(c.sweepInterval)
		c.stopCh = make(chan struct{})
		c.closeWg.Add(1)
		go c.cleanupLoop()
	}
	return c, nil
}

func (c *cache[V]) Enabled() bool { return c.enabled }

func (c *cache[V]) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		if c.stopCh != nil {
			close(c.stopCh)
			c.closeWg.Wait()
			if c.ticker != nil {
				c.ticker.Stop()
			}
		}
	})
	if c.provider != nil {
		return c.provider.Close(ctx)
	}
	return nil
}

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

func (c *cache[V]) GetBulk(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	if !c.enabled {
		// if disabled, everything is missing
		missing := make([]string, 0, len(keys))
		missing = append(missing, keys...)
		return out, missing, nil
	}
	if len(keys) == 0 {
		return out, nil, nil
	}

	// Try bulk entry
	bulkKey := c.bulkKey(keys)
	if raw, ok, err := c.provider.Get(ctx, bulkKey); err == nil && ok {
		items, err := wire.DecodeBulk(raw)
		if err == nil {
			if c.bulkValid(items) {
				// decode and return those we have (keys may be subset/superset)
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

func (c *cache[V]) SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error {
	if !c.enabled || len(items) == 0 {
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
			// skip bulk; seed singles instead
			c.log.Debug("SetBulkWithGens skipped (gen mismatch)", Fields{"key": k})
			for kk2, v := range items {
				if obs2, ok := observedGens[kk2]; ok {
					_ = c.SetWithGen(ctx, kk2, v, obs2, c.defaultTTL)
				}
			}
			return nil
		}
	}

	// encode all into wire bulk
	wireItems := make([]wire.BulkItem, 0, len(items))
	// stable iteration order improves determinism
	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

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
	bk := c.bulkKey(keys)
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

func (c *cache[V]) SnapshotGen(key string) uint64 {
	k := c.singleKey(key)
	return c.snapshotGen(k)
}

func (c *cache[V]) SnapshotGens(keys []string) map[string]uint64 {
	out := make(map[string]uint64, len(keys))
	for _, k := range keys {
		out[k] = c.SnapshotGen(k)
	}
	return out
}

func (c *cache[V]) snapshotGen(storageKey string) uint64 {
	c.genMu.RLock()
	e, ok := c.gens[storageKey]
	c.genMu.RUnlock()
	if !ok {
		return 0
	}
	return e.Gen
}

func (c *cache[V]) bumpGen(storageKey string) uint64 {
	c.genMu.Lock()
	e := c.gens[storageKey]
	e.Gen++
	e.UpdatedAt = time.Now()
	c.gens[storageKey] = e
	c.genMu.Unlock()
	return e.Gen
}

func (c *cache[V]) singleKey(userKey string) string {
	// isolate by namespace
	return "single:" + c.ns + ":" + userKey
}

func (c *cache[V]) bulkKey(userKeys []string) string {
	return util.BulkKey("bulk:"+c.ns, userKeys)
}

func (c *cache[V]) bulkValid(items []wire.BulkItem) bool {
	for _, it := range items {
		if it.Gen != c.snapshotGen(c.singleKey(it.Key)) {
			return false
		}
	}
	return true
}

func (c *cache[V]) cleanupLoop() {
	defer c.closeWg.Done()
	retention := c.genRetention
	for {
		select {
		case <-c.ticker.C:
			c.cleanupOldGenerations(retention)
		case <-c.stopCh:
			return
		}
	}
}

func (c *cache[V]) cleanupOldGenerations(retention time.Duration) {
	cutoff := time.Now().Add(-retention)
	var candidates []string

	c.genMu.RLock()
	for k, e := range c.gens {
		if e.UpdatedAt.IsZero() {
			continue
		}
		if e.UpdatedAt.Before(cutoff) {
			candidates = append(candidates, k)
		}
	}
	c.genMu.RUnlock()

	if len(candidates) == 0 {
		return
	}

	c.genMu.Lock()
	removed := 0
	for _, k := range candidates {
		if e, ok := c.gens[k]; ok && !e.UpdatedAt.After(cutoff) {
			delete(c.gens, k)
			removed++
		}
	}
	c.genMu.Unlock()

	if removed > 0 {
		c.log.Debug("generation cleanup removed stale entries", Fields{"removed": removed})
	}
}
