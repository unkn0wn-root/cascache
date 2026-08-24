package cascache

import (
	"cmp"
	"context"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/flight"
	"github.com/unkn0wn-root/cascache/v4/internal/keyspace"
	"github.com/unkn0wn-root/cascache/v4/internal/typednil"
)

// Cache stores values of type V and serves one only while no invalidation has
// retired it. It is safe for concurrent use if its codec and callbacks are. The
// caller owns its dependencies.
type Cache[V any] struct {
	// Invalidation does not depend on V.
	inv *Invalidator

	codec      codec.Codec[V]
	ttl        time.Duration
	cost       SetCostFunc
	computeTTL TTLFunc
	onLoad     LoadFunc

	fills flight.Group[fillResult[V]]
}

// New creates a cache. The zero Cache is not usable.
func New[V any](opts Options[V]) (*Cache[V], error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	c := &Cache[V]{
		inv: &Invalidator{
			space:    keyspace.New(opts.Namespace),
			backend:  opts.Backend,
			disabled: opts.Disabled,
			observer: nilIfNil(opts.Observer),
		},
		codec:      opts.Codec,
		ttl:        cmp.Or(opts.DefaultTTL, DefaultEntryTTL),
		cost:       opts.ComputeSetCost,
		computeTTL: opts.ComputeTTL,
		onLoad:     opts.OnLoad,
	}
	if c.cost == nil {
		c.cost = func(string, []byte) int64 { return 1 }
	}
	if c.computeTTL == nil {
		c.computeTTL = func() (time.Duration, error) { return c.ttl, nil }
	}

	c.fills.Timeout = opts.LoadTimeout
	c.fills.OnPanic = c.observePanic

	return c, nil
}

// Catch typed nils during construction.
func isNil(v any) bool { return typednil.Is(v) }

// Flatten typed nils so call sites need one nil check.
func nilIfNil(o Observer) Observer {
	if isNil(o) {
		return nil
	}
	return o
}

// Enabled reports whether the cache stores and serves values.
func (c *Cache[V]) Enabled() bool { return c != nil && !c.inv.disabled }

// Get returns the cached value for key, if one is present and still current.
// Invalid or outdated entries are misses and are removed when possible. Backend
// read failures are returned as errors.
func (c *Cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if c.inv.disabled {
		return zero, false, nil
	}

	bkey := c.inv.space.Key(key)
	r, err := c.inv.backend.Read(ctx, bkey)
	if err != nil {
		return zero, false, c.opErr(OpGet, key, err)
	}

	e := c.decode(ctx, key, bkey, r)
	return e.val, e.ok, nil
}

// Snapshot returns the invalidation state to pair with [Cache.Set]. Take it
// immediately before reading the source. An invalidation during that read makes
// the snapshot stale and causes the later write to be refused.
func (c *Cache[V]) Snapshot(ctx context.Context, key string) (Snapshot, error) {
	if c.inv.disabled {
		return Snapshot{}, nil
	}

	fence, err := c.inv.backend.Ensure(ctx, c.inv.space.Key(key), backend.NewFence())
	if err != nil {
		return Snapshot{}, c.opErr(OpSnapshot, key, err)
	}
	if !fence.Valid() {
		return Snapshot{}, c.opErr(OpSnapshot, key, ErrBackendContract)
	}
	return Snapshot{fence: fence}, nil
}

// Set stores value while snapshot is still current, at the cache's own TTL: the
// one [Options.ComputeTTL] returns, or [Options.DefaultTTL] when it is not set.
// It is the TTL [Cache.Load] gives its fills.
// A snapshot that is no longer current returns [SetOutcomeConflict], not an
// error: the value was already stale when it arrived, and declining to cache it
// is the point.
func (c *Cache[V]) Set(
	ctx context.Context,
	key string,
	value V,
	snapshot Snapshot,
) (SetResult, error) {
	if c.inv.disabled {
		return SetResult{Outcome: SetOutcomeDisabled}, nil
	}

	ttl, err := c.computeTTL()
	if err != nil {
		return SetResult{}, c.opErr(OpComputeTTL, key, wrapComputeTTL(err))
	}
	return c.SetWithTTL(ctx, key, value, snapshot, ttl)
}

// SetWithTTL is [Cache.Set] at a TTL of the caller's choosing. A zero ttl uses
// [Options.DefaultTTL] and [NoExpiration] asks for no expiry; the backend may
// shorten either so a value cannot outlive the invalidation state that judges it.
func (c *Cache[V]) SetWithTTL(
	ctx context.Context,
	key string,
	value V,
	snapshot Snapshot,
	ttl time.Duration,
) (SetResult, error) {
	if c.inv.disabled {
		return SetResult{Outcome: SetOutcomeDisabled}, nil
	}

	req, err := c.prepareWrite(key, value, snapshot, ttl)
	if err != nil {
		return SetResult{}, c.opErr(OpSet, key, err)
	}

	res, err := c.inv.backend.CompareAndStore(ctx, req)
	if err != nil {
		return SetResult{}, c.opErr(OpSet, key, err)
	}
	return c.setResult(key, res)
}

// Invalidate makes the cached value for key unusable. See
// [Invalidator.Invalidate].
func (c *Cache[V]) Invalidate(ctx context.Context, key string) error {
	return c.inv.Invalidate(ctx, key)
}

// Invalidator returns a handle that can invalidate keys without knowing V.
func (c *Cache[V]) Invalidator() *Invalidator { return c.inv }

func (c *Cache[V]) observe(e Event) { c.inv.observe(e) }

func (c *Cache[V]) opErr(op Op, key string, err error) error { return c.inv.opErr(op, key, err) }

func (c *Cache[V]) observePanic(key string, value any, stack []byte) {
	c.observe(Event{
		Type: EventLoaderPanic,
		Op:   OpLoad,
		Key:  key,
		Err:  &PanicError{Value: value, Stack: stack},
	})
}
