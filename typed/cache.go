// Package typed adds typed keys, jittered TTLs and metrics to cascache.
package typed

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
)

// Cache wraps [cascache.Cache] with keys of type K.
type Cache[K comparable, V any] struct {
	cache   *cascache.Cache[V]
	key     func(K) string
	metrics Metrics
}

// New creates a cache over a caller-owned backend. For a process-local backend:
//
//	b, err := backend.NewLocal(store, backend.LocalOptions{})
//
// For local values with shared invalidation:
//
//	b, err := redis.NewShared(store, client, redis.Options{})
func New[K comparable, V any](opts Options[K, V]) (*Cache[K, V], error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if opts.Backend == nil {
		return nil, errors.New("typed: backend is required")
	}
	return build(opts, opts.Backend)
}

// NewRedis creates a cache that stores values and invalidation state atomically
// in Redis. It does not close the client.
func NewRedis[K comparable, V any](
	client goredis.UniversalClient,
	opts Options[K, V],
) (*Cache[K, V], error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := opts.checkInvalidationTTL(redisbackend.DefaultInvalidationTTL); err != nil {
		return nil, err
	}

	b, err := redisbackend.New(client, redisbackend.Options{InvalidationTTL: opts.InvalidationTTL})
	if err != nil {
		return nil, err
	}
	return build(opts, b)
}

func build[K comparable, V any](opts Options[K, V], b backend.Backend) (*Cache[K, V], error) {
	core, err := cascache.New(opts.coreOptions(b))
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{cache: core, key: opts.KeyFunc, metrics: opts.Metrics}, nil
}

// Enabled reports whether the cache stores and serves values.
func (c *Cache[K, V]) Enabled() bool { return c.cache.Enabled() }

// Get returns the value stored for key, if one is present and still current.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if !c.cache.Enabled() {
		return zero, false, nil
	}

	v, ok, err := c.cache.Get(ctx, c.key(key))
	switch {
	case err != nil:
		if c.metrics.Error != nil {
			c.metrics.Error(cascache.OpGet, err)
		}
	case ok:
		if c.metrics.Hit != nil {
			c.metrics.Hit()
		}
	default:
		if c.metrics.Miss != nil {
			c.metrics.Miss()
		}
	}
	return v, ok, err
}

// Load returns a cached value, or calls load after a miss. Fills use the
// cache's jittered TTL.
func (c *Cache[K, V]) Load(ctx context.Context, key K, load cascache.Loader[V]) (V, error) {
	return c.cache.Load(ctx, c.key(key), load)
}

// Snapshot returns the invalidation state to pair with [Cache.Set]. Take it
// before reading the source.
func (c *Cache[K, V]) Snapshot(ctx context.Context, key K) (cascache.Snapshot, error) {
	snapshot, err := c.cache.Snapshot(ctx, c.key(key))
	if err != nil && c.metrics.Error != nil {
		c.metrics.Error(cascache.OpSnapshot, err)
	}
	return snapshot, err
}

// Set stores value while snapshot is still current, at the cache's own jittered
// TTL. It is the TTL [Cache.Load] gives its fills.
func (c *Cache[K, V]) Set(
	ctx context.Context,
	key K,
	value V,
	snapshot cascache.Snapshot,
) (cascache.SetResult, error) {
	res, err := c.cache.Set(ctx, c.key(key), value, snapshot)
	c.metrics.observeSet(res, err)
	return res, err
}

// SetWithTTL is [Cache.Set] at a TTL of the caller's choosing. A zero ttl uses
// MaxTTL.
func (c *Cache[K, V]) SetWithTTL(
	ctx context.Context,
	key K,
	value V,
	snapshot cascache.Snapshot,
	ttl time.Duration,
) (cascache.SetResult, error) {
	res, err := c.cache.SetWithTTL(ctx, c.key(key), value, snapshot, ttl)
	c.metrics.observeSet(res, err)
	return res, err
}

// Invalidate makes the cached value for key unusable. Retry errors because the
// invalidation may not have taken effect.
func (c *Cache[K, V]) Invalidate(ctx context.Context, key K) error {
	if !c.cache.Enabled() {
		return nil
	}

	if err := c.cache.Invalidate(ctx, c.key(key)); err != nil {
		if c.metrics.Error != nil {
			c.metrics.Error(cascache.OpInvalidate, err)
		}
		return err
	}
	if c.metrics.Invalidated != nil {
		c.metrics.Invalidated()
	}
	return nil
}

// Invalidator returns a handle that invalidates this cache's keys without
// knowing its value type. Keys still go through this cache's KeyFunc.
func (c *Cache[K, V]) Invalidator() *Invalidator[K] {
	return &Invalidator[K]{
		inv:           c.cache.Invalidator(),
		key:           c.key,
		onError:       c.metrics.Error,
		onInvalidated: c.metrics.Invalidated,
	}
}

// Invalidator retires entries without knowing the cache's value type.
type Invalidator[K comparable] struct {
	inv           *cascache.Invalidator
	key           func(K) string
	onError       func(cascache.Op, error)
	onInvalidated func()
}

// Invalidate makes the cached value for key unusable. It records the same
// metrics [Cache.Invalidate] does.
func (i *Invalidator[K]) Invalidate(ctx context.Context, key K) error {
	if !i.inv.Enabled() {
		return nil
	}

	if err := i.inv.Invalidate(ctx, i.key(key)); err != nil {
		if i.onError != nil {
			i.onError(cascache.OpInvalidate, err)
		}
		return err
	}
	if i.onInvalidated != nil {
		i.onInvalidated()
	}
	return nil
}
