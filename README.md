# cascache

A Go cache that does not return a cached value after it has been invalidated.
Values can be stored in process or in Redis. Replicated services can also keep
values in process while sharing invalidation state through Redis.

Every key has **invalidation state**, represented by a 128-bit token. Each
cached value stores the token that was current when it was written. The value
is returned only if that token is still current. Invalidating a key creates a
new token, which makes existing cached copies invalid, including copies in
other replicas. The `backend` package calls this token a *fence*.

```go
import "github.com/unkn0wn-root/cascache/v4"
```

> [!WARNING]
> v4 is a complete redesign. If you're coming from v3, expect these breaking
> changes:
>
> - The module path is now `github.com/unkn0wn-root/cascache/v4`.
> - The cache API is built around `Cache[V]`, `Load`, `Snapshot`, and `Set`.
>   Constructors, options, reads, and writes have changed.
> - The batch and read-guard APIs have been removed.
> - Backend contracts now live under `backend`, and Redis backends live under
>   `backend/redis`. The old `redis` and `version` packages have been removed.
> - Storage keys use the `cas:v4` prefix and entries use wire format version 4,
>   so cached v3 entries are not reused.
>
> v4 is now the stable, long-lived API. Fixes and compatible
> additions may be released within v4, but any future breaking API change would
> require a new major version.

## Why

A normal cache-aside write can restore old data after an invalidation:

1. Request A starts reading a value from the source.
2. Request B updates the source and invalidates the cache.
3. Request A finishes later and writes its older value into the cache.

With an ordinary cache `SET`, the older write can silently win. CASCache takes
a snapshot of the key's invalidation state before reading the source and checks
it again before storing the result. If the key changed while the read was in
progress, the write is refused.

A successful invalidation also makes older cached copies unusable, even when
deleting them fails. CASCache only knows about changes followed by a successful
`Invalidate` call, and it cannot make an old response from the source current.

## Safe failure behavior

If the cache cannot confirm that a value is current, it treats the value as a
miss. This includes invalidation state that is missing, expired or evicted, as
well as failed cleanup. The value is loaded again instead of being returned.
This works because tokens have no valid zero value and are never reused.

Redis replica reads are the exception. Replication lag can return an old value
and its matching old token, so the cache cannot tell that the data is outdated.
The Redis backend rejects known replica-read configurations unless they are
explicitly allowed.

## Quick start

```go
package main

import (
	"context"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	rp "github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

type User struct {
	ID   string
	Name string
}

func newUserCache() (*cascache.Cache[User], *backend.Local, error) {
	store, err := rp.New(rp.Config{
		NumCounters: 1_000_000,
		MaxCost:     64 << 20,
		BufferItems: 64,
	})
	if err != nil {
		return nil, nil, err
	}

	b, err := backend.NewLocal(store, backend.LocalOptions{})
	if err != nil {
		return nil, nil, err
	}

	cache, err := cascache.New(cascache.Options[User]{
		Namespace:  "user",
		Backend:    b,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
	})
	if err != nil {
		_ = b.Close()
		return nil, nil, err
	}
	return cache, b, nil
}

func readUser(ctx context.Context, cache *cascache.Cache[User], id string) (User, error) {
	return cache.Load(ctx, id, func(ctx context.Context) (User, error) {
		return db.ReadUser(ctx, id)
	})
}
```

`Load` is the normal read path. If several callers request the same missing key
at the same time, the cache runs the loader once and returns the result to all
of them. If the loader returns a value, a cache read or write error does not
hide it.

After an invalidation completes, a later load will not return the old value. If
a shared loader call started earlier and its result cannot be confirmed as
current, the caller starts or joins another call. Changes made without calling
`Invalidate` still depend on the TTL.

A caller can stop waiting without canceling a loader call that other callers
are still using. `Options.LoadTimeout` applies to each loader call, so it may
apply twice when a value has to be loaded again. A loader panic is returned as
a `*PanicError`, which unwraps to `ErrLoaderPanic` and includes the stack.

## Invalidation

Update the source first, then invalidate:

```go
if err := db.UpdateUser(ctx, user); err != nil {
	return err
}
return cache.Invalidate(ctx, user.ID)
```

`Invalidate` updates the key's invalidation state before removing the cached
value. This prevents a load already in progress from writing an older value
back to the cache. Copies in other replicas also stop being returned, even if
they have not been deleted yet.

Retry if `Invalidate` returns an error. The invalidation state may not have
changed, so the old value may still be returned. If the state changed but
deleting the value failed, `Invalidate` does not return that cleanup error. The
remaining value no longer passes the token check. The failure is reported as
`EventCleanupFailed`, and a later read tries to remove the value again.

A service that only invalidates values does not need their type or codec:

```go
inv, err := cascache.NewInvalidator(cascache.InvalidatorOptions{
	Namespace: "user",
	Backend:   b,
})
```

`Cache.Invalidator()` returns the same handle for a cache you already have.

## Manual cache fills

`Load` handles the normal cache-fill sequence. When you need to manage that
sequence yourself, take the snapshot **before** reading the source:

```go
snapshot, err := cache.Snapshot(ctx, key)
if err != nil {
	return err
}

value, err := loadFromSource(ctx, key)
if err != nil {
	return err
}

res, err := cache.Set(ctx, key, value, snapshot)
```

The order matters. If the key is invalidated between the snapshot and the
write, the invalidation state changes and the write is refused. Taking the
snapshot after reading the source could allow an older value to be cached.

If the write is refused, `Set` returns `SetOutcomeConflict` rather than an
error. The value was not cached because the key changed after the snapshot.

## Backend setups

The cache stores data through a `backend.Backend`. There are three common ways
to configure it:

```go
import (
	"github.com/unkn0wn-root/cascache/v4/backend"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
)

// One process: values and invalidation state both in memory.
b, _ := backend.NewLocal(store, backend.LocalOptions{})

// Many replicas, sharing values and invalidation state in Redis. Each atomic
// operation uses one server-side script.
b, _ := redisbackend.New(client, redisbackend.Options{})

// Many replicas, each keeping values in memory with shared invalidation state.
// Reads stay in the process, but invalidation applies to every copy.
b, _ := redisbackend.NewShared(store, client, redisbackend.Options{})
```

`backend/redis` is the Redis *backend*. `provider/redis` is only a plain value
store. When values and invalidation state live in the same Redis, prefer
`backend/redis.New`, which needs fewer round trips and updates them atomically.

With `NewShared`, each replica can delete only its own in-memory copy. Copies in
other replicas no longer match the shared invalidation state and are discarded
when they are read.

## Redis reads must go to the primary

The Redis client passed to CASCache **must route reads to the current primary**.
After an invalidation, a lagging replica may still return both the old value and
its matching old fence. The fence check succeeds even though the data is old,
so CASCache may return it.

Replication is still recommended for high availability. Replicas may be
promoted during failover; CASCache operations must simply use whichever node is
the current primary. A cache error or source reload during failover is safe. A
read from a lagging replica is not.

The examples below import `github.com/redis/go-redis/v9` as `goredis`.

For standalone or managed Redis, use the primary or writer endpoint, never a
reader endpoint:

```go
client := goredis.NewClient(&goredis.Options{
	Addr: "redis-primary:6379",
})
```

With Sentinel, use primary discovery and leave `ReplicaOnly` false:

```go
client := goredis.NewFailoverClient(&goredis.FailoverOptions{
	MasterName:    "mymaster",
	SentinelAddrs: []string{"sentinel-1:26379", "sentinel-2:26379"},
	ReplicaOnly:   false,
})
```

With Redis Cluster, leave every replica-read option false:

```go
client := goredis.NewClusterClient(&goredis.ClusterOptions{
	Addrs: []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"},

	ReadOnly:       false,
	RouteByLatency: false,
	RouteRandomly:  false,
})
```

The same fields must remain false on `goredis.UniversalOptions`. In Sentinel
mode, its `ReadOnly` field becomes `ReplicaOnly`. `RouteByLatency` and
`RouteRandomly` automatically enable replica reads for Cluster clients.

Prefer `typed.NewRedis` for application caches; it does not expose an option to
permit replica reads. When using `redisbackend.New` or
`redisbackend.NewShared`, leave `AllowReplicaReads` false and treat
`redisbackend.ErrReplicaReads` as a startup configuration failure. CASCache can
detect known `goredis.ClusterClient` replica-read settings, but it cannot detect
a managed-service reader endpoint, an external read-routing proxy, or every
custom/Sentinel configuration.

You can also provide your own backend. `backend.Backend` documents the required
behavior. Use `backendtest.TestBackend` to check your implementation.
`backend.FenceStore` and `backend.NewComposite` are for custom backends. Most
cache users do not need them.

## Invalidation lifetime

Invalidation state may expire. If it is missing, the cache returns a miss. Its
lifetime therefore affects the hit rate, not whether old data can be returned.
The built-in backends give it a limited lifetime by default.

The invalidation state must live at least as long as the cached value. The
backend enforces this by shortening the value's TTL when needed and reports the
TTL it applied:

```go
res, _ := cache.SetWithTTL(ctx, key, value, snapshot, 48*time.Hour)
res.EffectiveTTL // clamped to the invalidation lifetime
```

Track `RejectStateMissing` in your metrics. Frequent events mean values are
being discarded because their invalidation state is missing, which lowers the
hit rate.

## Monitoring

Cache events are sent to an `Observer`:

```go
cascache.Options[User]{
	Observer: cascache.ObserverFunc(func(e cascache.Event) {
		log.Printf("%s %s %s: %v", e.Type, e.Op, e.Reason, e.Err)
	}),
}
```

`hooks/slog` logs events and redacts keys by default. `hooks/async` runs an
observer from a bounded background queue instead of inside the cache call.
`MultiObserver` combines several observers. New event types may be added, so
ignore unknown types rather than treating them as errors.

`Options.OnLoad` reports completed loads separately, including hits, misses,
writes attributed to the load, and timing.

## Typed keys

`typed` adds keys of your own type, jittered TTLs and metrics:

```go
cache, err := typed.NewRedis(client, typed.Options[uuid.UUID, Frame]{
	Config: typed.Config{
		Namespace: "gw:users",
		MaxTTL:    10 * time.Minute,
		MinTTL:    8 * time.Minute,
		Jitter:    0.2,
		Metrics:   metrics,
	},
	KeyFunc: func(id uuid.UUID) string { return id.String() },
	Codec:   codec.JSON[Frame]{},
})
```

Jitter spreads expiration times, so entries written together do not all reload
at once.

## Ownership

The cache does not close anything passed to it. You are responsible for closing
backends, stores, and Redis clients. `backend.Local` runs a cleanup goroutine,
so close it when you are done. Closing it does not close the value store.

## Storage layout

An entry occupies two keys that share a Redis Cluster hash tag, so one MGET or
one script can touch the pair:

```
cas:v4:val:{tag}:s:<len(namespace)>:<namespace>:<key>
cas:v4:fen:{tag}:s:<len(namespace)>:<namespace>:<key>
```

The `v4` prefix identifies the storage layout version, not the module version.
Encoding the namespace length prevents collisions between caches that share a
store.

## Benchmarks

The benchmarks in [`_benchmarks`](_benchmarks/README.md) use one cached
256-byte value. The local cases use a simple in-memory store. These results are
the median of 5 x 1s runs on an Apple M4 Max with Go 1.26.0.
Redis 8.10.1 was running in Docker on the same machine.

### Read

| Read | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| CASCache local | 215 | 704 | 5 |
| CASCache Redis | 142,360 | 1,392 | 19 |
| Plain Redis `GET` | 131,714 | 456 | 6 |

### Set

| Set | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| CASCache local | 225 | 736 | 5 |
| CASCache Redis | 142,487 | 1,032 | 20 |
| Plain Redis `SET` | 136,250 | 258 | 8 |

### Invalidate

| Invalidate | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| CASCache local | 121 | 176 | 3 |
| CASCache Redis | 119,179 | 672 | 18 |
| Plain Redis `DEL` | 133,926 | 168 | 6 |

The Redis results are close enough to treat as the same range on this setup.
Most of their time is spent on the trip to Redis. Plain Redis `DEL` does not
provide the same invalidation behavior as CASCache. Run the benchmarks on the
same machine before and after a change when you want to compare versions.
