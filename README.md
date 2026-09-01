# cascache

`cascache` is a Go cache that keeps an older in-flight load from restoring
invalidated data. After an invalidation succeeds, cached copies written under
the previous state are no longer returned.

Values can live in the current process, in Redis, or in local stores that share
invalidation state through Redis.

Each key has an invalidation token. A cached value records the token that was
current when it was written and is returned only while that token still
matches. The `backend` package calls this token a fence.

## Installation

```sh
go get github.com/unkn0wn-root/cascache/v4
```

Import the package as:

```go
import "github.com/unkn0wn-root/cascache/v4"
```

Version 4 uses a new module path, API, storage key layout, and wire format. See
the [changelog](CHANGELOG.md) when upgrading from version 3.

## Why

A normal cache-aside write can restore old data after an invalidation:

1. Request A starts reading a value from the source.
2. Request B updates the source and invalidates the cache.
3. Request A finishes later and writes its older value into the cache.

With a normal cache `SET`, the last write wins. CASCache takes a snapshot of the
key's invalidation state before reading the source and checks it again before
storing the result. If the key was invalidated during the read, the cache
refuses the write.

A completed invalidation also makes existing copies unusable, including copies
held by other processes that share the same invalidation state.

## Quick start

This example creates a process-local cache backed by Ristretto:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

type User struct {
	ID   string
	Name string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	store, err := ristretto.New(ristretto.Config{
		NumCounters: 10_000,
		MaxCost:     64 << 20,
		BufferItems: 64,
	})
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	b, err := backend.NewLocal(store, backend.LocalOptions{})
	if err != nil {
		return err
	}
	defer b.Close()

	cache, err := cascache.New(cascache.Options[User]{
		Namespace:  "users",
		Backend:    b,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
	})
	if err != nil {
		return err
	}

	user, err := cache.Load(ctx, "42", func(context.Context) (User, error) {
		return User{ID: "42", Name: "Ada"}, nil
	})
	if err != nil {
		return err
	}

	fmt.Println(user.Name)
	return nil
}
```

`Load` is the normal read path. Concurrent calls for the same missing key share
one loader run. Cache read and write errors do not hide a value returned by the
loader; use `Options.OnLoad` or an observer to record them.

See [EXAMPLE.md](EXAMPLE.md) for a complete repository example with reads,
updates, and invalidation.

## Updating and invalidating

Update the source first, then invalidate the cached value:

```go
if err := db.UpdateUser(ctx, user); err != nil {
	return err
}
return cache.Invalidate(ctx, user.ID)
```

`Invalidate` changes the key's invalidation state before removing the stored
value. A load already in progress cannot write an older value back and have it
served.

Retry an `Invalidate` error because the invalidation may not have taken effect.
If invalidation succeeds but cleanup fails, the old value is already unusable.
The cleanup failure is reported as `EventCleanupFailed`.

A service that only invalidates values does not need their type or codec:

```go
inv, err := cascache.NewInvalidator(cascache.InvalidatorOptions{
	Namespace: "users",
	Backend:   b,
})
```

`Cache.Invalidator` returns the same kind of handle for an existing cache.

## Manual fills

Use `Load` for the usual cache-aside flow. If you need to manage the read
yourself, take the snapshot before reading the source:

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

If the key is invalidated between `Snapshot` and `Set`, the write returns
`SetOutcomeConflict`. This is not an error; the value was already stale and was
not cached.

## Backend choices

The cache stores values and invalidation state through a `backend.Backend`.

| Setup | Constructor | Values | Invalidation state |
| --- | --- | --- | --- |
| One process | `backend.NewLocal` | Caller-owned store | Process memory |
| Shared Redis | `backend/redis.New` | Redis | Redis |
| Local values across replicas | `backend/redis.NewShared` | Caller-owned store | Redis |

Use `backend/redis.New` when values and invalidation state belong in the same
Redis instance. It updates them atomically and needs fewer round trips.

Use `backend/redis.NewShared` when each replica keeps values locally but all
replicas must observe the same invalidations. Each process removes its own
outdated values when they are read.

The `provider` packages adapt BigCache, Redis, and Ristretto as value stores.
`provider/redis` stores values only; it is not the same as the Redis backend.

## Redis reads must use the primary

> [!WARNING]
> The Redis client used by a CASCache backend must route reads to the current
> primary.

Replication lag can return an old value together with its matching old fence.
The fence check then succeeds even though the data is outdated. A read from a
lagging replica is therefore unsafe.

Use a primary or writer endpoint. With go-redis:

- leave `ReplicaOnly` false for Sentinel clients;
- leave `ReadOnly`, `RouteByLatency`, and `RouteRandomly` false for Cluster and
  Universal clients;
- treat `backend/redis.ErrReplicaReads` as a startup configuration error.

Replication can still be used for availability and failover. Cache operations
must use whichever node is the current primary.

The Redis backend rejects replica-read settings it can detect unless
`AllowReplicaReads` is set. It cannot detect every managed-service reader
endpoint, proxy, or custom client. Enabling replica reads gives up the guarantee
that later reads observe a completed invalidation.

## Failure behavior

If the cache cannot prove that a value is current, it returns a miss instead of
the value. Missing, expired, or evicted invalidation state therefore lowers the
hit rate but does not make an old value current again.

Fences have no valid zero value and are never reused. Losing fence state cannot
make an older value current.

Invalidation state must live at least as long as its cached values. Built-in
backends shorten value TTLs when needed and report the applied TTL through
`SetResult.EffectiveTTL`. Frequent `RejectStateMissing` events usually mean the
invalidation lifetime is too short.

CASCache only knows about changes followed by a successful `Invalidate` call.
Changes made without invalidation are governed by the value's TTL.

## Loader behavior

Canceling one caller does not cancel a loader run that still has other callers.
A run ends when no callers remain or `Options.LoadTimeout` expires. Loaders must
honor their context.

A loader that ignores cancellation may overlap a later run for the same key. A
late result can still be returned to callers waiting for it, but it is not given
a new cache lifetime. Loader panics return a `*PanicError`.

## Typed keys and metrics

The `typed` package adds typed keys, TTL jitter, and metric callbacks:

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

Jitter spreads expiration times so entries written together do not all reload
at once.

Core cache events are sent to an `Observer`. The `hooks/slog` package logs them,
`hooks/async` moves observation to a bounded background queue, and
`MultiObserver` combines observers. Observers should ignore event types they do
not recognize.

## Ownership

The cache does not close dependencies supplied by the caller. Close backends,
stores, and Redis clients you own. In particular, close `backend.Local` to stop
its cleanup goroutine; closing it does not close the value store.

## Custom backends

Implement `backend.Backend` to add another storage arrangement. The interface
documents its ordering, concurrency, and ownership requirements. Use
`backendtest.TestBackend` to run the shared conformance suite.

`backend.FenceStore` and `backend.NewComposite` are available when values and
invalidation state need separate stores. Most cache users do not need them.

## Further reading

- [Complete example](EXAMPLE.md)
- [Benchmarks](_benchmarks/README.md)
- [Changelog](CHANGELOG.md)
- [Package documentation](https://pkg.go.dev/github.com/unkn0wn-root/cascache/v4)
