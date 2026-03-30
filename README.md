# CasCache

CasCache is a Go cache library that prevents stale data from silently winning.

Most caches treat writes as unconditional: you `SET` a value and it sticks until someone deletes it or it expires. That works fine until two requests overlap.

1. request A reads a user record from the database
2. while A is still working, request B updates that same record and invalidates the cache
3. request A finishes and writes the now-outdated record back into the cache

The cache now holds stale data and nobody knows. A `DEL` followed by a `SET` does not prevent this because nothing ties the `SET` back to the state that existed when the read started.

CasCache fixes this by remembering what version of a key you saw before you started your work. When you try to write, it checks whether that version is still current. If something changed in between, the write is rejected. On reads, it checks again - a cached value is only served if it still matches the latest known version. If anything is off, the cache treats it as a miss.

## v3 - breaking change

> [!IMPORTANT]
> This README covers **v3**, which is not compatible with v2. Both the Go API and the on-wire format changed. Cached values written by v2 will **not** decode under v3.

The main change: CAS validation moved from generation counters (a monotonic uint64) to fence tokens (16 bytes of cryptographically random data).

A fence is an opaque random token assigned to a key every time its state changes. Validation is a simple equality check - does the fence embedded in the cached value still match the authoritative fence? If yes, the value is fresh. If not, it is stale. There is no numeric comparison or ordering involved.

## Performance

CasCache does more work than a plain cache. Here is what that costs, measured against plain Redis with the same codec and payload.

### Single-key Redis operations

| Operation | Plain Redis | CasCache | Extra cost | Redis round trips |
| --- | --- | --- | --- | --- |
| Single read (`Get`) | ~17µs | ~33µs | +16µs | 2 (value + fence check) |
| Snapshot then set (full fill) | ~16µs | ~36µs | +20µs | 3 (miss + snapshot + Lua conditional set) |
| Single write (`SetIfVersion`) | ~16µs | ~23µs | +7µs | 1 (Lua script, atomic) |
| Invalidate | ~15µs | ~16µs | +1µs | 1 (Lua script, atomic) |

Single-key reads are the most expensive path because every `Get` needs two round trips: one for the value and one for the authoritative fence. Writes and invalidates use Lua scripts to stay atomic in a single round trip. The extra cost per operation is in the low tens of microseconds.

### Batch operations

| Operation | Extra cost vs plain Redis | Redis round trips |
| --- | --- | --- |
| Batch get (32 small keys) | ~4% | 2 (batch blob + `MGET` all fences) |
| Batch get (32 medium keys) | ~1% | 2 (batch blob + `MGET` all fences) |

Batch reads amortize the fence-check cost across all keys with a single `MGET`, so the overhead nearly disappears.

### E2E comparison at 800 req/s (mixed workload, 60 seconds)

| Metric | CasCache | Naive (plain SET/GET/DEL) |
| --- | --- | --- |
| p50 | 13.8ms | 11.8ms |
| p95 | 217.8ms | 218.7ms |
| p99 | 612.0ms | 614.1ms |
| Stale reads | 0 | 6,339 |

At p50 cascache is about 18% slower. At p95 and p99 the difference disappears because tail latency is dominated by network, Redis, and the database.

## Why

What CasCache does:

- stale writers do not overwrite newer state after a successful invalidate
- single-key reads only serve values that still match current authoritative version state
- batch reads validate every requested member before serving the batch hit
- backend trouble degrades to misses or skipped writes instead of uncertain freshness

What it does not try to do:

- make arbitrary multi-key writes globally atomic
- recover from a source-of-truth write that succeeded when `Invalidate` did not
- turn a local in-process version store into a distributed invalidation system

## How it works

CasCache keeps authoritative version state for every logical key.

The normal fill path is:

1. `SnapshotVersion`
2. do your service, app, business logic (db, API etc.)
3. `SetIfVersion`

The normal write path is:

1. write where you want
2. `Invalidate`

That means the cache never trusts a value just because it exists. A value must still match the current version state when it is read.

## Installation

```bash
go get github.com/unkn0wn-root/cascache/v3
```

## Quick start

### Build a cache

```go
package main

import (
	"context"
	"time"

	"github.com/unkn0wn-root/cascache/v3"
	"github.com/unkn0wn-root/cascache/v3/codec"
	ristrettoprovider "github.com/unkn0wn-root/cascache/v3/provider/ristretto"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newUserCache() (cascache.CAS[User], error) {
	provider, err := ristrettoprovider.New(ristrettoprovider.Config{
		NumCounters: 1_000_000,
		MaxCost:     64 << 20,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	return cascache.New(cascache.Options[User]{
		Namespace:  "user",
		Provider:   provider,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
		BatchTTL:   5 * time.Minute,
	})
}

func main() {
	cache, err := newUserCache()
	if err != nil {
		panic(err)
	}
	defer cache.Close(context.Background())
}
```

### Read path

```go
type UserStore interface {
	Load(ctx context.Context, id string) (User, error)
}

type UserRepo struct {
	Cache cascache.CAS[User]
	Store UserStore
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	if user, ok, err := r.Cache.Get(ctx, id); err != nil {
		return User{}, err
	} else if ok {
		return user, nil
	}

	version, err := r.Cache.SnapshotVersion(ctx, id)
	if err != nil {
		return User{}, err
	}

	user, err := r.Store.Load(ctx, id)
	if err != nil {
		return User{}, err
	}

	// SetIfVersion returns (WriteResult, error). A version mismatch is not
	// an error - it means another request invalidated this key while you were
	// loading. The cache skips the write and the next reader will fill it
	// with fresh data. We already have the value from the database, so we
	// return it either way.
	_, _ = r.Cache.SetIfVersion(ctx, id, user, version, 0)
	return user, nil
}
```

### Write path

```go
type UserWriter interface {
	Save(ctx context.Context, user User) error
}

type UserWriteRepo struct {
	Cache  cascache.CAS[User]
	Writer UserWriter
}

func (r *UserWriteRepo) Save(ctx context.Context, user User) error {
	if err := r.Writer.Save(ctx, user); err != nil {
		return err
	}

	// Treat invalidate failures as real incidents.
	return r.Cache.Invalidate(ctx, user.ID)
}
```

## Choosing a topology

CasCache can be used in a few different shapes.
The right choice depends on where values live and whether replicas need shared freshness decisions.

| Constructor | Use it when | Notes |
| --- | --- | --- |
| `cascache.New(...)` | values live in any supported provider and one process owns freshness decisions | default version store is local and in-process |
| `cascache.New(...)` + `redis.NewVersionStore(...)` | values should stay outside Redis, but replicas must agree on freshness | common pattern for per-node Ristretto or BigCache plus shared Redis version state |
| `redis.New(...)` | both values and version state should live in Redis | preferred Redis entry point; includes Redis-native single-key compare-and-write and invalidate |

Use the lower-level Redis constructors only when you are intentionally composing a custom topology:

- `redis.NewVersionStore(...)`
- `redis.NewProvider(...)`
- `redis.NewKeyMutator(...)`

If values live in Redis, prefer `redis.New(...)`.

## Redis example

```go
package main

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v3/codec"
	cascacheredis "github.com/unkn0wn-root/cascache/v3/redis"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newRedisUserCache() error {
	rdb := goredis.NewClient(&goredis.Options{
		Addr: "127.0.0.1:6379",
	})

	cache, err := cascacheredis.New(cascacheredis.Options[User]{
		Namespace:  "user",
		Client:     rdb,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
		BatchTTL:   5 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer cache.Close(context.Background())

	return nil
}
```

`redis.New(...)` is the recommended entry point when values are stored in Redis. It wires:

- the Redis value provider
- the Redis-backed version store
- the Redis-native single-key "compare-and-write" path
- the Redis-native single-key invalidate path

Single-key `SetIfVersion` and `Invalidate` are atomic inside Redis. Batch entries are still validated on read rather than written as one globally atomic multi-key transaction.

## Batch APIs

CasCache also supports grouped batch entries:

- `GetMany`
- `SnapshotVersions`
- `SetIfVersions`

On read, the cache tries the batch entry first but checks every member against current version state before serving it. If any member is stale, undecodable, or missing, the whole batch is rejected and the cache falls back to single-key reads.

On write, a batch stores all members as one combined value, but each member is still checked individually. Writing as a batch does not make the write atomic across keys.

The default seed behavior is:

- `BatchReadSeedOff`
- `BatchWriteSeedStrict`

That keeps reads simple by default and preserves per-key CAS checks when singles are materialized after a successful batch write.

## Providers

This repository currently includes:

- `provider/ristretto`
- `provider/bigcache`
- `redis` as a Redis-backed provider and full Redis topology

Provider notes:

- Ristretto may reject writes under pressure; CasCache reports that as `provider_rejected`
- BigCache ignores per-entry TTL and uses its global `LifeWindow`
- Redis supports per-entry TTL and the Redis-native single-key mutation path

## Codecs

- `codec.JSON`
- `codec.NewCBOR` / `codec.MustCBOR`
- `codec.Msgpack`
- `codec.NewProtobuf`
- `codec.Bytes`
- `codec.String`

## Hooks

CasCache exposes a small hook surface for operational events such as:

- self-healed corrupt or stale entries
- rejected batches
- provider write rejections
- version-store snapshot and bump errors
- invalidate outages

Helpful but totaly optional packages:

- `hooks/slog` for structured logging
- `hooks/async` for non-blocking hook fan-out

Hooks should stay cheap and non-blocking. If they can block, wrap them in `hooks/async`.

## Summary

- If the source-of-truth write succeeds, `Invalidate` is part of correctness.
- If several replicas need shared freshness decisions, do not rely on the default local version store.
- If values live in Redis, prefer `redis.New(...)` over manual wiring.
- If you want per-node hot reads in local memory, keep values in Ristretto or BigCache and share only version state through `redis.NewVersionStore(...)`.
