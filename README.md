# CasCache

**TL;DR**

If you run multiple pods, replicas, or services that share a cache and you need to make sure a cached key always reflects the latest known state and not something a slow writer quietly overwrote three seconds ago - CasCache is built for that.
It will not serve stale data and it will not let a late write silently win or override. When it cannot prove a value is current, it treats the entry as a miss instead of serving something outdated.

---

Most caches treat writes as unconditional: you `SET` a value and it sticks until someone deletes it or it expires. That works fine until two requests overlap.

1. request A reads a user record from the database
2. while A is still working, request B updates that same record and invalidates the cache
3. request A finishes and writes the now-outdated record back into the cache

The cache now holds stale data and nobody knows. A `DEL` followed by a `SET` does not prevent this because nothing ties the `SET` back to the state that existed when the read started.

CasCache fixes this by remembering what version of a key you saw before you started your work. When you try to write, it checks whether that version is still current. If something changed in between, the write is rejected. On reads, it checks again - a cached value is only served if it still matches the latest known version. If anything is off, the cache treats it as a miss.

## Index

- [Performance](#performance)
  - [Single-key Redis operations](#single-key-redis-operations)
  - [Batch operations](#batch-operations)
  - [50,000 req/s target](#50000-reqs-target-mixed-workload-30-seconds)
- [Why](#why)
- [How it works](#how-it-works)
  - [About source reads](#about-source-reads)
  - [Read guards](#read-guards)
- [Installation](#installation)
- [Quick start](#quick-start)
  - [Build a cache](#build-a-cache)
  - [Read path](#read-path)
  - [Write path](#write-path)
- [Choosing a topology](#choosing-a-topology)
- [Redis example](#redis-example)
- [Batch APIs](#batch-apis)
- [Providers](#providers)
- [Codecs](#codecs)
- [Hooks](#hooks)

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

### 50,000 req/s target (mixed workload, 30 seconds)

Tested on `goos=darwin`, `goarch=arm64`, `cpu=Apple M4 Max`, using 5 API containers plus PostgreSQL and Redis. Both runs used the same mixed workload with a `50,000 req/s` offered load for `30s` and issued `1,500,000` requests. The comparison baseline here is ordinary Redis cache-aside: read miss -> load from Postgres -> store in Redis, write -> update Postgres -> delete the Redis entry.

| Metric | CasCache | Redis cache-aside baseline |
| --- | --- | --- |
| p50 | 1.9ms | 0.8ms |
| p95 | 22.9ms | 15.3ms |
| p99 | 207.8ms | 201.2ms |
| Max | 1337.1ms | 1091.6ms |
| **Stale reads** | **0** | **195,461** |

At p50 the gap is about `1.1ms` (`1.9ms` vs `0.8ms`). At p95 the gap is about `7.6ms` (`22.9ms` vs `15.3ms`). At p99 the gap is about `6.6ms` (`207.8ms` vs `201.2ms`). The Redis cache-aside baseline was faster in this run, but it served `195,461` stale reads while CasCache served `0`.

## Why

What you get:

- a slow writer that finishes after an invalidate cannot silently overwrite the cache with outdated data
- reads only serve values that still match the current version state, so your users do not see stale results just because something was cached
- batch reads check every member before serving the batch, not just the batch key itself
- if Redis or your backend has a bad moment, the cache degrades to misses or skipped writes instead of quietly serving data it cannot verify

What CasCache does not try to solve:

- if your "source-of-truth" write succeeds but `Invalidate` fails, the cache does not know the source moved forward
- it does not prove that the value you just loaded from your database, api etc. was the newest one that existed anywhere - it only proves the cache entry still matches the last known version

## How it works

CasCache keeps authoritative version state for every logical key.

The normal fill path is:

1. `SnapshotVersion`
2. do your service, app, business logic (db, API etc.)
3. `SetIfVersion`

Use `SetIfVersionWithTTL` only when you need to override the cache's default TTL for that write.

The normal write path is:

1. write where you want
2. `Invalidate`

That means the cache never trusts a value just because it exists. A value must still match the current version state when it is read.

### About source reads

CasCache guarantees cache freshness against its authoritative version state. It does not guarantee that the value you just loaded from a database or API was the newest value that existed anywhere.

If a fill reads from a lagging database replica or an "eventually consistent" API, that request can still observe old data from the source. What CasCache prevents is that old data being accepted back into the cache after the key has moved to a new fence. Once version state changes, old cache entries stop validating on read and stale refill attempts are rejected on write.

So the guarantee is closer to "no stale cache refill after a successful invalidate" than "the cache can prove your source read was globally current." If a path needs that stronger guarantee, the fill has to read from an authority that is fresh enough for your consistency model, or use `ReadGuard` / `BatchReadGuard` on critical read paths.

### Read guards

Most paths do **not** need a read guard. The normal version check already guarantees that once a key is invalidated, older cached bytes stop being valid.

`ReadGuard` is a guard function you pass through options that the cache calls on every hit for a given key. At its simplest it is just extra cache-level logic - you look at the decoded value and decide whether it is still good enough to serve.
The tradeoff comes when your guard calls another authority (API, DB, whatever). That is an extra I/O round trip per key, which means more pressure on that authority for every single cache hit and some extra lookup latency on the cache itself.
For batch endpoints, use `BatchReadGuard` so a single source call can validate many keys at once instead of hitting the authority per member.

Typical cases where a read guard makes sense:

- fills come from a lagging replica or eventually consistent API, but one endpoint must only serve values confirmed by a primary or another authoritative source
- the cached value carries a revision, timestamp, or some business state field that must still match source state before use

Example:

```go
type User struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

type UserMetaStore interface {
	CurrentUpdatedAt(ctx context.Context, id string) (time.Time, error)
}

var meta UserMetaStore

cache, err := cascache.New(cascache.Options[User]{
	Namespace: "user",
	Provider:  provider,
	Codec:     codec.JSON[User]{},
	ReadGuard: func(ctx context.Context, key string, cached User) (bool, error) {
		current, err := meta.CurrentUpdatedAt(ctx, key)
		if err != nil {
			return false, err
		}
		return cached.UpdatedAt.Equal(current), nil
	},
})
```

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
	// with fresh data. Here we explicitly ignore the error and return value,
	// but cascache gives you the possibility to handle them if you want to
	// short-circuit, log, or react to a failed cache write.
	_, _ = r.Cache.SetIfVersion(ctx, id, user, version)
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

If values live in Redis, simply use `redis.New(...)` to make things easy for you.

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

## Batch APIs

CasCache also supports grouped batch entries:

- `GetMany`
- `SnapshotVersions`
- `SetIfVersions`
- `SetIfVersionsWithTTL` when you need a per-call TTL override

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
