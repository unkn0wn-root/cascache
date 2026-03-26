# CasCache

CasCache is a Go cache library where invalidation is part of correctness.

The problem it is built to solve:

1. request A reads old data
2. request B updates and invalidates the cache
3. request A finishes later and tries to write the old data back into the cache

A plain `DEL` followed by a later `SET` does not prevent that race. CasCache does. It snapshots per-key version state before a fill, only stores if that snapshot is still current, and validates cached values again on read before serving them.

## Why use it

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
2. read from the source of truth
3. `SetIfVersion`

The normal write path is:

1. write to the source of truth
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

	// A version mismatch here is normal under contention.
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

CasCache can be used in a few different shapes. The right choice depends on where values live and whether replicas need shared freshness decisions.

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
- the Redis-native single-key compare-and-write path
- the Redis-native single-key invalidate path

Single-key `SetIfVersion` and `Invalidate` are atomic inside Redis. Batch entries are still validated on read rather than written as one globally atomic multi-key transaction.

## Batch APIs

CasCache also supports grouped batch entries:

- `GetMany`
- `SnapshotVersions`
- `SetIfVersions`

Batch reads are optimistic but conservative:

- the cache first tries the grouped batch entry
- every requested member is checked against current authoritative version state
- undecodable, incomplete, or stale batches are rejected
- the cache falls back to single-key reads when needed

Batch writes are about efficiency, not stronger atomicity. A successful batch write stores one combined value, but freshness is still enforced per member.

The default seed behavior is:

- `BatchReadSeedOff`
- `BatchWriteSeedStrict`

That keeps reads simple by default and preserves per-key CAS checks when singles are materialized after a successful batch write.

## Strict mode and retention

> [!IMPORTANT]
> The default behavior is strict and safe.
>
> - `cascache.New(...)` uses `version.NewLocal()`, which does not run cleanup.
> - `redis.Options.VersionTTL` defaults to `0`, which means authoritative version state does not expire automatically.
>
> Keep those defaults if you want the strongest CAS semantics.

CasCache can also be configured to bound version metadata growth:

- Redis version expiry via `redis.Options.VersionTTL`
- local in-process cleanup via `version.NewLocalWithCleanup(...)`

Those options are valid, but they are not free. They turn the version store into a retention-bounded mode rather than a fully strict one.

What changes when version state is allowed to disappear:

- old cached values are still rejected conservatively on read once version state is gone
- very old `missing` snapshots can become valid again after that key's version state has been removed

In practice:

- use persistent version state for strict correctness
- treat `VersionTTL` and local cleanup as advanced retention options

If you run several replicas and correctness matters across restarts, use a shared persistent version store such as Redis.

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

## Hooks and observability

CasCache exposes a small hook surface for high-signal operational events such as:

- self-healed corrupt or stale entries
- rejected batches
- provider write rejections
- version-store snapshot and bump errors
- invalidate outages

Helpful optional packages in this repository:

- `hooks/slog` for structured logging
- `hooks/async` for non-blocking hook fan-out

Hooks should stay cheap and non-blocking. If they can block, wrap them in `hooks/async`.

## Summary

- If the source-of-truth write succeeds, `Invalidate` is part of correctness.
- If several replicas need shared freshness decisions, do not rely on the default local version store.
- If values live in Redis, prefer `redis.New(...)` over manual wiring.
- If you want per-node hot reads in local memory, keep values in Ristretto or BigCache and share only version state through `redis.NewVersionStore(...)`.

CasCache is for the cases where "cache invalidation is hard" is not a joke but an actual correctness requirement.

If you follow the normal flow:

- `SnapshotVersion`
- load from the source of truth
- `SetIfVersion`
- `Invalidate` after successful writes

then stale writers are contained, cache hits are validated before they are served, and failures degrade to misses instead of uncertain freshness.
