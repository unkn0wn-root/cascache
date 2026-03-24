# CasCache

`cascache` is a cache library for applications/backends where invalidation is part of correctness, not just best-effort cleanup.

Most caches are fine at storing values, but they do not solve one of the hardest cache bugs: a request can read old data, another request can update the source of truth and invalidate the cache, and then the first request can arrive late and put the old value back.

The usual race usually looks like this:

1. one request reads old data from the database
2. another request updates the database and invalidates the cache
3. the first request finishes later and writes its old data back into the cache

That race is easy to miss, and a plain `DEL` followed by a later `SET` does not prevent it. The cache has no memory that the key was invalidated after the stale reader started.

CasCache is built around one idea: every logical key has a version. Readers return a cached value only if the stored version still matches the current one. Writers snapshot the version before reading the source of truth, then store only if that version is still current. After a successful write to the source of truth, `Invalidate` bumps the version so any older snapshot loses automatically.

The effect of this is:

- single-key reads do not serve values that have already been invalidated
- stale writers do not put old data back into the cache
- version-store trouble degrades to misses or skipped writes instead of "maybe stale"
- TTL stays an eviction policy, not the thing that makes freshness safe

## Why CasCache

CasCache exists for systems where "usually fresh" is not good enough. That includes data such as permissions, profiles, pricing, feature flags, inventory, or any other record where a successful write should take effect immediately from the cache's point of view.

Common cache patterns still leave a stale-data window:

| Pattern | What you do | What still goes wrong |
| --- | --- | --- |
| TTL only | store `user:42` for 5 minutes | readers can see stale data until the TTL expires |
| delete then set | `DEL user:42`, later `SET user:42` | a slower request can repopulate the cache with an older snapshot |
| write-through | update the database, then update the cache | concurrent readers still need perfect coordination to avoid serving old data |
| version inside the value | store `{version, payload}` together | you still need a trusted current version somewhere else |

CasCache changes the contract. Instead of hoping invalidation reaches every writer in time, it validates freshness on read and requires writes to prove they observed the current version before they store anything.

Use it when:

- stale data after invalidation is unacceptable
- several API nodes can race on the same records
- you prefer a cache miss over serving a value that might be stale
- you want local in-memory caches on each node, but shared freshness decisions across nodes

The mental model is:

```text
DB write succeeds  ->  Invalidate(key)
Cache miss path    ->  SnapshotVersion -> load source of truth -> SetIfVersion
Cache hit path     ->  Get validates stored version against the current version
Batch hit path     ->  GetMany validates each requested member or falls back to singles
Multi-node setup   ->  share versions in Redis, or keep both values and versions in Redis
```

## Core idea

The safe pattern is:

1. call `SnapshotVersion`
2. read the source of truth
3. call `SetIfVersion`

After a successful write to the source of truth, call `Invalidate`.

```go
version, err := cache.SnapshotVersion(ctx, key)
if err != nil {
	return err
}

value, err := loadFromDB(ctx, key)
if err != nil {
	return err
}

_, _ = cache.SetIfVersion(ctx, key, value, version, 0)
```

## Choosing right topology

Most confusion looking at this library can come from the Redis related constructors. The short rule is:

- if you are unsure (recommend in multi-pod/container env.), use `cascache/redis.New(...)`
- use `cascache/redis.NewGenStore(...)` only when values should stay outside Redis
- treat `cascache/redis.NewProvider(...)` and `cascache/redis.NewKeyMutator(...)` as advanced composition APIs

CasCache supports three real deployment shapes, and choosing the right one matters more than any individual option:

| Use this | Choose it when | What it means |
| --- | --- | --- |
| `cascache.New(...)` | your cache is local to one process, or each node keeps its own cache and cross-node invalidation is handled elsewhere | values live in your chosen provider, versions live in the built-in local store |
| `cascache.New(...)` + `cascache/redis.NewGenStore(...)` | values should stay in a non-Redis provider such as Ristretto or BigCache, but versions must be shared across processes | Redis stores versions only; values still live in your provider |
| `cascache/redis.New(...)` | both values and versions should live in Redis | the package wires the Redis provider, the Redis version store, and the Redis-native single-key mutation path together |

The important distinction is this:

- `cascache/redis.NewGenStore(...)` gives you shared versions.
- `cascache/redis.New(...)` gives you shared versions and Redis native, single-key, atomic "compare-and-write" / invalidate.

If both values and versions are in Redis, prefer `cascache/redis.New(...)`.

If values are not in Redis, `cascache/redis.NewGenStore(...)` is the right tool. It keeps versions shared across nodes, but it cannot make a write atomic across Redis and a separate value store. Safety still comes from version checks on read and conditional writes, not from one cross-system transaction.

`cascache/redis.NewProvider(...)` is not the normal starting point. Use it only if you are intentionally composing `cascache.New(...)` by hand around a custom topology, for example:

- values in Redis, but generations in some non-Redis `GenStore`
- values in Redis, but single-key mutation is handled by custom code
- migration or framework code that needs the Redis pieces separately

If you manually combine `cascache/redis.NewProvider(...)` and `cascache/redis.NewGenStore(...)` with `cascache.New(...)`, the cache still works correctly, but you do not get the Redis-native single-key atomic path unless you also provide `cascache/redis.NewKeyMutator(...)` as both `KeyWriter` and `KeyInvalidator`. In practice, most callers should use `cascache/redis.New(...)` instead of wiring Redis pieces by hand.

### 1. Local versions

Use plain `cascache.New(...)` when:

- one process owns the cache
- each process can safely keep its own cache state
- you want an in-memory value store such as Ristretto or BigCache

This is the simplest setup. It is strict within the process, but it is not a distributed invalidation system by itself.

### 2. Shared versions in Redis

Use `cascache/redis.NewGenStore(...)` with `cascache.New(...)` when:

- several processes need to agree on whether cached values are still fresh
- values should remain in a non-Redis store
- you want distributed invalidation without moving the whole cache into Redis

This is a good fit for per-node caches backed by Ristretto or BigCache. Each node keeps its own values, but all nodes consult the same version store.

Choose this over `cascache/redis.New(...)` when:

- you want hot reads to stay in local process memory
- you want to reduce Redis memory use by keeping only versions there
- you want per-node caches with shared invalidation, not one shared Redis value store

What you get:

- shared freshness decisions across processes
- safe read validation against Redis-backed versions
- generic cache behavior with any provider

What you do not get:

- one atomic operation across Redis and a separate value store

### 3. Strict Redis cache

Use `cascache/redis.New(...)` when:

- Redis is your value store
- you want one shared cache across processes
- you want single-key `SetIfVersion` and `Invalidate` to happen inside Redis

This constructor exists for more than convenience. It wires the Redis-native single-key CAS implementation and keeps the single value key and single version key in the same Redis Cluster slot.

What you get:

- shared values
- shared versions
- atomic single-key compare-and-write in Redis
- atomic single-key invalidate in Redis

Batch entries are still validated on read. CasCache does not try to make arbitrary multi-key batch writes globally atomic.

This should be treated as the default Redis entry point. If you are asking "I want CasCache with Redis, which constructor should I use?", this is usually the answer.

### 4. More Advanced Redis composition

Use `cascache/redis.NewProvider(...)`, `cascache/redis.NewGenStore(...)`, and `cascache/redis.NewKeyMutator(...)` directly only when you are intentionally assembling a custom topology with `cascache.New(...)`.

That is an advanced path. Most application code should not start there.

## Quick start

### Generic cache

```go
package main

import (
	"time"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	ristrettoprovider "github.com/unkn0wn-root/cascache/provider/ristretto"
)

type User struct {
	ID   string
	Name string
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
		BatchTTL:    5 * time.Minute,
	})
}
```

### Safe repository pattern

```go
type UserRepo struct {
	Cache cascache.CAS[User]
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	if cached, ok, err := r.Cache.Get(ctx, id); err == nil && ok {
		return cached, nil
	}

	version, err := r.Cache.SnapshotVersion(ctx, id)
	if err != nil {
		return User{}, err
	}

	user, err := r.dbSelectUser(ctx, id)
	if err != nil {
		return User{}, err
	}

	_, _ = r.Cache.SetIfVersion(ctx, id, user, version, 0)
	return user, nil
}

func (r *UserRepo) UpdateName(ctx context.Context, id, name string) error {
	if err := r.dbUpdateName(ctx, id, name); err != nil {
		return err
	}
	return r.Cache.Invalidate(ctx, id)
}
```

## Redis examples

### Shared versions, non-Redis values

This keeps values in an in-memory provider but stores versions in Redis so all nodes agree on invalidation.

```go
package main

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	ristrettoprovider "github.com/unkn0wn-root/cascache/provider/ristretto"
	cascacheredis "github.com/unkn0wn-root/cascache/redis"
)

func newSharedVersionCache() (cascache.CAS[User], error) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	genStore, err := cascacheredis.NewGenStore(client)
	if err != nil {
		return nil, err
	}

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
		GenStore:   genStore,
		DefaultTTL: 5 * time.Minute,
		BatchTTL:    5 * time.Minute,
	})
}
```

In this setup, you still own the Redis client lifecycle because the genstore leaves shared clients open by default.

### Strict Redis cache

This stores both values and versions in Redis and enables the Redis-native single-key CAS path.

```go
package main

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	cascacheredis "github.com/unkn0wn-root/cascache/redis"
)

func newRedisUserCache() (cascache.CAS[User], error) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	return cascacheredis.New(cascacheredis.Options[User]{
		Namespace:   "user",
		Client:      client,
		Codec:       codec.JSON[User]{},
		DefaultTTL:  5 * time.Minute,
		BatchTTL:     5 * time.Minute,
		CloseClient: true,
	})
}
```

## Batch caching

Batch support is for validated set caching.

`GetMany` reads one stored batch blob, checks every requested member against the current versions, and serves only the members that are still valid. If the batch entry is stale, corrupt, or missing members, CasCache drops it and falls back to single-key reads.

`SetIfVersions` writes one batch blob when every observed version still matches. A successful batch write may also seed single-key entries, depending on `BatchWriteSeed`.

Batch is intentionally conservative:

- it is not globally atomic across keys
- it is validated on read
- successful batch hits are read-only by default

Example:

```go
func (r *UserRepo) GetMany(ctx context.Context, ids []string) (map[string]User, error) {
	values, missing, err := r.Cache.GetMany(ctx, ids)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		return values, nil
	}

	versions, err := r.Cache.SnapshotVersions(ctx, missing)
	if err != nil {
		return nil, err
	}

	loaded, err := r.dbSelectUsers(ctx, missing)
	if err != nil {
		return nil, err
	}

	items := make([]cascache.VersionedValue[User], 0, len(loaded))
	for _, user := range loaded {
		items = append(items, cascache.VersionedValue[User]{
			Key:     user.ID,
			Value:   user,
			Version: versions[user.ID],
		})
		values[user.ID] = user
	}

	_, _ = r.Cache.SetIfVersions(ctx, items, 0)
	return values, nil
}
```

## Read guards

`ReadGuard` and `BatchReadGuard` let you add one more check after decode and version validation.

Use them when version matching alone is not enough. Examples include:

- a database row version column
- a soft-delete flag
- a short-lived business rule that must be checked against the source of truth

If a guard rejects a value, CasCache deletes that cache entry and treats it as a miss.

## Notes

- `Get` does not return a single-key value whose stored version is no longer current.
- `SetIfVersion` stores only when the observed version still matches.
- `Invalidate` returns an error if the version bump fails.
- `GetMany` validates requested members against current versions before serving them.
- built-in strict stores do not recycle versions back to zero

## API

```go
type CAS[V any] interface {
	Enabled() bool
	Close(context.Context) error

	Get(ctx context.Context, key string) (V, bool, error)
	SnapshotVersion(ctx context.Context, key string) (Version, error)
	SetIfVersion(ctx context.Context, key string, value V, version Version, ttl time.Duration) (WriteResult, error)
	Invalidate(ctx context.Context, key string) error

	GetMany(ctx context.Context, keys []string) (map[string]V, []string, error)
	SnapshotVersions(ctx context.Context, keys []string) (map[string]Version, error)
	SetIfVersions(ctx context.Context, items []VersionedValue[V], ttl time.Duration) (BatchWriteResult, error)
}
```
