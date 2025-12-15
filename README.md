# CasCache

Provider-agnostic CAS like (**C**ompare-**A**nd-**S**et or generation-guarded conditional set) cache with pluggable codecs and a pluggable generation store.
Safe single-key reads (no stale values), optional bulk caching with read-side validation,
and an opt‑in distributed mode for multi-replica deployments.

---

## Contents
- [Why CasCache](#why-cascache)
- [Getting started](#getting-started)
- [Design](#design)
- [Wire format](#wire-format)
- [Providers](#providers)
- [Codecs](#codecs)
- [Distributed generations (multi-replica)](#distributed-generations-multi-replica)
- [API](#api)
- [Cache Type alias](#cache-type-alias)
- [Performance notes](#performance-notes)

---

## Why CasCache

#### TL;DR
Apps that rely on “delete-then-set” patterns can still surface stale data, especially when multiple replicas race one another. **cascache** offers a clear guarantee:

> **After you invalidate a key, the cache will never serve the previous value.**
> No additional delete cycles, manual timing coordination, or best-effort TTL tuning.

It does this with **generation-guarded writes** (CAS) and **read-side validation** using a tiny per-key counter.

---

### What goes wrong with “normal” caches

| Pattern             | What you do                            | What still goes wrong                                                                 |
|---------------------|----------------------------------------|---------------------------------------------------------------------------------------|
| **TTL only**        | Set `user:42` for 5m                   | Readers can see **up to 5m stale** after a write. Reducing TTL increases DB load.     |
| **Delete then set** | `DEL user:42` then `SET user:42`       | Races: a reader between `DEL` and new `SET` repopulates from **old DB snapshot**.     |
| **Write-through**   | Update DB, then cache                  | Concurrent readers can serve **old data** until invalidation is coordinated perfectly.|
| **Version in value**| Store `{version, payload}`             | Readers still need **current version**; coordinating that is the same hard problem.   |

---

### What CasCache guarantees

- **Post-invalidation freshness:**
  Each key carries a **generation**. Mutations call `Invalidate(key)` to bump the generation. Reads accept a cached value **only if** its stored generation matches the current one; otherwise the entry is **deleted** and treated as a miss.

- **Safe conditional writes (CAS):**
  Writers snapshot the generation **before** reading from the backing store. `SetWithGen(k, v, obs)` commits only if the generation is unchanged. If another writer updated the key, the write is **skipped**, preventing stale data from being reintroduced.

- **Graceful failure modes:**
  If the generation store is slow or unavailable, single-key reads **self-heal** by treating results as misses, and CAS writes **skip**. The cache never serves stale data; the system simply performs extra work.

- **Validated bulk caching:**
  Bulk entries are checked **member by member** during reads. If any entry is stale, the bulk payload is discarded and CasCache falls back to single-key fetches. Extras in the bulk are ignored; missing members render it invalid.

- **Pluggable components:**
  Works with **Ristretto/BigCache/Redis** for storage and **JSON/CBOR/Msgpack/Proto** for payloads. Wire decoding stays tight and zero-copy for payloads.

---

### When to use it

- Workloads that **must reflect updates immediately** (profile data, product catalogs, permissions, pricing, feature flags etc.)
- Environments with **multiple replicas** where coordinating invalidations is error-prone.
- Systems that need **predictable behaviour** under incidents: either serve fresh data or miss, never “maybe stale.”

---

### Comparison with common patterns

- **TTL-only caching** trades freshness for load. CasCache preserves freshness and uses TTLs strictly for eviction.
- **Delete-then-set** introduces race windows. Generations make freshness a **read-time concern** instead of a timing exercise.
- **Write-through caching** still needs coordination. CAS reduces coordination to an explicit sequence: **bump → snapshot → compare**.

---

### Minimal mental model

```text
DB write succeeds  →  Cache.Invalidate(k)       // bump gen; clear single
Read slow path     →  snap := SnapshotGen(ctx, k) → load DB → SetWithGen(k, v, snap)
Read fast path     →  Get(k) validates stored gen == current; else self-heals
Bulk read          →  every member’s gen must match; else drop bulk → singles
Multi-replica      →  use RedisGenStore so all nodes see the same gen
```

---

## Getting started
#### 1) Build the cache

```go
import (
	"context"
	"time"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	rp "github.com/unkn0wn-root/cascache/provider/ristretto"
)

type User struct{ ID, Name string }

func newUserCache() (cascache.CAS[User], error) {
	rist, err := rp.New(rp.Config{
		NumCounters: 1_000_000,
		MaxCost:     64 << 20, // 64 MiB
		BufferItems: 64,
		Metrics:     false,
	})
	if err != nil { return nil, err }

	return cascache.New[User](cascache.Options[User]{
		Namespace:  "user",
		Provider:   rist,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
		BulkTTL:    5 * time.Minute,
		// GenStore: nil -> Local (single-process) generations
	})
}
```

#### 2) Safe single read (never stale)
> **Rule:** Snapshot the generation **before** touching the DB. If you read first, a concurrent invalidation can bump the generation and your later `SetWithGen` may reinsert stale data.

```go
type UserRepo struct {
	Cache cascache.CAS[User]
	// db handle...
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	if u, ok, _ := r.Cache.Get(ctx, id); ok {
		return u, nil
	}

	// CAS snapshot BEFORE reading DB
	obs := r.Cache.SnapshotGen(ctx, id)

	u, err := r.dbSelectUser(ctx, id) // your DB load
	if err != nil { return User{}, err }

	// Conditionally cache only if generation didn't move
	_ = r.Cache.SetWithGen(ctx, id, u, obs, 0)
	return u, nil
}
```

#### 3) Mutations invalidate (one line)
> Rule: after a successful DB write, call Invalidate(key) to bump the generation.

```go
func (r *UserRepo) UpdateName(ctx context.Context, id, name string) error {
	if err := r.dbUpdateName(ctx, id, name); err != nil { return err }
	_ = r.Cache.Invalidate(ctx, id) // bump gen + clear single
	return nil
}
```

#### 4) Optional bulk (safe set caching)
> If any member is stale, the bulk is dropped and you fall back to singles. In multi-replica apps, use a shared GenStore (below) or disable bulk.

```go
func (r *UserRepo) GetMany(ctx context.Context, ids []string) (map[string]User, error) {
	values, missing, _ := r.Cache.GetBulk(ctx, ids)
	if len(missing) == 0 {
		return values, nil
	}

	// Snapshot *before* DB read
	obs := r.Cache.SnapshotGens(ctx, missing)

	// Load missing from DB in one shot
	loaded, err := r.dbSelectUsers(ctx, missing)
	if err != nil { return nil, err }

	// Index for SetBulkWithGens
	items := make(map[string]User, len(loaded))
	for _, u := range loaded { items[u.ID] = u }

	// Conditionally write bulk (or it will seed singles if any gen moved)
	_ = r.Cache.SetBulkWithGens(ctx, items, obs, 0)

	// Merge and return
	for k, v := range items { values[k] = v }
	return values, nil
}
```

---

## Distributed generations (multi-replica)

Local generations are correct within a single process. In multi-replica deployments, share generations to eliminate cross-node windows and keep bulks safe across nodes.
> LocalGenStore is **single-process only**. In multi-replica setups, both singles and bulks can be stale on nodes that haven’t observed the bump.
Use a shared GenStore (e.g., Redis) for cross-replica correctness or run a single instance.


```go
import "github.com/redis/go-redis/v9"

func newUserCacheDistributed() (cascache.CAS[User], error) {
	rist, _ := rp.New(rp.Config{NumCounters:1_000_000, MaxCost:64<<20, BufferItems:64})
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	gs  := cascache.NewRedisGenStoreWithTTL(rdb, "user", 90*24*time.Hour)

	return cascache.New[User](cascache.Options[User]{
		Namespace: "user",
		Provider:  rist,                      // or Redis/BigCache for values
		Codec:     codec.JSON[User]{},
		GenStore:  gs,                        // shared generations
		BulkTTL:   5 * time.Minute,
	})
}
```

You can reuse the same client across caches:

```go
rdb := redis.NewClusterClient(/* ... */) // or UniversalClient

userCache, _ := cascache.New[user](cascache.Options[user]{
    Namespace: "app:prod:user",
    Provider:  myRedisProvider{Client: rdb},               // same client
    Codec:     c.JSON[user]{},
    GenStore:  gen.NewRedisGenStoreWithTTL(rdb, "app:prod:user", 24*time.Hour),
})

pageCache, _ := cascache.New[page](cascache.Options[page]{
    Namespace: "app:prod:page",
    Provider:  myRedisProvider{Client: rdb},               // same client
    Codec:     c.JSON[page]{},
    GenStore:  gen.NewRedisGenStoreWithTTL(rdb, "app:prod:page", 24*time.Hour),
})

permCache, _ := cascache.New[permission](cascache.Options[permission]{
    Namespace: "app:prod:perm",
    Provider:  myRedisProvider{Client: rdb},               // same client
    Codec:     c.JSON[permission]{},
    GenStore:  gen.NewRedisGenStoreWithTTL(rdb, "app:prod:perm", 24*time.Hour),
})
```
---

> **Alternative type name:** You can use `cascache.Cache[V]` instead of `cascache.CAS[V]`. See [Cache Type alias](#cache-type-alias).

---

## Design

### Read path (single)

```
      Get(k)
        │
  provider.Get("single:<ns>:"+k) ───► [wire.DecodeSingle]
        │
   gen == currentGen("single:<ns>:"+k) ?
        │ yes                                 no
        ▼                                    ┌───────────────┐
  codec.Decode(payload)                      │ Del(entry)    │
        │                                    │ return miss   │
      return v                               └───────────────┘
```

### Write path (single, CAS)

```
obs := SnapshotGen(ctx, k)   // BEFORE DB read
v   := DB.Read(k)
SetWithGen(k, v, obs)        // write iff currentGen(k) == obs
```

### Bulk read validation

```
GetBulk(keys)   -> provider.Get("bulk:<ns>:hash(sorted(keys))")
Decode -> [(key,gen,payload)]*n
for each item: gen == currentGen("single:<ns>:"+key) ?
if all valid -> decode all, return
else         -> drop bulk, fall back to singles
```

### Components
- **Provider** - byte store with TTLs: Ristretto/BigCache/Redis.
- **Codec[V]** - serialize/deserialize your `V` to/from `[]byte`.
- **GenStore** - per-key generation counter (Local by default; Redis available).

### Keys
- Single entry: `single:<ns>:<key>`
- Bulk entry:   `bulk:<ns>:<first16(sha256(sorted(keys))>>`

### CAS model
- Per-key generation is **monotonic**.
- Reads validate only; no write amplification.
- Mutations call `Invalidate(key)` → bump generation and delete the single entry.

---

## Wire format

Small binary envelope before the codec payload. Big-endian integers. Magic `"CASC"`.

**Single**

```
+---------+---------+---------+---------------+---------------+-------------------+
| magic   | version | kind    | gen (u64)     | vlen (u32)    | payload (vlen)    |
| "CASC"  |   0x01  |  0x01   | 8 bytes       | 4 bytes       | vlen bytes        |
+---------+---------+---------+---------------+---------------+-------------------+
```

**Bulk**

```
+---------+---------+---------+------------------------+
| magic   | version | kind    | n (u32)                |
+---------+---------+---------+------------------------+
repeated n times:
+----------------+-----------------+----------------+-------------------+------------------+
| keyLen (u16)   | key (keyLen)    | gen (u64)      | vlen (u32)        | payload (vlen)   |
+----------------+-----------------+----------------+-------------------+------------------+
```

Decoders are zero-copy for payloads and keys (one `string` alloc per bulk item).

---

## Providers

```go
type Provider interface {
    Get(ctx context.Context, key string) ([]byte, bool, error)
    Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error)
    Del(ctx context.Context, key string) error
    Close(ctx context.Context) error
}
```

- **Ristretto**: in-process; per-entry TTL; cost-based eviction.
- **BigCache**: in-process; global life window; per-entry TTL ignored.
- **Redis**: distributed (optional); per-entry TTL. Pass an existing
  `goredis.UniversalClient` and reuse it across caches; set
  `CloseClient: true` in the provider config only when this cache owns the
  client and should close it on teardown.

Use any provider for values. Generations can be local or distributed independently.

---

## Codecs

```go
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}
type JSON[V any] struct{}
```

You can drop in Msgpack/CBOR/Proto or decorators (compression/encryption). CAS is codec-agnostic.

---

## API

```go
type CAS[V any] interface {
    Enabled() bool
    Close(context.Context) error

    // Single
    Get(ctx context.Context, key string) (V, bool, error)
    SetWithGen(ctx context.Context, key string, value V, observedGen uint64, ttl time.Duration) error
    Invalidate(ctx context.Context, key string) error

    // Bulk
    GetBulk(ctx context.Context, keys []string) (map[string]V, []string, error)
    SetBulkWithGens(ctx context.Context, items map[string]V, observedGens map[string]uint64, ttl time.Duration) error

    // Generations
    SnapshotGen(ctx context.Context, key string) uint64
    SnapshotGens(ctx context.Context, keys []string) map[string]uint64
}
```
---

## Cache Type Alias

For readability, we provide a type alias:

```go
type Cache[V any] = CAS[V]
```

You may use either name. They are **identical types**. Example:

```go
var a cascache.CAS[User]
var b cascache.Cache[User]

a = b // ok
b = a // ok
```

In examples we often use `CAS` to emphasize the CAS semantics, but `Cache` is equally valid and may read more naturally in your codebase.

---

## Performance notes

- **Time:** O(1) singles; O(n) bulk for n members.
- **Allocations:** zero-copy wire decode; one `string` alloc per bulk item.
```go
ComputeSetCost: func(key string, raw []byte, isBulk bool, n int) int64 {
    if isBulk { return int64(n) }
    return 1
}
```
- **Cleanup (local gens):** periodic prune by last bump time (default retention 30d).

---
