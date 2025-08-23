# cascache

Provider-agnostic CAS (Compare-And-Set) cache with pluggable codecs and a pluggable generation store.
Safe single-key reads (no stale values), optional bulk caching with read-side validation,
and an opt‑in distributed mode for multi-replica deployments.

---

## Contents
- [Overview](#overview)
- [Quick start](#quick-start)
- [Design](#design)
- [Wire format](#wire-format)
- [Providers](#providers)
- [Codecs](#codecs)
- [Distributed generations](#distributed-generations)
- [API](#api)
- [Cache Type alias](#cache-type-alias)
- [Performance notes](#performance-notes)

---

## Overview

- **CAS safety:** Writers snapshot a per-key **generation** before the DB read. Cache writes commit only if the generation is unchanged.
- **Singles:** Never return stale values; corrupt/type-mismatched entries self-heal.
- **Bulk:** Cache a set-shaped result. On read, validate every member’s generation. Reject the bulk if any member is stale.
- **Composable:** Plug any value provider (Ristretto/BigCache/Redis) and any codec (JSON/Msgpack/CBOR/Proto).
- **Distributed** Keep local generations (default) or plug a shared `GenStore` (e.g., Redis) for cross-replica correctness and warm restarts.

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
obs := SnapshotGen(k)        // BEFORE DB read
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

---

## Quick start

```go
import (
    "context"
    "time"

    "github.com/unkn0wn-root/cascache"
    rp "github.com/unkn0wn-root/cascache/provider/ristretto"
)

type User struct{ ID, Name string }

func buildCache() cascache.CAS[User] {
    // Value provider (in-process)
    rist, _ := rp.New(rp.Config{
        NumCounters: 1_000_000,
        MaxCost:     64 << 20, // 64 MiB
        BufferItems: 64,
        Metrics:     false,
    })

    cc, _ := cascache.New[User](cascache.Options[User]{
        Namespace:  "user",
        Provider:   rist,
        Codec:      cascache.JSONCodec[User]{},
        DefaultTTL: 5 * time.Minute,
        BulkTTL:    5 * time.Minute,
        // GenStore: nil -> Local (default). See "Distributed generations".
    })
    return cc
}

func readUser(ctx context.Context, c cascache.CAS[User], id string) (User, bool) {
    if u, ok, _ := c.Get(ctx, id); ok { return u, true }
    obs := c.SnapshotGen(id)  // CAS snapshot BEFORE DB read
    u := loadFromDB(id)
    _ = c.SetWithGen(ctx, id, u, obs, 0)
    return u, true
}
```

> **Alternative type name:** You can use `cascache.Cache[V]` instead of `cascache.CAS[V]`. See [Cache Type alias](#cache-type-alias).

---

## Design

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
- **Redis**: distributed (optional); per-entry TTL.

Use any provider for values. Generations can be local or distributed independently.

---

## Codecs

```go
type Codec[V any] interface {
    Encode(V) ([]byte, error)
    Decode([]byte) (V, error)
}
type JSONCodec[V any] struct{}
```

You can drop in Msgpack/CBOR/Proto or decorators (compression/encryption). CAS is codec-agnostic.

---

## Distributed generations

Local generations are correct for singles but bulk validation can be stale across replicas.
Use a shared `GenStore` to eliminate this window and survive restarts.

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
gs  := cascache.NewRedisGenStore(rdb, "user") // namespace should match Options.Namespace
// or, with TTL:
// gs  := gen.NewRedisGenStoreWithTTL(rdb, "user", 90*24*time.Hour) // with TTL to prevent growth

cache, _ := cascache.New[User](cascache.Options[User]{
    Namespace: "user",
    Provider:  ristrettoProvider, // or Redis, BigCache
    Codec:     cascache.JSONCodec[User]{},
    GenStore:  gs,                // shared generations
})
```

**Behavior**
- Singles: never stale (same as local).
- Bulks: validated against shared generations across replicas.
- Restarts: generations persist; valid entries remain valid.

> If you do not use a distributed GenStore in a multi-replica deployment,
set Options.DisableBulk = true (or use a very short BulkTTL). Singles remain safe:
they never return stale data.

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
    SnapshotGen(key string) uint64
    SnapshotGens(keys []string) map[string]uint64
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
- **Ristretto cost hint:** evict bulks first under pressure.
```go
ComputeSetCost: func(key string, raw []byte, isBulk bool, n int) int64 {
    if isBulk { return int64(n) }
    return 1
}
```
- **TTLs:** `DefaultTTL`, `BulkTTL`. For BigCache, the global life window applies.
- **Cleanup (local gens):** periodic prune by last bump time (default retention 30d).

---

