## Quickstart

```go
package main

import (
	"context"
	"fmt"
	"sync"
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

type InMemoryDB struct {
	mu sync.RWMutex
	m  map[string]User
}

func (db *InMemoryDB) Get(id string) (User, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.m[id], nil
}

func (db *InMemoryDB) UpdateName(id, name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	u := db.m[id]
	u.Name = name
	db.m[id] = u
	return nil
}

type UserRepo struct {
	DB    *InMemoryDB
	Cache *cascache.Cache[User]
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	return r.Cache.Load(ctx, id, func(ctx context.Context) (User, error) {
		return r.DB.Get(id)
	})
}

// Update the database first, then invalidate the cache. This makes existing
// cached copies unusable and prevents a concurrent load from writing an older
// value back to the cache.
func (r *UserRepo) UpdateName(ctx context.Context, id, name string) error {
	if err := r.DB.UpdateName(id, name); err != nil {
		return err
	}
	return r.Cache.Invalidate(ctx, id)
}

// newUserCache creates a cache for one process. Values and invalidation state
// are both kept locally.
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

func main() {
	ctx := context.Background()

	cache, b, err := newUserCache()
	if err != nil {
		panic(err)
	}
	// The cache does not close the backend, so close it here.
	defer b.Close()

	db := &InMemoryDB{
		m: map[string]User{
			"42": {ID: "42", Name: "Linus"},
		},
	}

	repo := &UserRepo{DB: db, Cache: cache}

	u1, _ := repo.GetByID(ctx, "42")
	fmt.Println("First read:", u1.Name)

	_ = repo.UpdateName(ctx, "42", "Tommy Lee Jones")

	u2, _ := repo.GetByID(ctx, "42")
	fmt.Println("After update:", u2.Name)
}
```

Expected output:

```text
First read: Linus
After update: Tommy Lee Jones
```

## Sharing invalidation state across replicas

Keep the invalidation state in Redis while each replica stores values in local
memory. Invalidating a key from one replica makes cached copies in every
replica invalid:

```go
// import redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"

b, err := redisbackend.NewShared(store, client, redisbackend.Options{})
```

Nothing else changes. The replica that invalidates deletes only its own copy.
Copies in other replicas no longer match the shared state and are discarded
when read.

## Filling by hand

`Load` covers the usual case. When you need your own write flow, take a snapshot
before reading from the database:

```go
snapshot, err := cache.Snapshot(ctx, id)
if err != nil {
	return err
}

user, err := db.Get(id)
if err != nil {
	return err
}

res, err := cache.Set(ctx, id, user, snapshot)
if err != nil {
	return err
}
if res.Outcome == cascache.SetOutcomeConflict {
	// The key was invalidated while we were reading. The value we loaded was
	// already stale, so it was not cached. Nothing to do.
}
```
