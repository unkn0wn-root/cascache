## Quickstart

This is a **complete, end-to-end** example that wires the cache into a tiny repo layer and uses it.

> Save as `main.go`, then run with `go run .`

```go
// main.go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	rp "github.com/unkn0wn-root/cascache/provider/ristretto"
)

type User struct{ ID, Name string }

// demo in memory db
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

// repo that uses cascache
type UserRepo struct {
	DB    *InMemoryDB
	Cache cascache.CAS[User]
}

func (r *UserRepo) GetByID(ctx context.Context, id string) (User, error) {
	// 1: try cache
	if u, ok, _ := r.Cache.Get(ctx, id); ok {
		return u, nil
	}
	// 1: snapshot generation BEFORE DB read
	obs := r.Cache.SnapshotGen(id)
	// 2: read DB
	u, err := r.DB.Get(id)
	if err != nil { return User{}, err }
	// 3: conditionally cache only if generation didn't move
	_ = r.Cache.SetWithGen(ctx, id, u, obs, 0)
	return u, nil
}

func (r *UserRepo) UpdateName(ctx context.Context, id, name string) error {
	if err := r.DB.UpdateName(id, name); err != nil {
		return err
	}
	// bump gen + clear single
	_ = r.Cache.Invalidate(ctx, id)
	return nil
}

// cache construction
func newUserCache() (cascache.CAS[User], error) {
	rist, err := rp.New(rp.Config{
		NumCounters: 1_000_000,
		MaxCost:     64 << 20,
		BufferItems: 64,
	})
	if err != nil { return nil, err }

	return cascache.New[User](cascache.Options[User]{
		Namespace:  "user",
		Provider:   rist,
		Codec:      codec.JSONCodec[User]{},
		DefaultTTL: 5 * time.Minute,
		BulkTTL:    5 * time.Minute,
	})
}

func main() {
	ctx := context.Background()
	cache, err := newUserCache()
	if err != nil { panic(err) }

	db := &InMemoryDB{m: map[string]User{"42": {ID: "42", Name: "Linus"}}}
	repo := &UserRepo{DB: db, Cache: cache}

	u1, _ := repo.GetByID(ctx, "42")
	fmt.Println("First read:", u1.Name) // Linus (miss -> DB -> cache)

	_ = repo.UpdateName(ctx, "42", "Tommy Lee Jones") // DB write + invalidate

	u2, _ := repo.GetByID(ctx, "42")
	fmt.Println("After update:", u2.Name) // Tommy Lee Jones (old entry invalidated/self-healed)
}
```

**Run it:**
```bash
go run .
```

Expected output:
```
First read: Linus
After update: Tommy Lee Jones
```

---

## Optional: bulk usage

```go
// Inside main(), after creating repo:
vals, missing, _ := repo.Cache.GetBulk(ctx, []string{"42", "99"})
if len(missing) > 0 {
	obs := repo.Cache.SnapshotGens(missing)
	items := map[string]User{}
	for _, id := range missing {
		u, _ := repo.DB.Get(id)
		items[id] = u
	}
	// conditionally write bulk (or it will seed singles if any gen moved)
	_ = repo.Cache.SetBulkWithGens(ctx, items, obs, 0)
	// merge for the caller if you need: for k, v := range items { vals[k] = v }
}
_ = vals
```

> If any member is stale at read time, the bulk is rejected and you fall back to safe singles.

