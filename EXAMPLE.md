# Example

This example shows a basic read and write flow.

It uses:

- `SnapshotVersion` before the database read
- `SetIfVersion` after the database read
- `Invalidate` after a database write

```go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/codec"
	ristrettoprovider "github.com/unkn0wn-root/cascache/provider/ristretto"
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

	user, err := r.DB.Get(id)
	if err != nil {
		return User{}, err
	}

	_, _ = r.Cache.SetIfVersion(ctx, id, user, version)
	return user, nil
}

func (r *UserRepo) UpdateName(ctx context.Context, id, name string) error {
	if err := r.DB.UpdateName(id, name); err != nil {
		return err
	}
	return r.Cache.Invalidate(ctx, id)
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

func main() {
	ctx := context.Background()

	cache, err := newUserCache()
	if err != nil {
		panic(err)
	}
	defer cache.Close(ctx)

	db := &InMemoryDB{
		m: map[string]User{
			"42": {ID: "42", Name: "Linus"},
		},
	}
	repo := &UserRepo{DB: db, Cache: cache}

	u1, _ := repo.GetByID(ctx, "42")
	fmt.Println("first read:", u1.Name)

	_ = repo.UpdateName(ctx, "42", "Tommy Lee Jones")

	u2, _ := repo.GetByID(ctx, "42")
	fmt.Println("after update:", u2.Name)
}
```

Expected output:

```text
first read: Linus
after update: Tommy Lee Jones
```

## Batch example

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

	loaded := make([]User, 0, len(missing))
	for _, id := range missing {
		user, err := r.DB.Get(id)
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, user)
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

	_, _ = r.Cache.SetIfVersions(ctx, items)
	return values, nil
}
```
