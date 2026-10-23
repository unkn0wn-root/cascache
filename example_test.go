package cascache_test

import (
	"context"
	"fmt"
	"log"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

func ExampleCache_Invalidate() {
	ctx := context.Background()

	store, err := ristretto.New(ristretto.Config{
		NumCounters: 200_000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(ctx)

	b, err := backend.NewLocal(store, backend.LocalOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	cache, err := cascache.New(cascache.Options[string]{
		Namespace: "names",
		Backend:   b,
		Codec:     codec.String{},
	})
	if err != nil {
		log.Fatal(err)
	}

	db := map[string]string{"42": "Ada"} // stands in for your database
	load := func(context.Context) (string, error) { return db["42"], nil }

	name, err := cache.Load(ctx, "42", load)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)

	// Ristretto applies writes in the background
	store.Wait()

	db["42"] = "Grace"
	name, err = cache.Load(ctx, "42", load)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name) // still cached

	if err = cache.Invalidate(ctx, "42"); err != nil {
		log.Fatal(err)
	}

	name, err = cache.Load(ctx, "42", load)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)

	// Output:
	// Ada
	// Ada
	// Grace
}

// A key invalidated between Snapshot and Set is not written. The value read
// under the old snapshot may be out of date, so Set reports a conflict.
func ExampleCache_Set() {
	ctx := context.Background()

	store, err := ristretto.New(ristretto.Config{
		NumCounters: 200_000,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(ctx)

	b, err := backend.NewLocal(store, backend.LocalOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	cache, err := cascache.New(cascache.Options[string]{
		Namespace: "names",
		Backend:   b,
		Codec:     codec.String{},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Take the snapshot before reading the source.
	snapshot, err := cache.Snapshot(ctx, "42")
	if err != nil {
		log.Fatal(err)
	}
	name := "Ada" // read from the source

	// Another request changes the source and invalidates the key.
	if err = cache.Invalidate(ctx, "42"); err != nil {
		log.Fatal(err)
	}

	res, err := cache.Set(ctx, "42", name, snapshot)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Outcome)

	// Output: conflict
}
