package cascache_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

type User struct {
	ID   string
	Name string
}

func Example() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	// Output: Ada
}

func run() error {
	ctx := context.Background()

	store, err := ristretto.New(ristretto.Config{
		NumCounters: 200_000, // about 10 times the entries it holds
		MaxCost:     1 << 20, // about 18,000 entries: Ristretto adds 56 to each entry's cost of 1
		BufferItems: 64,
	})
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	b, err := backend.NewLocal(store, backend.LocalOptions{})
	if err != nil {
		return err
	}
	defer b.Close()

	cache, err := cascache.New(cascache.Options[User]{
		Namespace:  "users",
		Backend:    b,
		Codec:      codec.JSON[User]{},
		DefaultTTL: 5 * time.Minute,
	})
	if err != nil {
		return err
	}

	user, err := cache.Load(ctx, "42", func(context.Context) (User, error) {
		return User{ID: "42", Name: "Ada"}, nil // read your database here
	})
	if err != nil {
		return err
	}

	fmt.Println(user.Name)
	return nil
}
