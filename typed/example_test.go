package typed_test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
	"github.com/unkn0wn-root/cascache/v4/typed"
)

type User struct {
	ID   int64
	Name string
}

func ExampleNew() {
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

	users, err := typed.New(typed.Options[int64, User]{
		Config: typed.Config{
			Namespace: "users",
			MaxTTL:    10 * time.Minute,
			MinTTL:    8 * time.Minute,
			Jitter:    1, // pick each TTL at random between MinTTL and MaxTTL
		},
		KeyFunc: func(id int64) string { return strconv.FormatInt(id, 10) },
		Codec:   codec.JSON[User]{},
		Backend: b,
	})
	if err != nil {
		log.Fatal(err)
	}

	user, err := users.Load(ctx, 42, func(context.Context) (User, error) {
		return User{ID: 42, Name: "Ada"}, nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(user.Name)

	// Output: Ada
}
