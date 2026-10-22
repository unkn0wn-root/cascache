package redis_test

import (
	"context"
	"fmt"
	"log"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

// Each process keeps values in its own store and shares invalidations
// through Redis.
func ExampleNewShared() {
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

	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()

	b, err := redisbackend.NewShared(store, client, redisbackend.Options{})
	if err != nil {
		log.Fatal(err)
	}

	cache, err := cascache.New(cascache.Options[string]{
		Namespace: "names",
		Backend:   b,
		Codec:     codec.String{},
	})
	if err != nil {
		log.Fatal(err)
	}

	name, err := cache.Load(ctx, "42", func(context.Context) (string, error) {
		return "Ada", nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
}

// Values and invalidation state both live in Redis.
func ExampleNew() {
	ctx := context.Background()

	client := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer client.Close()

	b, err := redisbackend.New(client, redisbackend.Options{})
	if err != nil {
		log.Fatal(err)
	}

	cache, err := cascache.New(cascache.Options[string]{
		Namespace: "names",
		Backend:   b,
		Codec:     codec.String{},
	})
	if err != nil {
		log.Fatal(err)
	}

	name, err := cache.Load(ctx, "42", func(context.Context) (string, error) {
		return "Ada", nil
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)
}
