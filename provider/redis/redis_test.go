package redis_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4/provider"
	"github.com/unkn0wn-root/cascache/v4/provider/providertest"
	redisprovider "github.com/unkn0wn-root/cascache/v4/provider/redis"
)

func TestStoreConformance(t *testing.T) {
	addr := os.Getenv("CASCACHE_TEST_REDIS")
	if addr == "" {
		t.Skip("set CASCACHE_TEST_REDIS to run the Redis store tests")
	}

	providertest.TestStore(t, providertest.Config{
		New: func(t testing.TB) provider.Store {
			client := goredis.NewClient(&goredis.Options{Addr: addr})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.Ping(ctx).Err(); err != nil {
				_ = client.Close()
				t.Fatalf("redis at %s: %v", addr, err)
			}

			s, err := redisprovider.New(redisprovider.Config{Client: client, CloseClient: true})
			if err != nil {
				t.Fatalf("redisprovider.New: %v", err)
			}
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			return s
		},
		SupportsTTL: true,
	})
}

func TestNewRequiresAClient(t *testing.T) {
	if _, err := redisprovider.New(redisprovider.Config{}); !errors.Is(err, redisprovider.ErrNilClient) {
		t.Fatalf("New without a client = %v, want ErrNilClient", err)
	}
}
