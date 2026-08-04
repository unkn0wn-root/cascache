package redis_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/backend/backendtest"
	"github.com/unkn0wn-root/cascache/v4/backend/redis"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

// Use CASCACHE_TEST_REDIS or skip tests that require a server.
func dial(t testing.TB) *goredis.Client {
	t.Helper()

	addr := os.Getenv("CASCACHE_TEST_REDIS")
	if addr == "" {
		t.Skip("set CASCACHE_TEST_REDIS to run the Redis backend tests")
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("redis at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newBackend(t testing.TB, opts redis.Options) *redis.Backend {
	t.Helper()
	b, err := redis.New(dial(t), opts)
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	return b
}

func TestBackendConformance(t *testing.T) {
	backendtest.TestBackend(t, func(t testing.TB) backend.Backend {
		return newBackend(t, redis.Options{InvalidationTTL: backend.NoExpiration})
	})
}

func TestHybridConformance(t *testing.T) {
	backendtest.TestBackend(t, func(t testing.TB) backend.Backend {
		b, err := redis.NewShared(
			memstore.New(memstore.Options{}),
			dial(t),
			redis.Options{InvalidationTTL: backend.NoExpiration},
		)
		if err != nil {
			t.Fatalf("redis.NewShared: %v", err)
		}
		return b
	})
}

// Losing fence state must not make a retired value current again.
func TestExpiredFenceCannotResurrectAValue(t *testing.T) {
	client := dial(t)
	b := newBackend(t, redis.Options{InvalidationTTL: backend.NoExpiration})
	key := backendtest.Key(t, "resurrection")
	ctx := context.Background()

	first, err := b.Ensure(ctx, key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CompareAndStore(ctx, backend.StoreRequest{
		Key: key, Expected: first, Value: []byte("retired"), Cost: 1, TTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Invalidate(ctx, key, backend.NewFence()); err != nil {
		t.Fatal(err)
	}

	if err := client.Del(ctx, backend.FenceKey(key)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, backend.ValueKey(key), []byte("retired"), time.Hour).Err(); err != nil {
		t.Fatal(err)
	}

	read, err := b.Read(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !read.Found {
		t.Fatal("the value is gone, so this test proves nothing; it must be present but unjudgeable")
	}
	if read.FenceFound {
		t.Fatal("a deleted fence came back")
	}

	res, err := b.CompareAndStore(ctx, backend.StoreRequest{
		Key: key, Expected: first, Value: []byte("resurrected"), Cost: 1, TTL: time.Hour,
	})
	if err != nil || res.Status != backend.StoreConflict {
		t.Fatalf("a write under a retired fence = %+v, %v; want conflict", res, err)
	}
}

func TestInvalidationTTL(t *testing.T) {
	client := dial(t)
	ctx := context.Background()

	t.Run("expires", func(t *testing.T) {
		b := newBackend(t, redis.Options{InvalidationTTL: 150 * time.Millisecond})
		key := backendtest.Key(t, "expiring")

		fence, err := b.Ensure(ctx, key, backend.NewFence())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.CompareAndStore(ctx, backend.StoreRequest{
			Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}

		time.Sleep(400 * time.Millisecond)

		read, err := b.Read(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		if read.Found || read.FenceFound {
			t.Fatalf("Read after the fence expired = %+v, want a miss", read)
		}
	})

	t.Run("clamps the value TTL", func(t *testing.T) {
		const fenceTTL = time.Hour
		b := newBackend(t, redis.Options{InvalidationTTL: fenceTTL})
		key := backendtest.Key(t, "clamped")

		fence, err := b.Ensure(ctx, key, backend.NewFence())
		if err != nil {
			t.Fatal(err)
		}
		res, err := b.CompareAndStore(ctx, backend.StoreRequest{
			Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: 48 * time.Hour,
		})
		if err != nil || res.Status != backend.StoreStored {
			t.Fatalf("CompareAndStore = %+v, %v", res, err)
		}
		if res.EffectiveTTL != fenceTTL {
			t.Fatalf("EffectiveTTL = %v, want %v", res.EffectiveTTL, fenceTTL)
		}

		// Compare absolute expiry because the PTTL calls occur at different times.
		valueAt, err := client.PExpireTime(ctx, backend.ValueKey(key)).Result()
		if err != nil {
			t.Skipf("PEXPIRETIME unavailable: %v", err)
		}
		fenceAt, err := client.PExpireTime(ctx, backend.FenceKey(key)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if valueAt > fenceAt {
			t.Fatalf("value outlives its fence: value expires at %d, fence at %d", valueAt, fenceAt)
		}
	})

	t.Run("clears an expiry left by an earlier configuration", func(t *testing.T) {
		key := backendtest.Key(t, "reconfigured")

		expiring := newBackend(t, redis.Options{InvalidationTTL: time.Hour})
		if _, err := expiring.Ensure(ctx, key, backend.NewFence()); err != nil {
			t.Fatal(err)
		}
		if ttl, err := client.PTTL(ctx, backend.FenceKey(key)).Result(); err != nil || ttl <= 0 {
			t.Fatalf("PTTL = %v, %v; want a positive TTL", ttl, err)
		}

		permanent := newBackend(t, redis.Options{InvalidationTTL: backend.NoExpiration})
		if _, err := permanent.Ensure(ctx, key, backend.NewFence()); err != nil {
			t.Fatal(err)
		}

		ttl, err := client.PTTL(ctx, backend.FenceKey(key)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl != -1 {
			t.Fatalf("PTTL after reconfiguring = %v, want no expiry", ttl)
		}
	})
}

func TestScriptsSurviveAColdServer(t *testing.T) {
	client := dial(t)
	ctx := context.Background()

	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Skipf("SCRIPT FLUSH unavailable: %v", err)
	}

	b := newBackend(t, redis.Options{InvalidationTTL: backend.NoExpiration})
	key := backendtest.Key(t, "cold")

	fence, err := b.Ensure(ctx, key, backend.NewFence())
	if err != nil {
		t.Fatalf("Ensure against a cold server: %v", err)
	}
	if _, err := b.CompareAndStore(ctx, backend.StoreRequest{
		Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: time.Minute,
	}); err != nil {
		t.Fatalf("CompareAndStore against a cold server: %v", err)
	}
	if _, err := b.Discard(ctx, key, []byte("v")); err != nil {
		t.Fatalf("Discard against a cold server: %v", err)
	}
	if _, err := b.Invalidate(ctx, key, backend.NewFence()); err != nil {
		t.Fatalf("Invalidate against a cold server: %v", err)
	}
}

func TestNewRejectsNilClient(t *testing.T) {
	if _, err := redis.New(nil, redis.Options{}); !errors.Is(err, redis.ErrNilClient) {
		t.Fatalf("New(nil) = %v, want ErrNilClient", err)
	}

	var typedNil *goredis.Client
	if _, err := redis.New(typedNil, redis.Options{}); !errors.Is(err, redis.ErrNilClient) {
		t.Fatalf("New(typed nil) = %v, want ErrNilClient", err)
	}
	if _, err := redis.NewShared(
		memstore.New(memstore.Options{}), nil, redis.Options{},
	); !errors.Is(err, redis.ErrNilClient) {
		t.Fatalf("NewShared(nil client) = %v, want ErrNilClient", err)
	}
}

func TestNewSharedRejectsNilStore(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	if _, err := redis.NewShared(nil, client, redis.Options{}); !errors.Is(err, backend.ErrNilStore) {
		t.Fatalf("NewShared(nil store) = %v, want ErrNilStore", err)
	}
	var typedNil *memstore.Store
	if _, err := redis.NewShared(typedNil, client, redis.Options{}); !errors.Is(err, backend.ErrNilStore) {
		t.Fatalf("NewShared(typed nil store) = %v, want ErrNilStore", err)
	}
}

// Replica lag can return a matching old value and fence, so reject replica reads.
func TestNewRejectsReplicaReads(t *testing.T) {
	client := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:    []string{"127.0.0.1:1"},
		ReadOnly: true,
	})
	t.Cleanup(func() { _ = client.Close() })

	if _, err := redis.New(client, redis.Options{}); !errors.Is(err, redis.ErrReplicaReads) {
		t.Fatalf("New(replica reads) = %v, want ErrReplicaReads", err)
	}
	if _, err := redis.New(client, redis.Options{AllowReplicaReads: true}); err != nil {
		t.Fatalf("New with AllowReplicaReads = %v, want it to be permitted", err)
	}
}

func TestSharedOptionsInvalidationTTL(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	cases := []struct {
		name string
		ttl  time.Duration
		bad  bool
	}{
		{name: "zero uses the default", ttl: 0},
		{name: "explicit", ttl: time.Hour},
		{name: "no expiration", ttl: backend.NoExpiration},
		{name: "other negatives are rejected", ttl: -5 * time.Second, bad: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := redis.NewShared(
				memstore.New(memstore.Options{}),
				client,
				redis.Options{InvalidationTTL: tc.ttl},
			)
			if tc.bad {
				if err == nil {
					t.Fatal("NewShared accepted an invalid invalidation TTL")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
