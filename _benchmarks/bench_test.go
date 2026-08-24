package benchmarks

import (
	"bytes"
	"context"
	"os"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

const redisAddressEnv = "CASCACHE_BENCH_REDIS"

var (
	benchmarkValue = bytes.Repeat([]byte("0123456789abcdef"), 16)
	benchmarkSink  []byte
)

func BenchmarkLocalRead(b *testing.B) {
	ctx := context.Background()
	cache := newLocalCache(b, "benchmark:local:read")
	seedCache(b, ctx, cache)
	benchmarkCacheRead(b, ctx, cache)
}

func BenchmarkRedisRead(b *testing.B) {
	ctx := context.Background()
	cache := newRedisCache(b, ctx, "benchmark:redis:read")
	seedCache(b, ctx, cache)
	benchmarkCacheRead(b, ctx, cache)
}

func BenchmarkPlainRedisRead(b *testing.B) {
	ctx := context.Background()
	client := newRedisClient(b, ctx)
	const key = "cascache:v4:benchmark:plain"

	if err := client.Set(ctx, key, benchmarkValue, 0).Err(); err != nil {
		b.Fatal(err)
	}
	if _, err := client.Get(ctx, key).Bytes(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()

	var got []byte
	var err error
	for i := 0; i < b.N; i++ {
		got, err = client.Get(ctx, key).Bytes()
		if err != nil {
			b.Fatal(err)
		}
	}
	benchmarkSink = got
}

func BenchmarkLocalSet(b *testing.B) {
	ctx := context.Background()
	cache := newLocalCache(b, "benchmark:local:set")
	snapshot := seedCache(b, ctx, cache)
	benchmarkCacheSet(b, ctx, cache, snapshot)
}

func BenchmarkRedisSet(b *testing.B) {
	ctx := context.Background()
	cache := newRedisCache(b, ctx, "benchmark:redis:set")
	snapshot := seedCache(b, ctx, cache)
	benchmarkCacheSet(b, ctx, cache, snapshot)
}

func BenchmarkPlainRedisSet(b *testing.B) {
	ctx := context.Background()
	client := newRedisClient(b, ctx)
	const key = "cascache:v4:benchmark:plain:set"

	if err := client.Set(ctx, key, benchmarkValue, 0).Err(); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Set(ctx, key, benchmarkValue, 0).Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLocalInvalidate(b *testing.B) {
	ctx := context.Background()
	cache := newLocalCache(b, "benchmark:local:invalidate")
	seedCache(b, ctx, cache)
	benchmarkCacheInvalidate(b, ctx, cache)
}

func BenchmarkRedisInvalidate(b *testing.B) {
	ctx := context.Background()
	cache := newRedisCache(b, ctx, "benchmark:redis:invalidate")
	seedCache(b, ctx, cache)
	benchmarkCacheInvalidate(b, ctx, cache)
}

func BenchmarkPlainRedisDelete(b *testing.B) {
	ctx := context.Background()
	client := newRedisClient(b, ctx)
	const key = "cascache:v4:benchmark:plain:delete"

	if err := client.Set(ctx, key, benchmarkValue, 0).Err(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.Del(ctx, key).Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func newLocalCache(b *testing.B, namespace string) *cascache.Cache[[]byte] {
	b.Helper()

	cacheBackend, err := backend.NewLocal(memstore.New(memstore.Options{}), backend.LocalOptions{
		InvalidationTTL: backend.NoExpiration,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = cacheBackend.Close() })

	return newCache(b, cacheBackend, namespace)
}

func newRedisCache(b *testing.B, ctx context.Context, namespace string) *cascache.Cache[[]byte] {
	b.Helper()

	cacheBackend, err := redisbackend.New(newRedisClient(b, ctx), redisbackend.Options{
		InvalidationTTL: backend.NoExpiration,
	})
	if err != nil {
		b.Fatal(err)
	}
	return newCache(b, cacheBackend, namespace)
}

func newCache(b *testing.B, cacheBackend backend.Backend, namespace string) *cascache.Cache[[]byte] {
	b.Helper()

	cache, err := cascache.New(cascache.Options[[]byte]{
		Namespace:  namespace,
		Backend:    cacheBackend,
		Codec:      codec.Bytes{},
		DefaultTTL: cascache.NoExpiration,
	})
	if err != nil {
		b.Fatal(err)
	}
	return cache
}

func seedCache(b *testing.B, ctx context.Context, cache *cascache.Cache[[]byte]) cascache.Snapshot {
	b.Helper()

	snapshot, err := cache.Snapshot(ctx, "entry")
	if err != nil {
		b.Fatal(err)
	}
	result, err := cache.Set(ctx, "entry", benchmarkValue, snapshot)
	if err != nil {
		b.Fatal(err)
	}
	if result.Outcome != cascache.SetOutcomeStored {
		b.Fatalf("seed outcome: %s", result.Outcome)
	}
	return snapshot
}

func benchmarkCacheRead(b *testing.B, ctx context.Context, cache *cascache.Cache[[]byte]) {
	b.Helper()

	if _, ok, err := cache.Get(ctx, "entry"); err != nil {
		b.Fatal(err)
	} else if !ok {
		b.Fatal("cache miss after seed")
	}

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()

	var got []byte
	for i := 0; i < b.N; i++ {
		var ok bool
		var err error
		got, ok, err = cache.Get(ctx, "entry")
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("cache miss")
		}
	}
	benchmarkSink = got
}

func benchmarkCacheSet(
	b *testing.B,
	ctx context.Context,
	cache *cascache.Cache[[]byte],
	snapshot cascache.Snapshot,
) {
	b.Helper()
	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := cache.Set(ctx, "entry", benchmarkValue, snapshot)
		if err != nil {
			b.Fatal(err)
		}
		if result.Outcome != cascache.SetOutcomeStored {
			b.Fatalf("set outcome: %s", result.Outcome)
		}
	}
}

func benchmarkCacheInvalidate(b *testing.B, ctx context.Context, cache *cascache.Cache[[]byte]) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := cache.Invalidate(ctx, "entry"); err != nil {
			b.Fatal(err)
		}
	}
}

func newRedisClient(b *testing.B, ctx context.Context) *goredis.Client {
	b.Helper()

	addr := os.Getenv(redisAddressEnv)
	if addr == "" {
		b.Skipf("set %s to run Redis benchmarks", redisAddressEnv)
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	b.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(ctx).Err(); err != nil {
		b.Fatal(err)
	}
	return client
}
