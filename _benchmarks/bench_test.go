package benchmarks

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync/atomic"
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

func BenchmarkLocalLoadHit(b *testing.B) {
	ctx := context.Background()
	cache := newLocalCache(b, "benchmark:local:load")
	seedCache(b, ctx, cache)

	load := func(context.Context) ([]byte, error) {
		return nil, errors.New("the loader ran on a hit")
	}

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()

	var got []byte
	for i := 0; i < b.N; i++ {
		var err error
		got, err = cache.Load(ctx, "entry", load)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchmarkSink = got
}

func BenchmarkLocalReload(b *testing.B) {
	ctx := context.Background()
	cache := newLocalCache(b, "benchmark:local:reload")
	benchmarkCacheReload(b, ctx, cache, nil)
}

func BenchmarkRedisReload(b *testing.B) {
	ctx := context.Background()
	client := newRedisClient(b, ctx)
	trips := countRoundTrips(client)

	cacheBackend, err := redisbackend.New(client, redisbackend.Options{
		InvalidationTTL: backend.NoExpiration,
	})
	if err != nil {
		b.Fatal(err)
	}
	cache := newCache(b, cacheBackend, "benchmark:redis:reload")
	benchmarkCacheReload(b, ctx, cache, trips)
}

func BenchmarkPlainRedisReload(b *testing.B) {
	ctx := context.Background()
	client := newRedisClient(b, ctx)
	trips := countRoundTrips(client)
	const key = "cascache:v4:benchmark:plain:reload"

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()
	trips.Store(0)

	for i := 0; i < b.N; i++ {
		if err := client.Del(ctx, key).Err(); err != nil {
			b.Fatal(err)
		}
		if err := client.Get(ctx, key).Err(); !errors.Is(err, goredis.Nil) {
			b.Fatalf("GET after DEL = %v, want a miss", err)
		}
		if err := client.Set(ctx, key, benchmarkValue, 0).Err(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(trips.Load())/float64(b.N), "round-trips/op")
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

func benchmarkCacheReload(
	b *testing.B,
	ctx context.Context,
	cache *cascache.Cache[[]byte],
	trips *atomic.Int64,
) {
	b.Helper()

	var loads int
	load := func(context.Context) ([]byte, error) {
		loads++
		return benchmarkValue, nil
	}
	reload := func() {
		if err := cache.Invalidate(ctx, "entry"); err != nil {
			b.Fatal(err)
		}
		got, err := cache.Load(ctx, "entry", load)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkSink = got
	}
	reload() // Warm up Lua scripts.

	b.SetBytes(int64(len(benchmarkValue)))
	b.ReportAllocs()
	b.ResetTimer()
	loads = 0
	if trips != nil {
		trips.Store(0)
	}

	for i := 0; i < b.N; i++ {
		reload()
	}
	if loads != b.N {
		b.Fatalf("the loader ran %d times in %d cycles", loads, b.N)
	}
	if trips != nil {
		b.ReportMetric(float64(trips.Load())/float64(b.N), "round-trips/op")
	}
}

func countRoundTrips(client *goredis.Client) *atomic.Int64 {
	h := &roundTripHook{}
	client.AddHook(h)
	return &h.n
}

type roundTripHook struct{ n atomic.Int64 }

func (h *roundTripHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *roundTripHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmd)
	}
}

func (h *roundTripHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		h.n.Add(1)
		return next(ctx, cmds)
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
