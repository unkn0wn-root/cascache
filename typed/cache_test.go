package typed_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
	"github.com/unkn0wn-root/cascache/v4/typed"
)

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var ada = user{ID: "42", Name: "Ada"}

type counters struct {
	mu sync.Mutex

	hits, misses, fills   int
	setSkipped            int
	invalidated           int
	loads                 int
	loadFailed            int
	errs                  []cascache.Op
	rejected              []cascache.RejectReason
	storeRejected         int
	cleanupFailed         int
	loaderPanic           int
	lastFillTTL           time.Duration
	lastLoadOutcomeString string
}

func (c *counters) metrics() typed.Metrics {
	return typed.Metrics{
		Hit:  func() { c.bump(&c.hits) },
		Miss: func() { c.bump(&c.misses) },
		Fill: func(ttl time.Duration) {
			c.mu.Lock()
			c.fills++
			c.lastFillTTL = ttl
			c.mu.Unlock()
		},
		SetSkipped:  func() { c.bump(&c.setSkipped) },
		Invalidated: func() { c.bump(&c.invalidated) },
		Error: func(op cascache.Op, _ error) {
			c.mu.Lock()
			c.errs = append(c.errs, op)
			c.mu.Unlock()
		},
		Load: func(outcome cascache.LoadOutcome) {
			c.mu.Lock()
			c.loads++
			c.lastLoadOutcomeString = outcome.String()
			c.mu.Unlock()
		},
		LoadFailed: func(error) { c.bump(&c.loadFailed) },
		EntryRejected: func(reason cascache.RejectReason) {
			c.mu.Lock()
			c.rejected = append(c.rejected, reason)
			c.mu.Unlock()
		},
		StoreRejected: func() { c.bump(&c.storeRejected) },
		CleanupFailed: func() { c.bump(&c.cleanupFailed) },
		LoaderPanic:   func() { c.bump(&c.loaderPanic) },
	}
}

func (c *counters) bump(n *int) {
	c.mu.Lock()
	*n++
	c.mu.Unlock()
}

func (c *counters) snapshot() counters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return counters{
		hits: c.hits, misses: c.misses, fills: c.fills,
		setSkipped: c.setSkipped, invalidated: c.invalidated,
		loads: c.loads, loadFailed: c.loadFailed,
		errs:          append([]cascache.Op(nil), c.errs...),
		rejected:      append([]cascache.RejectReason(nil), c.rejected...),
		storeRejected: c.storeRejected, cleanupFailed: c.cleanupFailed,
		loaderPanic: c.loaderPanic, lastFillTTL: c.lastFillTTL,
		lastLoadOutcomeString: c.lastLoadOutcomeString,
	}
}

func newBackend(t testing.TB, store *memstore.Store) backend.Backend {
	t.Helper()
	b, err := backend.NewLocal(store, backend.LocalOptions{CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func newCache(
	t testing.TB,
	tweak func(*typed.Options[string, user]),
) (*typed.Cache[string, user], *counters, *memstore.Store) {
	t.Helper()

	store := memstore.New(memstore.Options{})
	c := &counters{}
	opts := typed.Options[string, user]{
		Config: typed.Config{
			Namespace: "user",
			MaxTTL:    time.Hour,
			MinTTL:    30 * time.Minute,
			Jitter:    0.5,
			Metrics:   c.metrics(),
		},
		KeyFunc: func(k string) string { return k },
		Codec:   codec.JSON[user]{},
		Backend: newBackend(t, store),
	}
	if tweak != nil {
		tweak(&opts)
	}

	cache, err := typed.New(opts)
	if err != nil {
		t.Fatalf("typed.New: %v", err)
	}
	return cache, c, store
}

func TestConfigValidate(t *testing.T) {
	base := typed.Config{Namespace: "n", MaxTTL: time.Hour}

	cases := []struct {
		name  string
		tweak func(*typed.Config)
		want  string
	}{
		{"no namespace", func(c *typed.Config) { c.Namespace = "" }, "namespace"},
		{"zero max TTL", func(c *typed.Config) { c.MaxTTL = 0 }, "max TTL"},
		{"negative min TTL", func(c *typed.Config) { c.MinTTL = -time.Second }, "min TTL"},
		{"min above max", func(c *typed.Config) { c.MinTTL = 2 * time.Hour }, "min TTL"},
		{"jitter above one", func(c *typed.Config) { c.Jitter = 1.5 }, "jitter"},
		{"jitter below zero", func(c *typed.Config) { c.Jitter = -0.1 }, "jitter"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.tweak(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want an error mentioning %q", err, tc.want)
			}
		})
	}

	if err := base.Validate(); err != nil {
		t.Fatalf("Validate on a valid config: %v", err)
	}
}

func TestNewRequiresABackend(t *testing.T) {
	_, err := typed.New(typed.Options[string, user]{
		Config:  typed.Config{Namespace: "user", MaxTTL: time.Hour},
		KeyFunc: func(k string) string { return k },
		Codec:   codec.JSON[user]{},
	})
	if err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("New without a backend = %v, want an error mentioning the backend", err)
	}
}

func TestNewRequiresKeyFuncAndCodec(t *testing.T) {
	store := memstore.New(memstore.Options{})
	base := typed.Options[string, user]{
		Config:  typed.Config{Namespace: "user", MaxTTL: time.Hour},
		KeyFunc: func(k string) string { return k },
		Codec:   codec.JSON[user]{},
		Backend: newBackend(t, store),
	}

	noKey := base
	noKey.KeyFunc = nil
	if _, err := typed.New(noKey); err == nil || !strings.Contains(err.Error(), "key func") {
		t.Fatalf("New without a key func = %v", err)
	}

	noCodec := base
	noCodec.Codec = nil
	if _, err := typed.New(noCodec); err == nil || !strings.Contains(err.Error(), "codec") {
		t.Fatalf("New without a codec = %v", err)
	}
}

func TestNewRedisChecksInvalidationTTLAgainstMaxTTL(t *testing.T) {
	client := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() { _ = client.Close() })

	newRedis := func(maxTTL, fenceTTL time.Duration) error {
		_, err := typed.NewRedis(client, typed.Options[string, user]{
			Config:          typed.Config{Namespace: "user", MaxTTL: maxTTL},
			KeyFunc:         func(k string) string { return k },
			Codec:           codec.JSON[user]{},
			InvalidationTTL: fenceTTL,
		})
		return err
	}

	if err := newRedis(time.Hour, 2*time.Hour); err != nil {
		t.Fatalf("an invalidation TTL above MaxTTL was rejected: %v", err)
	}
	if err := newRedis(time.Hour, time.Hour); err != nil {
		t.Fatalf("an invalidation TTL equal to MaxTTL was rejected: %v", err)
	}
	if err := newRedis(time.Hour, cascache.NoExpiration); err != nil {
		t.Fatalf("non-expiring fences were rejected: %v", err)
	}
	if err := newRedis(time.Hour, 30*time.Minute); err == nil {
		t.Fatal("an invalidation TTL below MaxTTL was accepted")
	}

	if err := newRedis(redisbackend.DefaultInvalidationTTL+time.Hour, 0); err == nil {
		t.Fatal("a MaxTTL above the default invalidation TTL was accepted with no InvalidationTTL set")
	}
	if err := newRedis(time.Hour, 0); err != nil {
		t.Fatalf("a MaxTTL below the default invalidation TTL was rejected: %v", err)
	}
	if err := newRedis(time.Hour, -time.Second); err == nil {
		t.Fatal("a negative invalidation TTL was accepted")
	}
}

func TestGetRecordsHitMissAndError(t *testing.T) {
	ctx := context.Background()
	failing := errors.New("store is down")

	var fail bool
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Get: func(string) error {
			if fail {
				return failing
			}
			return nil
		},
	}})

	c := &counters{}
	cache, err := typed.New(typed.Options[string, user]{
		Config: typed.Config{
			Namespace: "user", MaxTTL: time.Hour, Metrics: c.metrics(),
		},
		KeyFunc: func(k string) string { return k },
		Codec:   codec.JSON[user]{},
		Backend: newBackend(t, store),
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get = %v, %v; want a miss", ok, err)
	}

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Set(ctx, "42", ada, fence); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := cache.Get(ctx, "42"); !ok || err != nil {
		t.Fatalf("Get = %v, %v; want a hit", ok, err)
	}

	fail = true
	if _, ok, err := cache.Get(ctx, "42"); ok || !errors.Is(err, failing) {
		t.Fatalf("Get = %v, %v; want the failure", ok, err)
	}

	got := c.snapshot()
	if got.hits != 1 || got.misses != 1 {
		t.Fatalf("hits = %d, misses = %d; want 1 and 1", got.hits, got.misses)
	}
	if len(got.errs) != 1 || got.errs[0] != cascache.OpGet {
		t.Fatalf("errors = %v, want one OpGet", got.errs)
	}
}

func TestSetRecordsAFillOnlyWhenStored(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, nil)

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cache.Set(ctx, "42", ada, fence)
	if err != nil || res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("Set = %+v, %v", res, err)
	}

	stale := fence
	if err := cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}
	res, err = cache.Set(ctx, "42", ada, stale)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != cascache.SetOutcomeConflict {
		t.Fatalf("Outcome = %v, want conflict", res.Outcome)
	}

	got := c.snapshot()
	if got.fills != 1 {
		t.Fatalf("fills = %d, want 1", got.fills)
	}
	if got.setSkipped != 1 {
		t.Fatalf("setSkipped = %d, want 1", got.setSkipped)
	}
	if got.invalidated != 1 {
		t.Fatalf("invalidated = %d, want 1", got.invalidated)
	}
}

func TestSetWithTTLUsesTheTTLItWasGiven(t *testing.T) {
	ctx := context.Background()
	cache, _, _ := newCache(t, nil)

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	const ttl = 45 * time.Minute
	res, err := cache.SetWithTTL(ctx, "42", ada, fence, ttl)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != cascache.SetOutcomeStored || res.EffectiveTTL != ttl {
		t.Fatalf("SetWithTTL = %+v, want stored at %v", res, ttl)
	}
}

func TestSetUsesTheJitteredRange(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, nil)

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cache.Set(ctx, "42", ada, fence)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("Outcome = %v, want stored", res.Outcome)
	}
	if res.EffectiveTTL < 30*time.Minute || res.EffectiveTTL > time.Hour {
		t.Fatalf("EffectiveTTL = %v, want it between MinTTL and MaxTTL", res.EffectiveTTL)
	}
	if got := c.snapshot().lastFillTTL; got != res.EffectiveTTL {
		t.Fatalf("Fill recorded %v, want %v", got, res.EffectiveTTL)
	}
}

func TestLoadRecordsMissThenHitAndFillsInRange(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, nil)

	load := func(context.Context) (user, error) { return ada, nil }

	if _, err := cache.Load(ctx, "42", load); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Load(ctx, "42", load); err != nil {
		t.Fatal(err)
	}

	got := c.snapshot()
	if got.misses != 1 || got.hits != 1 {
		t.Fatalf("misses = %d, hits = %d; want 1 and 1", got.misses, got.hits)
	}
	if got.fills != 1 {
		t.Fatalf("fills = %d, want 1", got.fills)
	}
	if got.lastFillTTL < 30*time.Minute || got.lastFillTTL > time.Hour {
		t.Fatalf("fill TTL = %v, want it between MinTTL and MaxTTL", got.lastFillTTL)
	}
	if got.loads != 1 || got.lastLoadOutcomeString != "loaded" {
		t.Fatalf("loads = %d, last outcome = %q", got.loads, got.lastLoadOutcomeString)
	}
}

func TestLoadRecordsAFailedRunOnce(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, nil)
	failure := errors.New("source is down")

	if _, err := cache.Load(ctx, "42", func(context.Context) (user, error) {
		return user{}, failure
	}); !errors.Is(err, failure) {
		t.Fatalf("Load = %v, want %v", err, failure)
	}

	if got := c.snapshot(); got.loadFailed != 1 {
		t.Fatalf("loadFailed = %d, want 1", got.loadFailed)
	}
}

func TestMetricsSeeRejectedEntries(t *testing.T) {
	ctx := context.Background()
	cache, c, store := newCache(t, nil)

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Set(ctx, "42", ada, fence); err != nil {
		t.Fatal(err)
	}

	key, raw := onlyEntry(t, store)
	raw[len(raw)-1]++
	if _, err := store.Set(ctx, key, raw, 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get = %v, %v; want a miss", ok, err)
	}

	got := c.snapshot()
	if len(got.rejected) != 1 || got.rejected[0] != cascache.RejectFrameCorrupt {
		t.Fatalf("rejected = %v, want one frame_corrupt", got.rejected)
	}
}

func TestMetricsSeeALoaderPanic(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, nil)

	_, err := cache.Load(ctx, "42", func(context.Context) (user, error) {
		panic("boom")
	})
	if !errors.Is(err, cascache.ErrLoaderPanic) {
		t.Fatalf("Load = %v, want ErrLoaderPanic", err)
	}
	if got := c.snapshot(); got.loaderPanic != 1 {
		t.Fatalf("loaderPanic = %d, want 1", got.loaderPanic)
	}
}

func TestDisabledCacheIsAPassThrough(t *testing.T) {
	ctx := context.Background()
	cache, _, store := newCache(t, func(o *typed.Options[string, user]) { o.Disabled = true })

	if cache.Enabled() {
		t.Fatal("Enabled reported true for a disabled cache")
	}

	var calls int
	got, err := cache.Load(ctx, "42", func(context.Context) (user, error) {
		calls++
		return ada, nil
	})
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v", got, err)
	}
	if calls != 1 {
		t.Fatalf("the loader ran %d times, want 1", calls)
	}
	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get = %v, %v; want a miss", ok, err)
	}
	if err := cache.Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if store.Len() != 0 {
		t.Fatal("a disabled cache wrote to the store")
	}
}

func TestKeyFuncSeparatesKeys(t *testing.T) {
	ctx := context.Background()
	cache, _, _ := newCache(t, func(o *typed.Options[string, user]) {
		o.KeyFunc = func(k string) string { return "prefix:" + k }
	})

	fence, err := cache.Snapshot(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Set(ctx, "a", ada, fence); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := cache.Get(ctx, "b"); ok || err != nil {
		t.Fatalf("Get(b) = %v, %v; want a miss", ok, err)
	}
	if _, ok, err := cache.Get(ctx, "a"); !ok || err != nil {
		t.Fatalf("Get(a) = %v, %v; want a hit", ok, err)
	}
}

func TestInvalidatorRetiresThroughTheSameKeyFunc(t *testing.T) {
	ctx := context.Background()
	cache, _, _ := newCache(t, func(o *typed.Options[string, user]) {
		o.KeyFunc = func(k string) string { return "prefix:" + k }
	})

	if _, err := cache.Load(ctx, "a", func(context.Context) (user, error) { return ada, nil }); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := cache.Get(ctx, "a"); !ok {
		t.Fatal("the value was not cached")
	}

	if err := cache.Invalidator().Invalidate(ctx, "a"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if _, ok, _ := cache.Get(ctx, "a"); ok {
		t.Fatal("the entry survived invalidation")
	}
}

// failingInvalidate is a backend whose invalidations fail; everything else
// behaves normally.
type failingInvalidate struct {
	backend.Backend
	err error
}

func (b failingInvalidate) Invalidate(
	context.Context,
	backend.Key,
	backend.Fence,
) (backend.InvalidateResult, error) {
	return backend.InvalidateResult{}, b.err
}

func TestInvalidatorRecordsTheSameMetricsAsTheCache(t *testing.T) {
	ctx := context.Background()
	failing := errors.New("store is down")

	c := &counters{}
	newInvalidator := func(b backend.Backend) *typed.Invalidator[string] {
		t.Helper()
		cache, err := typed.New(typed.Options[string, user]{
			Config: typed.Config{
				Namespace: "user", MaxTTL: time.Hour, Metrics: c.metrics(),
			},
			KeyFunc: func(k string) string { return k },
			Codec:   codec.JSON[user]{},
			Backend: b,
		})
		if err != nil {
			t.Fatal(err)
		}
		return cache.Invalidator()
	}

	working := newBackend(t, memstore.New(memstore.Options{}))
	if err := newInvalidator(working).Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if got := c.snapshot().invalidated; got != 1 {
		t.Fatalf("invalidated = %d, want 1", got)
	}

	broken := newInvalidator(failingInvalidate{Backend: working, err: failing})
	if err := broken.Invalidate(ctx, "42"); !errors.Is(err, failing) {
		t.Fatalf("Invalidate = %v, want the failure", err)
	}

	got := c.snapshot()
	if got.invalidated != 1 {
		t.Fatalf("invalidated = %d, want the failed call not to count", got.invalidated)
	}
	if len(got.errs) != 1 || got.errs[0] != cascache.OpInvalidate {
		t.Fatalf("errors = %v, want one %v", got.errs, cascache.OpInvalidate)
	}
}

// A disabled cache does nothing, so its invalidator records nothing.
func TestDisabledInvalidatorRecordsNothing(t *testing.T) {
	ctx := context.Background()
	cache, c, _ := newCache(t, func(o *typed.Options[string, user]) { o.Disabled = true })

	if err := cache.Invalidator().Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if got := c.snapshot().invalidated; got != 0 {
		t.Fatalf("invalidated = %d, want 0", got)
	}
}

func onlyEntry(t testing.TB, store *memstore.Store) (string, []byte) {
	t.Helper()

	keys := store.Keys()
	var values []string
	for _, k := range keys {
		if strings.HasPrefix(k, backend.ValueRoot) {
			values = append(values, k)
		}
	}
	if len(values) != 1 {
		t.Fatalf("store holds %d value keys, want 1: %v", len(values), keys)
	}

	raw, ok, err := store.Get(context.Background(), values[0])
	if err != nil || !ok {
		t.Fatalf("store.Get(%q) = %v, %v", values[0], ok, err)
	}
	return values[0], raw
}

type fencesThatCannotSnapshot struct{}

var errSnapshotFailed = errors.New("fence store is down")

func (fencesThatCannotSnapshot) Ensure(
	context.Context, backend.Key, backend.Fence,
) (backend.Fence, error) {
	return backend.Fence{}, errSnapshotFailed
}

func (fencesThatCannotSnapshot) Read(
	context.Context, backend.Key,
) (backend.Fence, bool, error) {
	return backend.Fence{}, false, nil
}

func (fencesThatCannotSnapshot) Retain(
	context.Context, backend.Key, backend.Fence,
) (bool, error) {
	return false, nil
}

func (fencesThatCannotSnapshot) Replace(
	context.Context, backend.Key, backend.Fence,
) error {
	return nil
}

func (fencesThatCannotSnapshot) Lifetime() time.Duration { return 0 }

func TestFillErrorsAreLabelledWithTheOperationThatFailed(t *testing.T) {
	ctx := context.Background()

	b, err := backend.NewComposite(memstore.New(memstore.Options{}), fencesThatCannotSnapshot{})
	if err != nil {
		t.Fatal(err)
	}

	c := &counters{}
	cache, err := typed.New(typed.Options[string, user]{
		Config: typed.Config{
			Namespace: "user", MaxTTL: time.Hour, Metrics: c.metrics(),
		},
		KeyFunc: func(k string) string { return k },
		Codec:   codec.JSON[user]{},
		Backend: b,
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := cache.Load(ctx, "42", func(context.Context) (user, error) { return ada, nil })
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v", got, err)
	}

	errs := c.snapshot().errs
	if len(errs) != 1 || errs[0] != cascache.OpSnapshot {
		t.Fatalf("reported operations = %v, want one snapshot", errs)
	}
}
