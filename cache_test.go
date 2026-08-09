package cascache_test

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	redisbackend "github.com/unkn0wn-root/cascache/v4/backend/redis"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var ada = user{ID: "42", Name: "Ada"}

func TestSnapshotIsOpaque(t *testing.T) {
	if reflect.TypeOf(cascache.Snapshot{}).Comparable() {
		t.Fatal("Snapshot is comparable; equality would expose unsupported semantics")
	}
}

type fakeFences struct {
	mu       sync.Mutex
	fences   map[string]backend.Fence
	lifetime time.Duration
	failWith error
}

func newFakeFences() *fakeFences {
	return &fakeFences{fences: make(map[string]backend.Fence)}
}

var _ backend.FenceStore = (*fakeFences)(nil)

func (f *fakeFences) Lifetime() time.Duration { return f.lifetime }

func (f *fakeFences) fail() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failWith
}

func (f *fakeFences) setFailure(err error) {
	f.mu.Lock()
	f.failWith = err
	f.mu.Unlock()
}

func (f *fakeFences) forget(key backend.Key) {
	f.mu.Lock()
	delete(f.fences, key.ID())
	f.mu.Unlock()
}

func (f *fakeFences) Ensure(_ context.Context, key backend.Key, candidate backend.Fence) (backend.Fence, error) {
	if err := f.fail(); err != nil {
		return backend.Fence{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if fence, ok := f.fences[key.ID()]; ok {
		return fence, nil
	}
	f.fences[key.ID()] = candidate
	return candidate, nil
}

func (f *fakeFences) Read(_ context.Context, key backend.Key) (backend.Fence, bool, error) {
	if err := f.fail(); err != nil {
		return backend.Fence{}, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	fence, ok := f.fences[key.ID()]
	return fence, ok, nil
}

func (f *fakeFences) Retain(_ context.Context, key backend.Key, expected backend.Fence) (bool, error) {
	if err := f.fail(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	fence, ok := f.fences[key.ID()]
	return ok && fence.Equal(expected), nil
}

func (f *fakeFences) Replace(_ context.Context, key backend.Key, next backend.Fence) error {
	if err := f.fail(); err != nil {
		return err
	}
	f.mu.Lock()
	f.fences[key.ID()] = next
	f.mu.Unlock()
	return nil
}

type eventLog struct {
	mu     sync.Mutex
	events []cascache.Event
}

func (l *eventLog) Observe(e cascache.Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *eventLog) all() []cascache.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]cascache.Event(nil), l.events...)
}

func (l *eventLog) find(t cascache.EventType) (cascache.Event, bool) {
	for _, e := range l.all() {
		if e.Type == t {
			return e, true
		}
	}
	return cascache.Event{}, false
}

type harness struct {
	cache   *cascache.Cache[user]
	store   *memstore.Store
	fences  *fakeFences
	backend backend.Backend
	events  *eventLog
	space   func(string) backend.Key
}

func newHarness(t testing.TB, tweak func(*cascache.Options[user])) *harness {
	t.Helper()

	const namespace = "test"

	store := memstore.New(memstore.Options{})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatalf("NewComposite: %v", err)
	}

	events := &eventLog{}
	opts := cascache.Options[user]{
		Namespace: namespace,
		Backend:   b,
		Codec:     codec.JSON[user]{},
		Observer:  events,
	}
	if tweak != nil {
		tweak(&opts)
	}

	cache, err := cascache.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return &harness{
		cache:   cache,
		store:   store,
		fences:  fences,
		backend: b,
		events:  events,
		space:   func(key string) backend.Key { return keyFor(t, namespace, key) },
	}
}

func keyFor(t testing.TB, namespace, key string) backend.Key {
	t.Helper()
	k, err := backend.NewKey("s:" + strconv.Itoa(len(namespace)) + ":" + namespace + ":" + key)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return k
}

func (h *harness) fill(t testing.TB, key string, value user) cascache.SetResult {
	t.Helper()

	fence, err := h.cache.Snapshot(context.Background(), key)
	if err != nil {
		t.Fatalf("Snapshot(%q): %v", key, err)
	}
	res, err := h.cache.Set(context.Background(), key, value, fence)
	if err != nil {
		t.Fatalf("Set(%q): %v", key, err)
	}
	if res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("Set(%q) = %v, want stored", key, res.Outcome)
	}
	return res
}

func (h *harness) mustGet(t testing.TB, key string) (user, bool) {
	t.Helper()
	v, ok, err := h.cache.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return v, ok
}

func (h *harness) storedBytes(t testing.TB, key string) ([]byte, bool) {
	t.Helper()
	raw, ok, err := h.store.Get(context.Background(), backend.ValueKey(h.space(key)))
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	return raw, ok
}

func TestNewValidatesOptions(t *testing.T) {
	store := memstore.New(memstore.Options{})
	b, err := backend.NewLocal(store, backend.LocalOptions{CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	base := cascache.Options[user]{Namespace: "n", Backend: b, Codec: codec.JSON[user]{}}

	cases := []struct {
		name  string
		tweak func(*cascache.Options[user])
		want  error
	}{
		{"no namespace", func(o *cascache.Options[user]) { o.Namespace = "" }, cascache.ErrNoNamespace},
		{"no backend", func(o *cascache.Options[user]) { o.Backend = nil }, cascache.ErrNoBackend},
		{"no codec", func(o *cascache.Options[user]) { o.Codec = nil }, cascache.ErrNoCodec},
		{"negative TTL", func(o *cascache.Options[user]) { o.DefaultTTL = -time.Second }, cascache.ErrInvalidTTL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			tc.tweak(&opts)
			if _, err := cascache.New(opts); !errors.Is(err, tc.want) {
				t.Fatalf("New = %v, want %v", err, tc.want)
			}
		})
	}

	opts := base
	var typedNil *backend.Local
	opts.Backend = typedNil
	if _, err := cascache.New(opts); !errors.Is(err, cascache.ErrNoBackend) {
		t.Fatalf("New(typed nil backend) = %v, want ErrNoBackend", err)
	}

	if _, err := cascache.New(base); err != nil {
		t.Fatalf("New with valid options: %v", err)
	}
}

func TestSnapshotSetGet(t *testing.T) {
	h := newHarness(t, nil)

	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("Get on an empty cache returned a value")
	}

	h.fill(t, "42", ada)

	got, ok := h.mustGet(t, "42")
	if !ok || got != ada {
		t.Fatalf("Get = %+v, %v; want %+v", got, ok, ada)
	}
}

func TestSnapshotRemainsCurrentUntilInvalidation(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	first, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []cascache.Snapshot{first, second} {
		res, err := h.cache.Set(ctx, "42", ada, snapshot)
		if err != nil || res.Outcome != cascache.SetOutcomeStored {
			t.Fatalf("Set before invalidation = %v, %v; want stored", res.Outcome, err)
		}
	}

	if err := h.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}
	third, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []cascache.Snapshot{first, second} {
		res, err := h.cache.Set(ctx, "42", ada, snapshot)
		if err != nil || res.Outcome != cascache.SetOutcomeConflict {
			t.Fatalf("Set with old snapshot = %v, %v; want conflict", res.Outcome, err)
		}
	}
	res, err := h.cache.Set(ctx, "42", ada, third)
	if err != nil || res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("Set with new snapshot = %v, %v; want stored", res.Outcome, err)
	}
}

func TestInvalidationDuringALoadRefusesTheWrite(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	fence, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}

	res, err := h.cache.Set(ctx, "42", ada, fence)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if res.Outcome != cascache.SetOutcomeConflict {
		t.Fatalf("Set = %v, want conflict", res.Outcome)
	}
	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a value loaded before an invalidation was served after it")
	}
}

func TestMissingFenceIsAMissAndTheEntryGoes(t *testing.T) {
	h := newHarness(t, nil)
	h.fill(t, "42", ada)

	h.fences.forget(h.space("42"))

	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a value with no fence was served")
	}

	e, found := h.events.find(cascache.EventEntryRejected)
	if !found || e.Reason != cascache.RejectStateMissing {
		t.Fatalf("events = %+v, want an entry_rejected with state_missing", h.events.all())
	}
	if e.Key != "42" {
		t.Fatalf("Event.Key = %q, want the caller's key", e.Key)
	}
	if _, ok := h.storedBytes(t, "42"); ok {
		t.Fatal("the unjudgeable entry was left in the store")
	}
}

func TestFenceMismatchIsAMissAndTheEntryGoes(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	h.fill(t, "42", ada)

	if err := h.fences.Replace(ctx, h.space("42"), backend.NewFence()); err != nil {
		t.Fatal(err)
	}

	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a retired value was served")
	}
	e, found := h.events.find(cascache.EventEntryRejected)
	if !found || e.Reason != cascache.RejectRetired {
		t.Fatalf("events = %+v, want an entry_rejected with retired", h.events.all())
	}
	if _, ok := h.storedBytes(t, "42"); ok {
		t.Fatal("the retired entry was left in the store")
	}
}

func TestDamagedFrameIsAMissAndTheEntryGoes(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	h.fill(t, "42", ada)

	raw, ok := h.storedBytes(t, "42")
	if !ok {
		t.Fatal("nothing was stored")
	}
	raw[len(raw)-1]++
	if _, err := h.store.Set(ctx, backend.ValueKey(h.space("42")), raw, 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a damaged frame was decoded and served")
	}
	e, found := h.events.find(cascache.EventEntryRejected)
	if !found || e.Reason != cascache.RejectFrameCorrupt {
		t.Fatalf("events = %+v, want an entry_rejected with frame_corrupt", h.events.all())
	}
	if _, ok := h.storedBytes(t, "42"); ok {
		t.Fatal("the damaged entry was left in the store")
	}
}

func TestFrameFromAnotherVersionIsLeftInPlace(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	h.fill(t, "42", ada)
	raw, ok := h.storedBytes(t, "42")
	if !ok {
		t.Fatal("nothing was stored")
	}

	// Byte 4 is the format version. A frame from a build that is not this one
	// is unreadable here, but perfectly readable there.
	raw[4]++
	if _, err := h.store.Set(ctx, backend.ValueKey(h.space("42")), raw, 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a frame from another version was decoded")
	}
	e, found := h.events.find(cascache.EventEntryRejected)
	if !found || e.Reason != cascache.RejectUnsupportedFormat {
		t.Fatalf("events = %+v, want an entry_rejected with unsupported_format", h.events.all())
	}
	if _, ok := h.storedBytes(t, "42"); !ok {
		t.Fatal("an entry another build can still read was deleted")
	}
}

func TestRejectionOnlyRemovesTheBytesItJudged(t *testing.T) {
	ctx := context.Background()

	var (
		store *memstore.Store
		key   string
		good  []byte
		armed bool
		gets  int
	)
	store = memstore.New(memstore.Options{Hook: memstore.Hook{
		Get: func(k string) error {
			if !armed || k != key {
				return nil
			}
			gets++
			// Replace the bytes during Discard's compare step.
			if gets == 2 {
				_, _ = store.Set(ctx, key, good, 1, time.Hour)
			}
			return nil
		},
	}})

	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	cache, err := cascache.New(cascache.Options[user]{
		Namespace: "test", Backend: b, Codec: codec.JSON[user]{},
	})
	if err != nil {
		t.Fatal(err)
	}

	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Set(ctx, "42", ada, fence); err != nil {
		t.Fatal(err)
	}

	key = backend.ValueKey(keyFor(t, "test", "42"))
	good, _, err = store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	damaged := append([]byte(nil), good...)
	damaged[len(damaged)-1]++
	if _, err := store.Set(ctx, key, damaged, 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	armed = true
	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get of a damaged frame = %v, %v; want a miss", ok, err)
	}
	armed = false

	got, ok, err := cache.Get(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got != ada {
		t.Fatalf("self-healing deleted the replacement: %+v, %v", got, ok)
	}
}

func TestGetFailsClosedWhenTheBackendCannotBeRead(t *testing.T) {
	h := newHarness(t, nil)
	h.fill(t, "42", ada)

	failure := errors.New("fence store is down")
	h.fences.setFailure(failure)

	_, ok, err := h.cache.Get(context.Background(), "42")
	if ok {
		t.Fatal("Get returned a value it could not judge")
	}
	if !errors.Is(err, failure) {
		t.Fatalf("Get error = %v, want %v", err, failure)
	}

	var opErr *cascache.OpError
	if !errors.As(err, &opErr) || opErr.Op != cascache.OpGet || opErr.Key != "42" {
		t.Fatalf("error lacks operation context: %#v", err)
	}
	if _, found := h.events.find(cascache.EventOperationFailed); !found {
		t.Fatal("a failed operation was not observed")
	}
}

func TestSnapshotFailsClosed(t *testing.T) {
	h := newHarness(t, nil)
	failure := errors.New("fence store is down")
	h.fences.setFailure(failure)

	snapshot, err := h.cache.Snapshot(context.Background(), "42")
	if !errors.Is(err, failure) {
		t.Fatalf("Snapshot error = %v, want %v", err, failure)
	}
	if _, setErr := h.cache.Set(context.Background(), "42", ada, snapshot); !errors.Is(setErr, cascache.ErrInvalidSnapshot) {
		t.Fatalf("Set with failed snapshot = %v, want ErrInvalidSnapshot", setErr)
	}
}

func TestSetRejectsAnInvalidFence(t *testing.T) {
	h := newHarness(t, nil)

	_, err := h.cache.Set(context.Background(), "42", ada, cascache.Snapshot{})
	if !errors.Is(err, cascache.ErrInvalidSnapshot) {
		t.Fatalf("Set(zero snapshot) = %v, want ErrInvalidSnapshot", err)
	}
	if _, ok := h.storedBytes(t, "42"); ok {
		t.Fatal("a write with no snapshot stored something")
	}
}

func TestSetTTL(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"explicit", time.Minute, time.Minute},
		{"zero uses the default", 0, cascache.DefaultEntryTTL},
		{"no expiration", cascache.NoExpiration, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			ctx := context.Background()

			fence, err := h.cache.Snapshot(ctx, "42")
			if err != nil {
				t.Fatal(err)
			}
			res, err := h.cache.SetWithTTL(ctx, "42", ada, fence, tc.ttl)
			if err != nil {
				t.Fatal(err)
			}
			if res.Outcome != cascache.SetOutcomeStored {
				t.Fatalf("Outcome = %v, want stored", res.Outcome)
			}
			if res.EffectiveTTL != tc.want {
				t.Fatalf("EffectiveTTL = %v, want %v", res.EffectiveTTL, tc.want)
			}
		})
	}

	t.Run("negative is rejected", func(t *testing.T) {
		h := newHarness(t, nil)
		ctx := context.Background()

		fence, err := h.cache.Snapshot(ctx, "42")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.cache.SetWithTTL(
			ctx,
			"42",
			ada,
			fence,
			-time.Second,
		); !errors.Is(
			err,
			cascache.ErrInvalidTTL,
		) {
			t.Fatalf("SetWithTTL(negative ttl) = %v, want ErrInvalidTTL", err)
		}
	})
}

func TestSetUsesTheComputedTTL(t *testing.T) {
	const ttl = 90 * time.Second
	h := newHarness(t, func(o *cascache.Options[user]) {
		o.ComputeTTL = func() (time.Duration, error) { return ttl, nil }
	})
	ctx := context.Background()

	fence, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	res, err := h.cache.Set(ctx, "42", ada, fence)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != cascache.SetOutcomeStored || res.EffectiveTTL != ttl {
		t.Fatalf("Set = %+v, want stored at %v", res, ttl)
	}

	failing := newHarness(t, func(o *cascache.Options[user]) {
		o.ComputeTTL = func() (time.Duration, error) { return 0, errors.New("no ttl") }
	})
	fence, err = failing.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}

	_, err = failing.cache.Set(ctx, "42", ada, fence)
	if !errors.Is(err, cascache.ErrComputeTTL) {
		t.Fatalf("Set = %v, want ErrComputeTTL", err)
	}

	var opErr *cascache.OpError
	if !errors.As(err, &opErr) || opErr.Op != cascache.OpComputeTTL {
		t.Fatalf("error operation = %v, want compute_ttl", opErr)
	}
	e, found := failing.events.find(cascache.EventOperationFailed)
	if !found || e.Op != cascache.OpComputeTTL {
		t.Fatalf("observed %+v, want an operation_failed for compute_ttl", failing.events.all())
	}
}

func TestSetRejectsANonpositiveCost(t *testing.T) {
	h := newHarness(t, func(o *cascache.Options[user]) {
		o.ComputeSetCost = func(string, []byte) int64 { return 0 }
	})
	ctx := context.Background()

	fence, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.cache.Set(ctx, "42", ada, fence); !errors.Is(err, cascache.ErrInvalidCost) {
		t.Fatalf("Set = %v, want ErrInvalidCost", err)
	}
}

func TestSetCostSeesTheCallerKeyAndTheWholeFrame(t *testing.T) {
	var (
		gotKey string
		gotLen int
	)
	h := newHarness(t, func(o *cascache.Options[user]) {
		o.ComputeSetCost = func(key string, raw []byte) int64 {
			gotKey, gotLen = key, len(raw)
			return int64(len(raw))
		}
	})

	h.fill(t, "42", ada)

	if gotKey != "42" {
		t.Fatalf("cost func key = %q, want the caller's key", gotKey)
	}
	raw, _ := h.storedBytes(t, "42")
	if gotLen != len(raw) {
		t.Fatalf("cost func saw %d bytes, want the stored frame's %d", gotLen, len(raw))
	}
}

func TestBackendRejectionIsReported(t *testing.T) {
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Set: func(string, []byte) (bool, error) { return false, nil },
	}})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	events := &eventLog{}
	cache, err := cascache.New(cascache.Options[user]{
		Namespace: "test", Backend: b, Codec: codec.JSON[user]{}, Observer: events,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	res, err := cache.Set(ctx, "42", ada, fence)
	if err != nil {
		t.Fatalf("a declined write is not an error: %v", err)
	}
	if res.Outcome != cascache.SetOutcomeBackendRejected {
		t.Fatalf("Outcome = %v, want backend_rejected", res.Outcome)
	}
	if _, found := events.find(cascache.EventStoreRejected); !found {
		t.Fatalf("events = %+v, want a store_rejected", events.all())
	}
}

func TestInvalidateReportsBackendFailure(t *testing.T) {
	h := newHarness(t, nil)
	failure := errors.New("fence store is down")
	h.fences.setFailure(failure)

	if err := h.cache.Invalidate(context.Background(), "42"); !errors.Is(err, failure) {
		t.Fatalf("Invalidate = %v, want %v", err, failure)
	}
}

func TestInvalidateDoesNotFailOnACleanupError(t *testing.T) {
	failure := errors.New("delete failed")
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Del: func(string) error { return failure },
	}})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	events := &eventLog{}
	cache, err := cascache.New(cascache.Options[user]{
		Namespace: "test", Backend: b, Codec: codec.JSON[user]{}, Observer: events,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	fence, err := cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Set(ctx, "42", ada, fence); err != nil {
		t.Fatal(err)
	}

	if err := cache.Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate = %v, want nil for a cleanup failure", err)
	}
	if _, found := events.find(cascache.EventCleanupFailed); !found {
		t.Fatalf("events = %+v, want a cleanup_failed", events.all())
	}

	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get after a failed cleanup = %v, %v; want a miss", ok, err)
	}
}

func TestNamespacesDoNotCollide(t *testing.T) {
	store := memstore.New(memstore.Options{})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}

	newCache := func(namespace string) *cascache.Cache[user] {
		t.Helper()
		c, err := cascache.New(cascache.Options[user]{
			Namespace: namespace, Backend: b, Codec: codec.JSON[user]{},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	ctx := context.Background()

	a, bb := newCache("user"), newCache("user:u")

	fenceA, err := a.Snapshot(ctx, "u:1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Set(ctx, "u:1", ada, fenceA); err != nil {
		t.Fatal(err)
	}

	if _, ok, err := bb.Get(ctx, "1"); err != nil || ok {
		t.Fatalf("a value leaked across namespaces: %v, %v", ok, err)
	}

	if err := bb.Invalidate(ctx, "1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := a.Get(ctx, "u:1"); err != nil || !ok {
		t.Fatalf("an invalidation crossed namespaces: %v, %v", ok, err)
	}
}

func TestDisabledCacheIsAPassThrough(t *testing.T) {
	h := newHarness(t, func(o *cascache.Options[user]) { o.Disabled = true })
	ctx := context.Background()

	if h.cache.Enabled() {
		t.Fatal("Enabled reported true for a disabled cache")
	}

	fence, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	res, err := h.cache.Set(ctx, "42", ada, fence)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if res.Outcome != cascache.SetOutcomeDisabled {
		t.Fatalf("Outcome = %v, want disabled", res.Outcome)
	}
	if res, err := h.cache.SetWithTTL(ctx, "42", ada, fence, time.Minute); err != nil ||
		res.Outcome != cascache.SetOutcomeDisabled {
		t.Fatalf("SetWithTTL = %+v, %v; want disabled", res, err)
	}
	if _, ok, err := h.cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get = %v, %v; want a miss", ok, err)
	}
	if err := h.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if h.store.Len() != 0 {
		t.Fatal("a disabled cache wrote to the store")
	}
}

func TestStringers(t *testing.T) {
	if got := cascache.SetOutcomeConflict.String(); got != "conflict" {
		t.Fatalf("SetOutcome.String = %q", got)
	}
	if got := cascache.RejectStateMissing.String(); got != "state_missing" {
		t.Fatalf("RejectReason.String = %q", got)
	}
	if got := cascache.EventEntryRejected.String(); got != "entry_rejected" {
		t.Fatalf("EventType.String = %q", got)
	}
	if got := cascache.OpInvalidate.String(); got != "invalidate" {
		t.Fatalf("Op.String = %q", got)
	}
	if got := cascache.OpComputeTTL.String(); got != "compute_ttl" {
		t.Fatalf("Op.String = %q", got)
	}
	if got := cascache.SetOutcome(200).String(); got != "unknown" {
		t.Fatalf("unknown SetOutcome.String = %q", got)
	}
}

type record struct {
	Key     string `json:"key"`
	Version int64  `json:"version"`
}

func TestRetiresCopiesOnEveryReplica(t *testing.T) {
	for _, arrangement := range arrangements(t) {
		t.Run(arrangement.name, func(t *testing.T) {
			checkCrossReplicaRetirement(t, arrangement.newReplicas(t, 3))
		})
	}
}

func checkCrossReplicaRetirement(t *testing.T, replicas []*cascache.Cache[record]) {
	t.Helper()

	ctx := context.Background()
	const key = "shared"

	var version atomic.Int64
	version.Store(1)
	load := func(context.Context) (record, error) {
		return record{Key: key, Version: version.Load()}, nil
	}

	for i, cache := range replicas {
		if _, err := cache.Load(ctx, key, load); err != nil {
			t.Fatalf("replica %d Load: %v", i, err)
		}
		got, ok, err := cache.Get(ctx, key)
		if err != nil || !ok || got.Version != 1 {
			t.Fatalf("replica %d did not cache version 1: %+v, %v, %v", i, got, ok, err)
		}
	}

	version.Store(2)
	if err := replicas[0].Invalidate(ctx, key); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	for i, cache := range replicas {
		got, ok, err := cache.Get(ctx, key)
		if err != nil {
			t.Fatalf("replica %d Get: %v", i, err)
		}
		if ok {
			t.Fatalf("replica %d still serves version %d after an invalidation elsewhere",
				i, got.Version)
		}
	}

	for i, cache := range replicas {
		got, err := cache.Load(ctx, key, load)
		if err != nil || got.Version != 2 {
			t.Fatalf("replica %d reloaded %+v, %v; want version 2", i, got, err)
		}
	}
}

// Once an invalidation completes, later reads must not return an older source
// version. Race loads, reads, and invalidations across replicas.
func TestNeverServesAStaleValue(t *testing.T) {
	for _, arrangement := range arrangements(t) {
		t.Run(arrangement.name, func(t *testing.T) {
			checkNoStaleReads(t, arrangement.newReplicas(t, 2))
		})
	}
}

func newReplicaCache(t testing.TB, b backend.Backend) *cascache.Cache[record] {
	t.Helper()
	cache, err := cascache.New(cascache.Options[record]{
		Namespace:  "staleness",
		Backend:    b,
		Codec:      codec.JSON[record]{},
		DefaultTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	return cache
}

func checkNoStaleReads(t *testing.T, replicas []*cascache.Cache[record]) {
	t.Helper()

	const (
		keys    = 4
		loaders = 3
		readers = 3
		rounds  = 200
	)

	var (
		source      [keys]atomic.Int64
		invalidated [keys]atomic.Int64
	)

	ctx := context.Background()
	keyOf := func(i int) string { return string(rune('a' + i)) }

	var (
		wg     sync.WaitGroup
		failed atomic.Bool
		served atomic.Int64
	)
	fail := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			t.Errorf(format, args...)
		}
	}

	loadInto := func(cache *cascache.Cache[record], key string, i int) error {
		_, err := cache.Load(ctx, key, func(context.Context) (record, error) {
			return record{Key: key, Version: source[i].Load()}, nil
		})
		return err
	}

	// Warm the replicas so the readers exercise cached values.
	for i := range keys {
		for _, cache := range replicas {
			if err := loadInto(cache, keyOf(i), i); err != nil {
				t.Fatalf("warm-up Load: %v", err)
			}
		}
	}

	done := make(chan struct{})

	for i := range keys {
		key := keyOf(i)

		for n := range loaders {
			cache := replicas[n%len(replicas)]
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					if err := loadInto(cache, key, i); err != nil {
						fail("Load(%q): %v", key, err)
						return
					}
				}
			}()
		}

		for n := range readers {
			cache := replicas[n%len(replicas)]
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}

					// Only invalidations completed before the read are binding.
					floor := invalidated[i].Load()

					got, ok, err := cache.Get(ctx, key)
					if err != nil {
						fail("Get(%q): %v", key, err)
						return
					}
					if !ok {
						continue
					}
					served.Add(1)
					if got.Version < floor {
						fail("Get(%q) served version %d after version %d was invalidated",
							key, got.Version, floor)
						return
					}
				}
			}()
		}
	}

	var invalidators sync.WaitGroup
	for i := range keys {
		key := keyOf(i)
		invalidators.Add(1)
		go func() {
			defer invalidators.Done()
			for range rounds {
				version := source[i].Add(1)
				if err := replicas[0].Invalidate(ctx, key); err != nil {
					fail("Invalidate(%q): %v", key, err)
					return
				}
				for {
					current := invalidated[i].Load()
					if current >= version || invalidated[i].CompareAndSwap(current, version) {
						break
					}
				}
			}
		}()
	}

	invalidators.Wait()
	close(done)
	wg.Wait()

	if got := served.Load(); got == 0 {
		t.Fatal("no read ever returned a cached value; the invariant was never exercised")
	}
}

// Losing fence state must not make a retired value current again.
func TestARetiredValueCannotComeBack(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	h.fill(t, "42", ada)
	retired, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}

	if err := h.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}

	h.fences.forget(h.space("42"))

	res, err := h.cache.Set(ctx, "42", ada, retired)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != cascache.SetOutcomeConflict {
		t.Fatalf("a write under a retired fence = %v, want conflict", res.Outcome)
	}
	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("an invalidated value came back")
	}

	fresh, err := h.cache.Snapshot(ctx, "42")
	if err != nil {
		t.Fatal(err)
	}
	res, err = h.cache.Set(ctx, "42", ada, fresh)
	if err != nil || res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("a write under a fresh snapshot = %v, %v; want stored", res.Outcome, err)
	}
}

type arrangement struct {
	name        string
	newReplicas func(t testing.TB, n int) []*cascache.Cache[record]
}

func arrangements(t *testing.T) []arrangement {
	t.Helper()

	out := []arrangement{{
		name: "per-replica values and shared process fences",
		newReplicas: func(t testing.TB, n int) []*cascache.Cache[record] {
			fences := newFakeFences()

			caches := make([]*cascache.Cache[record], n)
			for i := range caches {
				b, err := backend.NewComposite(memstore.New(memstore.Options{}), fences)
				if err != nil {
					t.Fatal(err)
				}
				caches[i] = newReplicaCache(t, b)
			}
			return caches
		},
	}}

	addr := os.Getenv("CASCACHE_TEST_REDIS")
	if addr == "" {
		t.Log("set CASCACHE_TEST_REDIS to also check the Redis arrangements")
		return out
	}

	dial := func(t testing.TB) *goredis.Client {
		t.Helper()
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

	return append(out,
		arrangement{
			name: "shared redis values and fences",
			newReplicas: func(t testing.TB, n int) []*cascache.Cache[record] {
				client := dial(t)
				caches := make([]*cascache.Cache[record], n)
				for i := range caches {
					b, err := redisbackend.New(client, redisbackend.Options{
						InvalidationTTL: backend.NoExpiration,
					})
					if err != nil {
						t.Fatal(err)
					}
					caches[i] = newReplicaCache(t, b)
				}
				return caches
			},
		},
		arrangement{
			name: "per-replica values and shared redis fences",
			newReplicas: func(t testing.TB, n int) []*cascache.Cache[record] {
				client := dial(t)
				caches := make([]*cascache.Cache[record], n)
				for i := range caches {
					b, err := redisbackend.NewShared(
						memstore.New(memstore.Options{}),
						client,
						redisbackend.Options{InvalidationTTL: backend.NoExpiration},
					)
					if err != nil {
						t.Fatal(err)
					}
					caches[i] = newReplicaCache(t, b)
				}
				return caches
			},
		},
	)
}
