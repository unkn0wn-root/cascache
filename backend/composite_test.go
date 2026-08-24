package backend_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/backend/backendtest"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

type testFenceStore struct {
	mu       sync.Mutex
	fences   map[string]backend.Fence
	lifetime time.Duration
}

func newTestFenceStore(lifetime time.Duration) *testFenceStore {
	if lifetime == backend.NoExpiration {
		lifetime = 0
	}
	return &testFenceStore{fences: make(map[string]backend.Fence), lifetime: lifetime}
}

func (s *testFenceStore) Lifetime() time.Duration { return s.lifetime }

func (s *testFenceStore) Ensure(
	_ context.Context,
	key backend.Key,
	candidate backend.Fence,
) (backend.Fence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.fences[key.ID()]; ok {
		return current, nil
	}
	s.fences[key.ID()] = candidate
	return candidate, nil
}

func (s *testFenceStore) Read(
	_ context.Context,
	key backend.Key,
) (backend.Fence, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fence, found := s.fences[key.ID()]
	return fence, found, nil
}

func (s *testFenceStore) Retain(
	_ context.Context,
	key backend.Key,
	expected backend.Fence,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fence, found := s.fences[key.ID()]
	return found && fence.Equal(expected), nil
}

func (s *testFenceStore) Replace(
	_ context.Context,
	key backend.Key,
	next backend.Fence,
) error {
	s.mu.Lock()
	s.fences[key.ID()] = next
	s.mu.Unlock()
	return nil
}

func newComposite(t testing.TB, retention time.Duration) *backend.Composite {
	t.Helper()

	b, err := backend.NewComposite(
		memstore.New(memstore.Options{}),
		newTestFenceStore(retention),
	)
	if err != nil {
		t.Fatalf("NewComposite: %v", err)
	}
	return b
}

func TestCompositeConformance(t *testing.T) {
	backendtest.TestBackend(t, func(t testing.TB) backend.Backend {
		return newComposite(t, backend.NoExpiration)
	})
}

func TestNewCompositeRejectsNilStores(t *testing.T) {
	fences := newTestFenceStore(0)

	if _, err := backend.NewComposite(nil, fences); !errors.Is(err, backend.ErrNilStore) {
		t.Fatalf("NewComposite(nil store) = %v, want ErrNilStore", err)
	}
	if _, err := backend.NewComposite(
		memstore.New(memstore.Options{}),
		nil,
	); !errors.Is(
		err,
		backend.ErrNilFenceStore,
	) {
		t.Fatalf("NewComposite(nil fences) = %v, want ErrNilFenceStore", err)
	}

	var typedNil *testFenceStore
	if _, err := backend.NewComposite(
		memstore.New(memstore.Options{}),
		typedNil,
	); !errors.Is(
		err,
		backend.ErrNilFenceStore,
	) {
		t.Fatalf("NewComposite(typed nil fences) = %v, want ErrNilFenceStore", err)
	}
}

func TestCompositeClampsTTLToFenceLifetime(t *testing.T) {
	const retention = time.Hour
	b := newComposite(t, retention)
	key := backendtest.Key(t, "clamp")

	fence, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"shorter than the fence", time.Minute, time.Minute},
		{"longer than the fence", 48 * time.Hour, retention},
		{"no expiry", 0, retention},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := b.CompareAndStore(context.Background(), backend.StoreRequest{
				Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: tc.ttl,
			})
			if err != nil || res.Status != backend.StoreStored {
				t.Fatalf("CompareAndStore = %+v, %v", res, err)
			}
			if res.EffectiveTTL != tc.want {
				t.Fatalf("EffectiveTTL = %v, want %v", res.EffectiveTTL, tc.want)
			}
		})
	}
}

func TestCompositeUnclampedWithoutFenceExpiry(t *testing.T) {
	b := newComposite(t, backend.NoExpiration)
	key := backendtest.Key(t, "unclamped")

	fence, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.CompareAndStore(context.Background(), backend.StoreRequest{
		Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: 48 * time.Hour,
	})
	if err != nil || res.Status != backend.StoreStored {
		t.Fatalf("CompareAndStore = %+v, %v", res, err)
	}
	if res.EffectiveTTL != 48*time.Hour {
		t.Fatalf("EffectiveTTL = %v, want the requested 48h", res.EffectiveTTL)
	}
}

func TestCompositeReportsAMissingFence(t *testing.T) {
	store := memstore.New(memstore.Options{})
	fences := newTestFenceStore(0)

	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	key := backendtest.Key(t, "orphan")

	if _, err := store.Set(context.Background(), backend.ValueKey(key), []byte("orphaned"), 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	read, err := b.Read(context.Background(), key)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !read.Found {
		t.Fatal("Read did not find the value")
	}
	if read.FenceFound {
		t.Fatal("Read reported a fence that does not exist")
	}
}

func TestCompositePropagatesStoreFailures(t *testing.T) {
	failure := errors.New("store is down")
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Set: func(string, []byte) (bool, error) { return false, failure },
	}})
	fences := newTestFenceStore(0)

	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	key := backendtest.Key(t, "failing")

	fence, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.CompareAndStore(context.Background(), backend.StoreRequest{
		Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: time.Minute,
	}); !errors.Is(err, failure) {
		t.Fatalf("CompareAndStore error = %v, want %v", err, failure)
	}
}

func TestCompositeReportsStoreRejection(t *testing.T) {
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Set: func(string, []byte) (bool, error) { return false, nil },
	}})
	fences := newTestFenceStore(0)

	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	key := backendtest.Key(t, "rejecting")

	fence, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.CompareAndStore(context.Background(), backend.StoreRequest{
		Key: key, Expected: fence, Value: []byte("v"), Cost: 1, TTL: time.Minute,
	})
	if err != nil || res.Status != backend.StoreRejected {
		t.Fatalf("CompareAndStore = %+v, %v; want rejected", res, err)
	}
}

func TestCompositeInvalidateReportsCleanupFailure(t *testing.T) {
	failure := errors.New("delete failed")
	store := memstore.New(memstore.Options{Hook: memstore.Hook{
		Del: func(string) error { return failure },
	}})
	fences := newTestFenceStore(0)

	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}
	key := backendtest.Key(t, "cleanup")

	before, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil {
		t.Fatal(err)
	}

	next := backend.NewFence()
	res, err := b.Invalidate(context.Background(), key, next)
	if err != nil {
		t.Fatalf("Invalidate returned an error for a delete failure: %v", err)
	}
	if !errors.Is(res.CleanupErr, failure) {
		t.Fatalf("CleanupErr = %v, want %v", res.CleanupErr, failure)
	}

	current, err := b.Ensure(context.Background(), key, backend.NewFence())
	if err != nil || !current.Equal(next) || current.Equal(before) {
		t.Fatalf("fence after a failed cleanup = %v, %v; want %v", current, err, next)
	}
}
