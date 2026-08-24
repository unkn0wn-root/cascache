package backend

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct{ nanos atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.nanos.Store(time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *clock) Now() time.Time      { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *clock) Add(d time.Duration) { c.nanos.Add(int64(d)) }

func newTestFences(t testing.TB, retention time.Duration) (*memoryFenceStore, *clock) {
	t.Helper()
	fences, err := newMemoryFenceStore(LocalOptions{InvalidationTTL: retention, CleanupInterval: -1})
	if err != nil {
		t.Fatalf("newMemoryFenceStore: %v", err)
	}
	c := newClock()
	fences.now = c.Now
	t.Cleanup(func() { _ = fences.close() })
	return fences, c
}

func testKey(t testing.TB, name string) Key {
	t.Helper()
	key, err := NewKey("memory-test:" + t.Name() + ":" + name)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return key
}

func TestMemoryFenceStoreDefaults(t *testing.T) {
	fences, err := newMemoryFenceStore(LocalOptions{})
	if err != nil {
		t.Fatalf("newMemoryFenceStore: %v", err)
	}
	t.Cleanup(func() { _ = fences.close() })

	if got := fences.Lifetime(); got != DefaultInvalidationTTL {
		t.Fatalf("Lifetime = %v, want %v", got, DefaultInvalidationTTL)
	}
}

func TestMemoryFenceStoreNoExpiration(t *testing.T) {
	fences, c := newTestFences(t, NoExpiration)
	if got := fences.Lifetime(); got != 0 {
		t.Fatalf("Lifetime = %v, want 0 for NoExpiration", got)
	}

	key := testKey(t, "kept")
	want, err := fences.Ensure(context.Background(), key, NewFence())
	if err != nil {
		t.Fatal(err)
	}

	c.Add(1000 * time.Hour)
	got, found, err := fences.Read(context.Background(), key)
	if err != nil || !found || !got.Equal(want) {
		t.Fatalf("Read after a long wait = %v, %v, %v; want %v", got, found, err, want)
	}
}

func TestMemoryFenceStoreRejectsInvalidRetention(t *testing.T) {
	if _, err := newMemoryFenceStore(LocalOptions{InvalidationTTL: -5 * time.Second}); err == nil {
		t.Fatal("newMemoryFenceStore accepted a negative retention that is not NoExpiration")
	}
}

func TestMemoryFenceStoreExpiry(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)
	key := testKey(t, "expiring")

	first, err := fences.Ensure(context.Background(), key, NewFence())
	if err != nil {
		t.Fatal(err)
	}

	c.Add(retention - time.Minute)
	if got, found, err := fences.Read(context.Background(), key); err != nil || !found || !got.Equal(first) {
		t.Fatalf("Read inside retention = %v, %v, %v", got, found, err)
	}

	c.Add(2 * time.Minute)
	if _, found, err := fences.Read(context.Background(), key); err != nil || found {
		t.Fatalf("Read past retention = %v, %v; want not found", found, err)
	}
}

func TestMemoryFenceStoreEnsureDoesNotRevive(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)
	key := testKey(t, "revive")

	first, err := fences.Ensure(context.Background(), key, NewFence())
	if err != nil {
		t.Fatal(err)
	}

	c.Add(retention + time.Minute)

	candidate := NewFence()
	got, err := fences.Ensure(context.Background(), key, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if got.Equal(first) {
		t.Fatal("Ensure revived an expired fence")
	}
	if !got.Equal(candidate) {
		t.Fatalf("Ensure = %v, want the candidate %v", got, candidate)
	}
}

func TestMemoryFenceStoreRetainNeverInstalls(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)
	key := testKey(t, "retain")

	observed, err := fences.Ensure(context.Background(), key, NewFence())
	if err != nil {
		t.Fatal(err)
	}

	current, err := fences.Retain(context.Background(), key, observed)
	if err != nil || !current {
		t.Fatalf("Retain of the current fence = %v, %v; want true", current, err)
	}

	c.Add(retention + time.Minute)

	current, err = fences.Retain(context.Background(), key, observed)
	if err != nil || current {
		t.Fatalf("Retain of an expired fence = %v, %v; want false", current, err)
	}
	if _, found, err := fences.Read(context.Background(), key); err != nil || found {
		t.Fatalf("Retain installed a fence: found = %v, %v", found, err)
	}

	if _, err := fences.Ensure(context.Background(), key, NewFence()); err != nil {
		t.Fatal(err)
	}
	if current, err := fences.Retain(context.Background(), key, observed); err != nil || current {
		t.Fatalf("Retain of a retired fence = %v, %v; want false", current, err)
	}
}

func TestMemoryFenceStoreRetentionRefresh(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)
	key := testKey(t, "refresh")

	first, err := fences.Ensure(context.Background(), key, NewFence())
	if err != nil {
		t.Fatal(err)
	}

	for range 3 {
		c.Add(retention / 2)
		if _, err := fences.Ensure(context.Background(), key, NewFence()); err != nil {
			t.Fatal(err)
		}
	}
	if got, found, err := fences.Read(context.Background(), key); err != nil || !found || !got.Equal(first) {
		t.Fatalf("Ensure did not refresh retention: %v, %v, %v", got, found, err)
	}

	for range 3 {
		c.Add(retention / 2)
		if _, _, err := fences.Read(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	if _, found, err := fences.Read(context.Background(), key); err != nil || found {
		t.Fatalf("reads refreshed retention: found = %v, %v", found, err)
	}
}

func TestMemoryFenceStoreReplaceRefreshes(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)
	key := testKey(t, "replace")

	if _, err := fences.Ensure(context.Background(), key, NewFence()); err != nil {
		t.Fatal(err)
	}
	c.Add(retention - time.Minute)

	next := NewFence()
	if err := fences.Replace(context.Background(), key, next); err != nil {
		t.Fatal(err)
	}
	c.Add(retention - time.Minute)

	got, found, err := fences.Read(context.Background(), key)
	if err != nil || !found || !got.Equal(next) {
		t.Fatalf("Read after Replace = %v, %v, %v; want %v", got, found, err, next)
	}
}

func TestMemoryFenceStoreCleanup(t *testing.T) {
	const retention = time.Hour
	fences, c := newTestFences(t, retention)

	for i := range 100 {
		key := testKey(t, string(rune('a'+i%26))+string(rune('a'+i/26)))
		if _, err := fences.Ensure(context.Background(), key, NewFence()); err != nil {
			t.Fatal(err)
		}
	}
	if got := fences.len(); got != 100 {
		t.Fatalf("Len = %d, want 100", got)
	}

	fences.cleanupExpired()
	if got := fences.len(); got != 100 {
		t.Fatalf("Cleanup removed live fences: Len = %d, want 100", got)
	}

	c.Add(retention + time.Minute)
	fences.cleanupExpired()
	if got := fences.len(); got != 0 {
		t.Fatalf("Len after cleanup = %d, want 0", got)
	}
}

func TestMemoryFenceStoreRejectsInvalidArguments(t *testing.T) {
	fences, _ := newTestFences(t, time.Hour)
	key := testKey(t, "arguments")

	if _, err := fences.Ensure(context.Background(), Key{}, NewFence()); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Ensure(zero key) = %v, want ErrInvalidKey", err)
	}
	if _, err := fences.Ensure(context.Background(), key, Fence{}); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("Ensure(zero fence) = %v, want ErrInvalidFence", err)
	}
	if _, _, err := fences.Read(context.Background(), Key{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Read(zero key) = %v, want ErrInvalidKey", err)
	}
	if _, err := fences.Retain(context.Background(), key, Fence{}); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("Retain(zero fence) = %v, want ErrInvalidFence", err)
	}
	if err := fences.Replace(context.Background(), key, Fence{}); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("Replace(zero fence) = %v, want ErrInvalidFence", err)
	}
}

func TestMemoryFenceStoreCloseIsIdempotent(t *testing.T) {
	fences, err := newMemoryFenceStore(LocalOptions{InvalidationTTL: time.Hour, CleanupInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := fences.close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	var nilStore *memoryFenceStore
	if err := nilStore.close(); err != nil {
		t.Fatalf("Close on a nil store: %v", err)
	}
}
