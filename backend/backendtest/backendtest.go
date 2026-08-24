// Package backendtest provides conformance tests for backend implementations.
package backendtest

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

// Factory returns a backend for one subtest.
type Factory func(testing.TB) backend.Backend

// Keep concurrent test processes separate on shared servers.
var runID = backend.NewFence().String()[:8]

// Key returns a storage-safe key for one subtest.
func Key(t testing.TB, name string) backend.Key {
	t.Helper()
	key, err := backend.NewKey("cascache-backendtest:" + runID + ":" + t.Name() + ":" + name)
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	return key
}

// TestBackend runs the full suite against one backend.
func TestBackend(t *testing.T, newBackend Factory) {
	t.Helper()

	t.Run("read-of-an-unknown-key-is-a-miss", func(t *testing.T) {
		b := newBackend(t)
		read, err := b.Read(ctx(t), Key(t, "absent"))
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if read.Found {
			t.Fatalf("Read of an unknown key = %+v, want a miss", read)
		}
	})

	t.Run("lifecycle", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "lifecycle")

		first := backend.NewFence()
		got, err := b.Ensure(ctx(t), key, first)
		if err != nil || !got.Equal(first) {
			t.Fatalf("Ensure on an empty key = %v, %v; want %v", got, err, first)
		}

		got, err = b.Ensure(ctx(t), key, backend.NewFence())
		if err != nil || !got.Equal(first) {
			t.Fatalf("Ensure on an existing key = %v, %v; want %v", got, err, first)
		}

		store(t, b, key, first, "one")

		read, err := b.Read(ctx(t), key)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if !read.Found || !read.FenceFound || !read.Fence.Equal(first) || !bytes.Equal(read.Value, []byte("one")) {
			t.Fatalf("Read = %+v, want %q under %v", read, "one", first)
		}

		res, err := b.CompareAndStore(ctx(t), request(key, backend.NewFence(), "stale"))
		if err != nil || res.Status != backend.StoreConflict {
			t.Fatalf("CompareAndStore with an unknown fence = %+v, %v; want conflict", res, err)
		}

		next := backend.NewFence()
		inv, err := b.Invalidate(ctx(t), key, next)
		if err != nil || inv.CleanupErr != nil {
			t.Fatalf("Invalidate = %+v, %v", inv, err)
		}

		read, err = b.Read(ctx(t), key)
		if err != nil {
			t.Fatalf("Read after invalidate: %v", err)
		}
		if read.Found {
			t.Fatalf("Read after invalidate = %+v, want a miss", read)
		}

		res, err = b.CompareAndStore(ctx(t), request(key, first, "retired"))
		if err != nil || res.Status != backend.StoreConflict {
			t.Fatalf("post-invalidate write with the old fence = %+v, %v; want conflict", res, err)
		}

		current, err := b.Ensure(ctx(t), key, backend.NewFence())
		if err != nil || !current.Equal(next) {
			t.Fatalf("fence after invalidate = %v, %v; want %v", current, err, next)
		}
	})

	t.Run("store-without-a-fence-conflicts", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "no-fence")

		res, err := b.CompareAndStore(ctx(t), request(key, backend.NewFence(), "value"))
		if err != nil || res.Status != backend.StoreConflict {
			t.Fatalf("CompareAndStore on a fenceless key = %+v, %v; want conflict", res, err)
		}
		read, err := b.Read(ctx(t), key)
		if err != nil || read.Found {
			t.Fatalf("Read = %+v, %v; want a miss", read, err)
		}
	})

	t.Run("discard-is-conditional", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "discard")
		fence := ensure(t, b, key)

		store(t, b, key, fence, "old")
		store(t, b, key, fence, "new")

		removed, err := b.Discard(ctx(t), key, []byte("old"))
		if err != nil || removed {
			t.Fatalf("Discard of superseded bytes = %v, %v; want false", removed, err)
		}
		read, err := b.Read(ctx(t), key)
		if err != nil || !bytes.Equal(read.Value, []byte("new")) {
			t.Fatalf("Discard removed the current value: %+v, %v", read, err)
		}

		removed, err = b.Discard(ctx(t), key, []byte("new"))
		if err != nil || !removed {
			t.Fatalf("Discard of the current bytes = %v, %v; want true", removed, err)
		}
		if read, err = b.Read(ctx(t), key); err != nil || read.Found {
			t.Fatalf("Read after Discard = %+v, %v; want a miss", read, err)
		}

		removed, err = b.Discard(ctx(t), key, []byte("new"))
		if err != nil || removed {
			t.Fatalf("Discard of an absent value = %v, %v; want false", removed, err)
		}
	})

	t.Run("invalidating-an-unknown-key-installs-a-fence", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "invalidate-unknown")

		next := backend.NewFence()
		if _, err := b.Invalidate(ctx(t), key, next); err != nil {
			t.Fatalf("Invalidate: %v", err)
		}
		got, err := b.Ensure(ctx(t), key, backend.NewFence())
		if err != nil || !got.Equal(next) {
			t.Fatalf("fence = %v, %v; want %v", got, err, next)
		}
	})

	t.Run("rejects-invalid-arguments", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "arguments")

		if _, err := b.Read(ctx(t), backend.Key{}); !errors.Is(err, backend.ErrInvalidKey) {
			t.Fatalf("Read(zero key) = %v, want ErrInvalidKey", err)
		}
		if _, err := b.Ensure(ctx(t), backend.Key{}, backend.NewFence()); !errors.Is(err, backend.ErrInvalidKey) {
			t.Fatalf("Ensure(zero key) = %v, want ErrInvalidKey", err)
		}
		if _, err := b.Ensure(ctx(t), key, backend.Fence{}); !errors.Is(err, backend.ErrInvalidFence) {
			t.Fatalf("Ensure(zero fence) = %v, want ErrInvalidFence", err)
		}
		if _, err := b.CompareAndStore(
			ctx(t),
			request(key, backend.Fence{}, "v"),
		); !errors.Is(
			err,
			backend.ErrInvalidFence,
		) {
			t.Fatalf("CompareAndStore(zero fence) = %v, want ErrInvalidFence", err)
		}
		if _, err := b.Invalidate(ctx(t), key, backend.Fence{}); !errors.Is(err, backend.ErrInvalidFence) {
			t.Fatalf("Invalidate(zero fence) = %v, want ErrInvalidFence", err)
		}
		if _, err := b.Discard(ctx(t), backend.Key{}, []byte("v")); !errors.Is(err, backend.ErrInvalidKey) {
			t.Fatalf("Discard(zero key) = %v, want ErrInvalidKey", err)
		}
	})

	t.Run("distinct-identities-never-collide", func(t *testing.T) {
		b := newBackend(t)
		names := collisionNames()

		keys := make([]backend.Key, len(names))
		fences := make([]backend.Fence, len(names))
		for i, name := range names {
			keys[i] = Key(t, name)
			fences[i] = ensure(t, b, keys[i])
			store(t, b, keys[i], fences[i], name)
		}
		for i, name := range names {
			read, err := b.Read(ctx(t), keys[i])
			if err != nil {
				t.Fatalf("Read(%q): %v", name, err)
			}
			if !read.Found || !read.FenceFound {
				t.Fatalf("Read(%q) = %+v; the entry was displaced by another identity", name, read)
			}
			if !bytes.Equal(read.Value, []byte(name)) || !read.Fence.Equal(fences[i]) {
				t.Fatalf("Read(%q) returned %q under %v; two identities share storage",
					name, read.Value, read.Fence)
			}
		}
	})

	t.Run("concurrent-ensure-agrees-on-one-fence", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "concurrent-ensure")

		const calls = 32
		got := make([]backend.Fence, calls)
		errs := make([]error, calls)

		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range calls {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				got[i], errs[i] = b.Ensure(ctx(t), key, backend.NewFence())
			}()
		}
		close(start)
		wg.Wait()

		if !got[0].Valid() {
			t.Fatalf("Ensure returned an invalid fence: %v", got[0])
		}
		for i := range got {
			if errs[i] != nil || !got[i].Equal(got[0]) {
				t.Fatalf("Ensure[%d] = %v, %v; want %v", i, got[i], errs[i], got[0])
			}
		}
	})

	t.Run("concurrent-invalidate-holds", func(t *testing.T) {
		b := newBackend(t)
		key := Key(t, "concurrent-invalidate")
		first := ensure(t, b, key)

		const writers = 32
		statuses := make([]backend.StoreStatus, writers)
		errs := make([]error, writers)
		next := backend.NewFence()

		var (
			inv    backend.InvalidateResult
			invErr error
		)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res, err := b.CompareAndStore(ctx(t), request(key, first, "racing"))
				statuses[i], errs[i] = res.Status, err
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			inv, invErr = b.Invalidate(ctx(t), key, next)
		}()
		close(start)
		wg.Wait()

		if invErr != nil || inv.CleanupErr != nil {
			t.Fatalf("Invalidate = %+v, %v", inv, invErr)
		}
		for i, status := range statuses {
			if errs[i] != nil {
				t.Fatalf("CompareAndStore[%d]: %v", i, errs[i])
			}
			switch status {
			case backend.StoreStored, backend.StoreConflict, backend.StoreRejected:
			default:
				t.Fatalf("CompareAndStore[%d] status = %v", i, status)
			}
		}

		current, err := b.Ensure(ctx(t), key, backend.NewFence())
		if err != nil || !current.Equal(next) {
			t.Fatalf("fence after the race = %v, %v; want %v", current, err, next)
		}
		res, err := b.CompareAndStore(ctx(t), request(key, first, "retired"))
		if err != nil || res.Status != backend.StoreConflict {
			t.Fatalf("write with the retired fence = %+v, %v; want conflict", res, err)
		}
		read, err := b.Read(ctx(t), key)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if read.Found && read.FenceFound && read.Fence.Equal(first) {
			t.Fatalf("a retired fence is current again: %+v", read)
		}
	})

	t.Run("never-reuses-a-fence", func(t *testing.T) {
		testFenceNonReuse(t, newBackend(t))
	})
}

// Race reads and invalidations. If a fence is observed before and after a
// different non-overlapping fence, it was reused.
func testFenceNonReuse(t *testing.T, b backend.Backend) {
	const (
		keys     = 4
		watchers = 4
		rounds   = 40
	)

	var clock atomic.Int64

	type observation struct {
		start, end int64
		fence      backend.Fence
	}

	var (
		mu   sync.Mutex
		seen = make(map[string][]observation, keys)
	)
	record := func(key backend.Key, start, end int64, fence backend.Fence) {
		if !fence.Valid() {
			return
		}
		mu.Lock()
		seen[key.ID()] = append(seen[key.ID()], observation{start: start, end: end, fence: fence})
		mu.Unlock()
	}

	all := make([]backend.Key, keys)
	for i := range all {
		all[i] = Key(t, "non-reuse-"+string(rune('a'+i)))
	}

	var wg sync.WaitGroup
	for _, key := range all {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				if _, err := b.Invalidate(context.Background(), key, backend.NewFence()); err != nil {
					return
				}
			}
		}()

		for range watchers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range rounds {
					start := clock.Add(1)
					fence, err := b.Ensure(context.Background(), key, backend.NewFence())
					end := clock.Add(1)
					if err != nil {
						return
					}
					record(key, start, end, fence)

					start = clock.Add(1)
					read, err := b.Read(context.Background(), key)
					end = clock.Add(1)
					if err != nil {
						return
					}
					if read.FenceFound {
						record(key, start, end, read.Fence)
					}
				}
			}()
		}
	}
	wg.Wait()

	for id, obs := range seen {
		type span struct{ firstEnd, lastStart, minStart, maxEnd int64 }
		spans := make(map[backend.Fence]*span, 8)
		for _, o := range obs {
			s, ok := spans[o.fence]
			if !ok {
				spans[o.fence] = &span{firstEnd: o.end, lastStart: o.start, minStart: o.start, maxEnd: o.end}
				continue
			}
			s.firstEnd = min(s.firstEnd, o.end)
			s.lastStart = max(s.lastStart, o.start)
			s.minStart = min(s.minStart, o.start)
			s.maxEnd = max(s.maxEnd, o.end)
		}

		for f, a := range spans {
			for g, other := range spans {
				if f == g {
					continue
				}
				if a.firstEnd < other.minStart && a.lastStart > other.maxEnd {
					t.Fatalf("key %s: fence %v was current, replaced by %v, then current again", id, f, g)
				}
			}
		}
	}
}

func ctx(t testing.TB) context.Context {
	t.Helper()
	return context.Background()
}

func request(key backend.Key, fence backend.Fence, value string) backend.StoreRequest {
	return backend.StoreRequest{
		Key:      key,
		Expected: fence,
		Value:    []byte(value),
		Cost:     1,
		TTL:      time.Minute,
	}
}

func ensure(t testing.TB, b backend.Backend, key backend.Key) backend.Fence {
	t.Helper()
	fence, err := b.Ensure(ctx(t), key, backend.NewFence())
	if err != nil {
		t.Fatalf("Ensure(%v): %v", key, err)
	}
	return fence
}

func store(t testing.TB, b backend.Backend, key backend.Key, fence backend.Fence, value string) {
	t.Helper()
	res, err := b.CompareAndStore(ctx(t), request(key, fence, value))
	if err != nil || res.Status != backend.StoreStored {
		t.Fatalf("CompareAndStore(%v, %q) = %+v, %v; want stored", key, value, res, err)
	}
}

func collisionNames() []string {
	long := strings.Repeat("c", 200)
	names := []string{
		"collide", "collide:x", "collide:x:y", "collide-x",
		"a:b", "a", "b",
		long + "a", long + "b",
	}
	slices.Sort(names)
	return names
}
