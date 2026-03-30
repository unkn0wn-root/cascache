package version

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"
)

type errorReader struct {
	err error
}

func (r errorReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestLocalSnapshotManyIncludesAllAndZeroForMissing(t *testing.T) {
	ctx := context.Background()
	s := NewLocal()
	t.Cleanup(func() { _ = s.Close(ctx) })

	keys := []CacheKey{NewCacheKey("a"), NewCacheKey("b"), NewCacheKey("c")}
	first, err := s.Advance(ctx, NewCacheKey("b"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Advance(ctx, NewCacheKey("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !first.Exists || !second.Exists {
		t.Fatalf("expected live snapshots, first=%+v second=%+v", first, second)
	}
	if first.Fence.Equal(second.Fence) {
		t.Fatalf("expected advance to replace fence, first=%s second=%s", first.Fence, second.Fence)
	}

	got, err := s.SnapshotMany(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	if got[NewCacheKey("a")].Exists || !got[NewCacheKey("b")].Exists ||
		!got[NewCacheKey("b")].Fence.Equal(second.Fence) ||
		got[NewCacheKey("c")].Exists {
		t.Fatalf("got=%v want a=missing,b=current,c=missing", got)
	}
}

func TestLocalSnapshotManyDoesNotMutateInput(t *testing.T) {
	ctx := context.Background()
	s := NewLocal()
	t.Cleanup(func() { _ = s.Close(ctx) })

	in := []CacheKey{NewCacheKey("x"), NewCacheKey("y")}
	cp := append([]CacheKey(nil), in...)
	if _, err := s.SnapshotMany(ctx, in); err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if in[i] != cp[i] {
			t.Fatalf("input mutated at %d: %q -> %q", i, cp[i], in[i])
		}
	}
}

func TestNewLocalDisablesAutomaticCleanup(t *testing.T) {
	s := NewLocal()
	if s.stopCh != nil {
		t.Fatalf("strict local version store should not start a cleanup goroutine")
	}
	if s.retention != 0 {
		t.Fatalf(
			"strict local version store should not retain cleanup settings, got %v",
			s.retention,
		)
	}
}

func TestLocalCreateIfMissingExistingDoesNotNeedNewFence(t *testing.T) {
	ctx := context.Background()
	s := NewLocal()
	t.Cleanup(func() { _ = s.Close(ctx) })

	initial, created, err := s.CreateIfMissing(ctx, NewCacheKey("k"))
	if err != nil {
		t.Fatalf("initial CreateIfMissing error: %v", err)
	}
	if !created || !initial.Exists {
		t.Fatalf("expected initial creation, got snap=%+v created=%v", initial, created)
	}

	orig := rand.Reader
	t.Cleanup(func() {
		rand.Reader = orig
	})
	rand.Reader = errorReader{err: errors.New("rng unavailable")}

	got, createdAgain, err := s.CreateIfMissing(ctx, NewCacheKey("k"))
	if err != nil {
		t.Fatalf("existing CreateIfMissing should not fail when rng is broken: %v", err)
	}
	if createdAgain {
		t.Fatalf("expected existing key to report created=false")
	}
	if !got.Exists || !got.Fence.Equal(initial.Fence) {
		t.Fatalf("unexpected existing snapshot: got=%+v want fence=%s", got, initial.Fence)
	}
}

func TestLocalCleanupPrunesOld(t *testing.T) {
	ctx := context.Background()
	s := NewLocalWithCleanup(0, time.Second) // retention=1s
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Advance(ctx, NewCacheKey("old")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	s.Cleanup(time.Second)

	g, err := s.Snapshot(ctx, NewCacheKey("old"))
	if err != nil {
		t.Fatal(err)
	}
	if g.Exists {
		t.Fatalf("expected pruned -> missing, got %+v", g)
	}
}
