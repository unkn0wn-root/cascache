package genstore

import (
	"context"
	"testing"
	"time"
)

func TestLocalSnapshotManyIncludesAllAndZeroForMissing(t *testing.T) {
	ctx := context.Background()
	s := NewLocalGenStore(0, 0)
	t.Cleanup(func() { _ = s.Close(ctx) })

	keys := []CacheKey{NewCacheKey("a"), NewCacheKey("b"), NewCacheKey("c")}
	// bump b twice -> gen=2
	if _, err := s.Bump(ctx, NewCacheKey("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Bump(ctx, NewCacheKey("b")); err != nil {
		t.Fatal(err)
	}

	got, err := s.SnapshotMany(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}

	if got[NewCacheKey("a")] != 0 || got[NewCacheKey("b")] != 2 || got[NewCacheKey("c")] != 0 {
		t.Fatalf("got=%v want a=0,b=2,c=0", got)
	}
}

func TestLocalSnapshotManyDoesNotMutateInput(t *testing.T) {
	ctx := context.Background()
	s := NewLocalGenStore(0, 0)
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

func TestLocalCleanupPrunesOld(t *testing.T) {
	ctx := context.Background()
	s := NewLocalGenStore(0, time.Second) // retention=1s
	t.Cleanup(func() { _ = s.Close(ctx) })

	if _, err := s.Bump(ctx, NewCacheKey("old")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond)
	s.Cleanup(time.Second)

	g, err := s.Snapshot(ctx, NewCacheKey("old"))
	if err != nil {
		t.Fatal(err)
	}
	if g != 0 {
		t.Fatalf("expected pruned -> 0, got %d", g)
	}
}
