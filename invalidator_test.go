package cascache_test

import (
	"context"
	"errors"
	"testing"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

func TestNewInvalidatorValidatesOptions(t *testing.T) {
	store := memstore.New(memstore.Options{})
	b, err := backend.NewComposite(store, newFakeFences())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := cascache.NewInvalidator(
		cascache.InvalidatorOptions{Backend: b},
	); !errors.Is(
		err,
		cascache.ErrNoNamespace,
	) {
		t.Fatalf("NewInvalidator without a namespace = %v, want ErrNoNamespace", err)
	}
	if _, err := cascache.NewInvalidator(
		cascache.InvalidatorOptions{Namespace: "n"},
	); !errors.Is(
		err,
		cascache.ErrNoBackend,
	) {
		t.Fatalf("NewInvalidator without a backend = %v, want ErrNoBackend", err)
	}
}

func TestStandaloneInvalidatorRetiresACacheEntry(t *testing.T) {
	const namespace = "test"
	ctx := context.Background()

	store := memstore.New(memstore.Options{})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}

	cache, err := cascache.New(cascache.Options[user]{
		Namespace: namespace, Backend: b, Codec: codec.JSON[user]{},
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
	if _, ok, _ := cache.Get(ctx, "42"); !ok {
		t.Fatal("the value was not cached")
	}

	inv, err := cascache.NewInvalidator(cascache.InvalidatorOptions{
		Namespace: namespace,
		Backend:   b,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := inv.Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	if _, ok, err := cache.Get(ctx, "42"); ok || err != nil {
		t.Fatalf("Get after invalidation = %v, %v; want a miss", ok, err)
	}
}

func TestInvalidatorNamespaceMustMatch(t *testing.T) {
	ctx := context.Background()

	store := memstore.New(memstore.Options{})
	fences := newFakeFences()
	b, err := backend.NewComposite(store, fences)
	if err != nil {
		t.Fatal(err)
	}

	cache, err := cascache.New(cascache.Options[user]{
		Namespace: "right", Backend: b, Codec: codec.JSON[user]{},
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

	wrong, err := cascache.NewInvalidator(cascache.InvalidatorOptions{
		Namespace: "wrong", Backend: b,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := cache.Get(ctx, "42"); !ok {
		t.Fatal("an invalidation in another namespace retired this entry")
	}
}

func TestCacheInvalidatorSharesTheCacheNamespace(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	h.fill(t, "42", ada)

	if err := h.cache.Invalidator().Invalidate(ctx, "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("the handle from Cache.Invalidator did not retire the entry")
	}
}

func TestDisabledInvalidatorIsANoOp(t *testing.T) {
	store := memstore.New(memstore.Options{})
	b, err := backend.NewComposite(store, newFakeFences())
	if err != nil {
		t.Fatal(err)
	}

	inv, err := cascache.NewInvalidator(cascache.InvalidatorOptions{
		Namespace: "test", Backend: b, Disabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if inv.Enabled() {
		t.Fatal("Enabled reported true for a disabled invalidator")
	}
	if err := inv.Invalidate(context.Background(), "42"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}
	if store.Len() != 0 {
		t.Fatal("a disabled invalidator touched the store")
	}
}
