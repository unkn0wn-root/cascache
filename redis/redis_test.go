package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/codec"
	coregen "github.com/unkn0wn-root/cascache/genstore"
)

type fakeClient struct {
	goredis.UniversalClient
}

type closableClient struct {
	goredis.UniversalClient
	closeCalls int
}

func (c *closableClient) Close() error {
	c.closeCalls++
	return nil
}

type fakeUniversalClient struct {
	goredis.UniversalClient
	closeCalls atomic.Int32
}

func (c *fakeUniversalClient) Close() error {
	c.closeCalls.Add(1)
	return nil
}

func TestNewKeyMutatorNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewKeyMutator(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewKeyMutator error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewProviderNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewProvider(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewProvider error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewGenStoreNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewGenStore(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewGenStore error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewNilClient(t *testing.T) {
	t.Parallel()

	if _, err := New(Options[struct{}]{
		Namespace: "user",
		Codec:     codec.JSON[struct{}]{},
	}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("New error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewConstructsCache(t *testing.T) {
	t.Parallel()

	cache, err := New(Options[struct{}]{
		Namespace: "user",
		Client:    &fakeClient{},
		Codec:     codec.JSON[struct{}]{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cache == nil {
		t.Fatal("New returned nil cache")
	}
}

func TestCloseClientOwnershipDisabledLeavesClientOpen(t *testing.T) {
	t.Parallel()

	client := &closableClient{}
	cache, err := New(Options[struct{}]{
		Namespace:   "user",
		Client:      client,
		Codec:       codec.JSON[struct{}]{},
		CloseClient: false,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.closeCalls != 0 {
		t.Fatalf("close calls = %d, want 0 when CloseClient is false", client.closeCalls)
	}
}

func TestCloseClientOwnershipEnabledClosesClientOnce(t *testing.T) {
	t.Parallel()

	client := &closableClient{}
	cache, err := New(Options[struct{}]{
		Namespace:   "user",
		Client:      client,
		Codec:       codec.JSON[struct{}]{},
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1 when CloseClient is true", client.closeCalls)
	}
}

func TestGenStoreCloseSharedClientNoop(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewGenStore(client)
	if err != nil {
		t.Fatalf("NewGenStore: %v", err)
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := client.closeCalls.Load(); got != 0 {
		t.Fatalf("shared client should not be closed, got %d close calls", got)
	}
}

func TestGenStoreCloseOwnedClientOnce(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewGenStoreWithOptions(GenStoreOptions{
		Client:      client,
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("NewGenStoreWithOptions: %v", err)
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close second: %v", err)
	}
	if got := client.closeCalls.Load(); got != 1 {
		t.Fatalf("owned client should be closed once, got %d close calls", got)
	}
}

func TestGenStoreWithOptionsRejectsNilClient(t *testing.T) {
	t.Parallel()

	_, err := NewGenStoreWithOptions(GenStoreOptions{})
	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewGenStoreWithOptions error mismatch: %v", err)
	}
}

func TestGenStoreNilReceiverReturnsErrNilClient(t *testing.T) {
	t.Parallel()

	var store *GenStore
	key := coregen.NewCacheKey("k")

	if _, err := store.Snapshot(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Snapshot error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.SnapshotMany(context.Background(), []coregen.CacheKey{key}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("SnapshotMany error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.Bump(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Bump error = %v, want %v", err, ErrNilClient)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close error = %v, want nil", err)
	}
}
