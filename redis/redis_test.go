package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v3/codec"
	"github.com/unkn0wn-root/cascache/v3/version"
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

func TestNewKeyMutatorWithOptionsStoresVersionTTL(t *testing.T) {
	t.Parallel()

	mutator, err := NewKeyMutatorWithOptions(KeyMutatorOptions{
		Client:     &fakeClient{},
		VersionTTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewKeyMutatorWithOptions: %v", err)
	}
	if mutator.versionTTL != 2*time.Minute {
		t.Fatalf("versionTTL = %v, want %v", mutator.versionTTL, 2*time.Minute)
	}
}

func TestNewProviderNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewProvider(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewProvider error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewVersionStoreNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewVersionStore(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewVersionStore error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewVersionStoreWithOptionsStoresVersionTTL(t *testing.T) {
	t.Parallel()

	store, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:     &fakeClient{},
		VersionTTL: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}
	if store.versionTTL != 90*time.Second {
		t.Fatalf("versionTTL = %v, want %v", store.versionTTL, 90*time.Second)
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

func TestVersionStoreCloseSharedClientNoop(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewVersionStore(client)
	if err != nil {
		t.Fatalf("NewVersionStore: %v", err)
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := client.closeCalls.Load(); got != 0 {
		t.Fatalf("shared client should not be closed, got %d close calls", got)
	}
}

func TestVersionStoreCloseOwnedClientOnce(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:      client,
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
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

func TestVersionStoreWithOptionsRejectsNilClient(t *testing.T) {
	t.Parallel()

	_, err := NewVersionStoreWithOptions(VersionStoreOptions{})
	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewVersionStoreWithOptions error mismatch: %v", err)
	}
}

func TestVersionStoreNilReceiverReturnsErrNilClient(t *testing.T) {
	t.Parallel()

	var store *VersionStore
	key := version.NewCacheKey("k")

	if _, err := store.Snapshot(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Snapshot error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.SnapshotMany(
		context.Background(),
		[]version.CacheKey{key},
	); !errors.Is(
		err,
		ErrNilClient,
	) {
		t.Fatalf("SnapshotMany error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.Advance(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Advance error = %v, want %v", err, ErrNilClient)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close error = %v, want nil", err)
	}
}

func TestParseFenceRoundTrip(t *testing.T) {
	t.Parallel()

	want, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}

	got, err := version.ParseFence(want.String())
	if err != nil {
		t.Fatalf("ParseFence: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("fence = %s, want %s", got, want)
	}
}

func TestParseFenceRejectsLegacyCounter(t *testing.T) {
	t.Parallel()

	if _, err := version.ParseFence("7"); err == nil {
		t.Fatal("expected legacy counter format to be rejected")
	}
}
