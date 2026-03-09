package genstore

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

type fakeUniversalClient struct {
	redis.UniversalClient
	closeCalls atomic.Int32
}

func (c *fakeUniversalClient) Close() error {
	c.closeCalls.Add(1)
	return nil
}

func TestRedisGenStoreCloseSharedClientNoop(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store := NewRedisGenStore(client)

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := client.closeCalls.Load(); got != 0 {
		t.Fatalf("shared client should not be closed, got %d close calls", got)
	}
}

func TestRedisGenStoreCloseOwnedClientOnce(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewRedisGenStoreWithOptions(RedisGenStoreOptions{
		Client:      client,
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("NewRedisGenStoreWithOptions: %v", err)
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

func TestRedisGenStoreNilClientReturnsError(t *testing.T) {
	t.Parallel()

	store := NewRedisGenStore(nil)

	k := NewCacheKey("k")
	if _, err := store.Snapshot(context.Background(), k); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("Snapshot error mismatch: %v", err)
	}
	if _, err := store.SnapshotMany(context.Background(), []CacheKey{k}); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("SnapshotMany error mismatch: %v", err)
	}
	if _, err := store.Bump(context.Background(), k); !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("Bump error mismatch: %v", err)
	}
}

func TestRedisGenStoreWithOptionsRejectsNilClient(t *testing.T) {
	t.Parallel()

	_, err := NewRedisGenStoreWithOptions(RedisGenStoreOptions{})
	if !errors.Is(err, ErrNilRedisClient) {
		t.Fatalf("NewRedisGenStoreWithOptions error mismatch: %v", err)
	}
}
