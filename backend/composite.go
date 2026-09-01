package backend

import (
	"bytes"
	"context"
	"errors"
	"hash/maphash"
	"sync"

	"github.com/unkn0wn-root/cascache/v4/internal/typednil"
	"github.com/unkn0wn-root/cascache/v4/provider"
)

const compositeShards = 256

var (
	// ErrNilStore reports a missing or typed-nil value store.
	ErrNilStore = errors.New("cascache/backend: nil value store")
	// ErrNilFenceStore reports a missing or typed-nil fence store.
	ErrNilFenceStore = errors.New("cascache/backend: nil fence store")
)

// Composite combines a value store with a [FenceStore]. It can pair local
// values with local or shared fences. Operations are not atomic across the two
// stores, so a race may waste a write or cause a miss. Reads still require the
// value's fence to be current.
//
// Value operations are serialized per key so [Composite.Discard] cannot remove
// a newer value. Prefer a native backend when the value store is remote.
type Composite struct {
	values provider.Store
	fences FenceStore
	seed   maphash.Seed
	locks  [compositeShards]sync.Mutex
}

var _ Backend = (*Composite)(nil)

// NewComposite joins a value store and a fence store. It takes ownership of
// neither.
func NewComposite(values provider.Store, fences FenceStore) (*Composite, error) {
	switch {
	case typednil.Is(values):
		return nil, ErrNilStore
	case typednil.Is(fences):
		return nil, ErrNilFenceStore
	}
	return &Composite{values: values, fences: fences, seed: maphash.MakeSeed()}, nil
}

func (b *Composite) shardLock(key Key) *sync.Mutex {
	return &b.locks[maphash.String(b.seed, key.ID())%compositeShards]
}

// Read reads the value first. Reading the fence first could reject a value
// stored between the two reads.
func (b *Composite) Read(ctx context.Context, key Key) (ReadResult, error) {
	if err := CheckKey(key); err != nil {
		return ReadResult{}, err
	}

	value, found, err := b.values.Get(ctx, ValueKey(key))
	if err != nil || !found {
		return ReadResult{}, err
	}

	fence, fenceFound, err := b.fences.Read(ctx, key)
	if err != nil {
		return ReadResult{}, err
	}
	return ReadResult{Value: value, Found: true, Fence: fence, FenceFound: fenceFound}, nil
}

func (b *Composite) Ensure(ctx context.Context, key Key, candidate Fence) (Fence, error) {
	if err := CheckKeyFence(key, candidate); err != nil {
		return Fence{}, err
	}
	return b.fences.Ensure(ctx, key, candidate)
}

func (b *Composite) CompareAndStore(ctx context.Context, req StoreRequest) (StoreResult, error) {
	if err := CheckKeyFence(req.Key, req.Expected); err != nil {
		return StoreResult{}, err
	}

	// Check and refresh together before storing under this fence.
	current, err := b.fences.Retain(ctx, req.Key, req.Expected)
	if err != nil {
		return StoreResult{}, err
	}
	if !current {
		return StoreResult{Status: StoreConflict}, nil
	}

	ttl := ClampTTL(req.TTL, b.fences.Lifetime())

	mu := b.shardLock(req.Key)
	mu.Lock()
	defer mu.Unlock()

	stored, err := b.values.Set(ctx, ValueKey(req.Key), req.Value, req.Cost, ttl)
	if err != nil {
		return StoreResult{}, err
	}
	if !stored {
		return StoreResult{Status: StoreRejected}, nil
	}
	return StoreResult{Status: StoreStored, EffectiveTTL: ttl}, nil
}

// Invalidate replaces the fence before deleting the value. This may delete a
// concurrent fill, but an in-flight load cannot restore a retired value.
func (b *Composite) Invalidate(ctx context.Context, key Key, next Fence) (InvalidateResult, error) {
	if err := CheckKeyFence(key, next); err != nil {
		return InvalidateResult{}, err
	}
	if err := b.fences.Replace(ctx, key, next); err != nil {
		return InvalidateResult{}, err
	}

	mu := b.shardLock(key)
	mu.Lock()
	defer mu.Unlock()

	return InvalidateResult{CleanupErr: b.values.Del(ctx, ValueKey(key))}, nil
}

func (b *Composite) Discard(ctx context.Context, key Key, rejected []byte) (bool, error) {
	if err := CheckKey(key); err != nil {
		return false, err
	}
	storage := ValueKey(key)

	mu := b.shardLock(key)
	mu.Lock()
	defer mu.Unlock()

	current, found, err := b.values.Get(ctx, storage)
	if err != nil || !found || !bytes.Equal(current, rejected) {
		return false, err
	}
	if err := b.values.Del(ctx, storage); err != nil {
		return false, err
	}
	return true, nil
}
