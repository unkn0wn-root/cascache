package redis

import (
	"context"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

// Read gets the value and fence with one MGET from their shared cluster slot.
func (b *Backend) Read(ctx context.Context, key backend.Key) (backend.ReadResult, error) {
	if err := checkKey(b.client, key); err != nil {
		return backend.ReadResult{}, err
	}

	valueKey, fenceKey := backend.StorageKeys(key)
	values, err := b.client.MGet(ctx, valueKey, fenceKey).Result()
	if err != nil {
		return backend.ReadResult{}, err
	}
	return readResult(values)
}

func (b *Backend) Ensure(ctx context.Context, key backend.Key, candidate backend.Fence) (backend.Fence, error) {
	if err := checkKeyFence(b.client, key, candidate); err != nil {
		return backend.Fence{}, err
	}
	return ensureFence(ctx, b.client, b.fenceTTL, key, candidate)
}

func (b *Backend) CompareAndStore(ctx context.Context, req backend.StoreRequest) (backend.StoreResult, error) {
	if err := checkKeyFence(b.client, req.Key, req.Expected); err != nil {
		return backend.StoreResult{}, err
	}

	ttl := backend.ClampTTL(req.TTL, b.fenceTTL)
	valueKey, fenceKey := backend.StorageKeys(req.Key)

	reply, err := compareAndStoreScript.Run(
		ctx,
		b.client,
		[]string{fenceKey, valueKey},
		req.Expected.Bytes(),
		req.Value,
		ttlMillis(ttl),
		ttlMillis(b.fenceTTL),
	).Int()
	if err != nil {
		return backend.StoreResult{}, err
	}

	switch reply {
	case 0:
		return backend.StoreResult{Status: backend.StoreConflict}, nil
	case 1:
		// Redis does not have an admission-rejected outcome.
		return backend.StoreResult{Status: backend.StoreStored, EffectiveTTL: ttl}, nil
	default:
		return backend.StoreResult{}, errStoreReply
	}
}

// Invalidate changes the fence and deletes the value atomically.
func (b *Backend) Invalidate(
	ctx context.Context,
	key backend.Key,
	next backend.Fence,
) (backend.InvalidateResult, error) {
	if err := checkKeyFence(b.client, key, next); err != nil {
		return backend.InvalidateResult{}, err
	}

	valueKey, fenceKey := backend.StorageKeys(key)
	reply, err := invalidateScript.Run(
		ctx,
		b.client,
		[]string{fenceKey, valueKey},
		next.Bytes(),
		ttlMillis(b.fenceTTL),
	).Int()
	if err != nil {
		return backend.InvalidateResult{}, err
	}
	if reply != 1 {
		return backend.InvalidateResult{}, errScriptReply
	}
	return backend.InvalidateResult{}, nil
}

func (b *Backend) Discard(ctx context.Context, key backend.Key, rejected []byte) (bool, error) {
	if err := checkKey(b.client, key); err != nil {
		return false, err
	}

	reply, err := discardScript.Run(ctx, b.client, []string{backend.ValueKey(key)}, rejected).Int()
	if err != nil {
		return false, err
	}
	return reply == 1, nil
}
