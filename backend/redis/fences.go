package redis

import (
	"context"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

func (f *fenceStore) Ensure(ctx context.Context, key backend.Key, candidate backend.Fence) (backend.Fence, error) {
	if err := checkKeyFence(f.client, key, candidate); err != nil {
		return backend.Fence{}, err
	}
	return ensureFence(ctx, f.client, f.fenceTTL, key, candidate)
}

func (f *fenceStore) Read(ctx context.Context, key backend.Key) (backend.Fence, bool, error) {
	if err := checkKey(f.client, key); err != nil {
		return backend.Fence{}, false, err
	}

	raw, err := f.client.Get(ctx, backend.FenceKey(key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return backend.Fence{}, false, nil
	}
	if err != nil {
		return backend.Fence{}, false, err
	}

	fence, err := backend.ParseFence(raw)
	if err != nil {
		return backend.Fence{}, false, wrapFenceParse(err)
	}
	return fence, true, nil
}

func (f *fenceStore) Retain(ctx context.Context, key backend.Key, expected backend.Fence) (bool, error) {
	if err := checkKeyFence(f.client, key, expected); err != nil {
		return false, err
	}

	reply, err := retainScript.Run(
		ctx,
		f.client,
		[]string{backend.FenceKey(key)},
		expected.Bytes(),
		ttlMillis(f.fenceTTL),
	).Int()
	if err != nil {
		return false, err
	}
	return reply == 1, nil
}

// Replace uses SET to replace both the fence and its old TTL.
func (f *fenceStore) Replace(ctx context.Context, key backend.Key, next backend.Fence) error {
	if err := checkKeyFence(f.client, key, next); err != nil {
		return err
	}
	return f.client.Set(ctx, backend.FenceKey(key), next.Bytes(), f.fenceTTL).Err()
}
