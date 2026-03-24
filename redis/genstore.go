package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	coregen "github.com/unkn0wn-root/cascache/genstore"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
)

// GenStore shares per-key generations across processes and survives restarts.
// v2 strict mode does not expire generation keys because recycling a missing
// generation back to zero can resurrect stale cache entries.
type GenStore struct {
	rdb         goredis.UniversalClient
	closeClient bool
	closeOnce   sync.Once
	closeErr    error
}

var _ coregen.GenStore = (*GenStore)(nil)

type GenStoreOptions struct {
	Client      goredis.UniversalClient
	CloseClient bool
}

// NewGenStore constructs the Redis-backed strict generation store.
// Shared clients are left open by default.
func NewGenStore(client goredis.UniversalClient) (*GenStore, error) {
	return NewGenStoreWithOptions(GenStoreOptions{Client: client})
}

// NewGenStoreWithOptions constructs the Redis-backed generation store with
// explicit lifecycle ownership. CloseClient should be true only when this
// genstore exclusively owns the client.
func NewGenStoreWithOptions(opts GenStoreOptions) (*GenStore, error) {
	if opts.Client == nil {
		return nil, ErrNilClient
	}
	return &GenStore{
		rdb:         opts.Client,
		closeClient: opts.CloseClient,
	}, nil
}

func (s *GenStore) client() (goredis.UniversalClient, error) {
	if s == nil || s.rdb == nil {
		return nil, ErrNilClient
	}
	return s.rdb, nil
}

func (s *GenStore) key(cacheKey coregen.CacheKey) string {
	return keyutil.GenStorageKey(keyutil.CacheKey(cacheKey.String()))
}

// Snapshot returns the current generation.
// Missing keys are treated as generation 0.
func (s *GenStore) Snapshot(ctx context.Context, cacheKey coregen.CacheKey) (uint64, error) {
	rdb, err := s.client()
	if err != nil {
		return 0, err
	}

	res, err := rdb.Get(ctx, s.key(cacheKey)).Result()
	if err == goredis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	u, err := strconv.ParseUint(res, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redis gen parse: %w", err)
	}
	return u, nil
}

// SnapshotMany returns generations for multiple keys.
// Missing keys map to 0.
func (s *GenStore) SnapshotMany(ctx context.Context, cacheKeys []coregen.CacheKey) (map[coregen.CacheKey]uint64, error) {
	if len(cacheKeys) == 0 {
		return map[coregen.CacheKey]uint64{}, nil
	}

	rdb, err := s.client()
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(cacheKeys))
	for i, k := range cacheKeys {
		keys[i] = s.key(k)
	}
	vals, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	out := make(map[coregen.CacheKey]uint64, len(cacheKeys))
	for i, v := range vals {
		switch vv := v.(type) {
		case nil:
			out[cacheKeys[i]] = 0
		case string:
			u, err := strconv.ParseUint(vv, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", cacheKeys[i], err)
			}
			out[cacheKeys[i]] = u
		case []byte:
			u, err := strconv.ParseUint(string(vv), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", cacheKeys[i], err)
			}
			out[cacheKeys[i]] = u
		default:
			str := fmt.Sprint(vv)
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", cacheKeys[i], err)
			}
			out[cacheKeys[i]] = u
		}
	}
	return out, nil
}

// Bump atomically increments the generation.
func (s *GenStore) Bump(ctx context.Context, cacheKey coregen.CacheKey) (uint64, error) {
	rdb, err := s.client()
	if err != nil {
		return 0, err
	}

	v, err := rdb.Incr(ctx, s.key(cacheKey)).Result()
	if err != nil {
		return 0, err
	}
	return uint64(v), nil
}

// Cleanup is not applicable for GenStore in strict mode.
func (s *GenStore) Cleanup(time.Duration) {}

// Close closes the underlying Redis client only when this genstore owns it.
// Shared clients are left open by default.
func (s *GenStore) Close(context.Context) error {
	if s == nil || !s.closeClient {
		return nil
	}

	rdb, err := s.client()
	if err != nil {
		return err
	}

	s.closeOnce.Do(func() {
		if err := rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			s.closeErr = err
		}
	})
	return s.closeErr
}
