package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	keyutil "github.com/unkn0wn-root/cascache/v3/internal/keys"
	"github.com/unkn0wn-root/cascache/v3/version"
)

// VersionStore shares per-key authoritative version state across processes and survives restarts.
type VersionStore struct {
	rdb         goredis.UniversalClient
	closeClient bool
	versionTTL  time.Duration
	closeOnce   sync.Once
	closeErr    error
}

var _ version.Store = (*VersionStore)(nil)

type VersionStoreOptions struct {
	Client      goredis.UniversalClient
	CloseClient bool
	VersionTTL  time.Duration // 0 keeps authoritative version state indefinitely
}

// NewVersionStore constructs the Redis-backed authoritative version store.
// Shared clients are left open by default.
func NewVersionStore(client goredis.UniversalClient) (*VersionStore, error) {
	return NewVersionStoreWithOptions(VersionStoreOptions{Client: client})
}

// NewVersionStoreWithOptions constructs the Redis-backed authoritative version
// store with explicit lifecycle ownership. CloseClient should be true only when
// this version store exclusively owns the client. VersionTTL bounds how long Redis
// retains authoritative state after create/advance operations; zero keeps it
// indefinitely.
func NewVersionStoreWithOptions(opts VersionStoreOptions) (*VersionStore, error) {
	if opts.Client == nil {
		return nil, ErrNilClient
	}
	return &VersionStore{
		rdb:         opts.Client,
		closeClient: opts.CloseClient,
		versionTTL:  opts.VersionTTL,
	}, nil
}

func (s *VersionStore) client() (goredis.UniversalClient, error) {
	if s == nil || s.rdb == nil {
		return nil, ErrNilClient
	}
	return s.rdb, nil
}

func (s *VersionStore) key(cacheKey version.CacheKey) string {
	return keyutil.VersionStorageKey(keyutil.CacheKey(cacheKey.String()))
}

// Snapshot returns the current authoritative state.
func (s *VersionStore) Snapshot(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, error) {
	rdb, err := s.client()
	if err != nil {
		return version.Snapshot{}, err
	}

	r, err := rdb.Get(ctx, s.key(cacheKey)).Result()
	if err == goredis.Nil {
		return version.Snapshot{}, nil
	}
	if err != nil {
		return version.Snapshot{}, err
	}

	f, err := version.ParseFence(r)
	if err != nil {
		return version.Snapshot{}, err
	}
	return version.Snapshot{Fence: f, Exists: true}, nil
}

// SnapshotMany returns authoritative state for multiple keys.
func (s *VersionStore) SnapshotMany(
	ctx context.Context,
	cacheKeys []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
	if len(cacheKeys) == 0 {
		return map[version.CacheKey]version.Snapshot{}, nil
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

	out := make(map[version.CacheKey]version.Snapshot, len(cacheKeys))
	for i, v := range vals {
		switch vv := v.(type) {
		case nil:
			out[cacheKeys[i]] = version.Snapshot{}
		case string:
			fence, parseErr := version.ParseFence(vv)
			if parseErr != nil {
				return nil, fmt.Errorf("redis fence parse at %s: %w", cacheKeys[i], parseErr)
			}
			out[cacheKeys[i]] = version.Snapshot{Fence: fence, Exists: true}
		case []byte:
			fence, parseErr := version.ParseFence(string(vv))
			if parseErr != nil {
				return nil, fmt.Errorf("redis fence parse at %s: %w", cacheKeys[i], parseErr)
			}
			out[cacheKeys[i]] = version.Snapshot{Fence: fence, Exists: true}
		default:
			return nil, fmt.Errorf(
				"redis fence parse at %s: unsupported mget type %T",
				cacheKeys[i],
				vv,
			)
		}
	}
	return out, nil
}

// CreateIfMissing creates authoritative state with a fresh fence only if absent.
func (s *VersionStore) CreateIfMissing(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, bool, error) {
	rdb, err := s.client()
	if err != nil {
		return version.Snapshot{}, false, err
	}

	f, err := version.NewFence()
	if err != nil {
		return version.Snapshot{}, false, err
	}

	sp := version.Snapshot{
		Fence:  f,
		Exists: true,
	}

	ok, err := rdb.SetNX(ctx, s.key(cacheKey), sp.Fence.String(), s.versionTTL).Result()
	if err != nil {
		return version.Snapshot{}, false, err
	}
	if ok {
		return sp, true, nil
	}

	c, err := s.Snapshot(ctx, cacheKey)
	return c, false, err
}

// Advance moves the authoritative fence to a fresh value.
func (s *VersionStore) Advance(ctx context.Context, cacheKey version.CacheKey) (version.Snapshot, error) {
	rdb, err := s.client()
	if err != nil {
		return version.Snapshot{}, err
	}

	f, err := version.NewFence()
	if err != nil {
		return version.Snapshot{}, err
	}

	st := f.String()
	if err := rdb.Set(ctx, s.key(cacheKey), st, s.versionTTL).Err(); err != nil {
		return version.Snapshot{}, err
	}
	return version.Snapshot{Fence: f, Exists: true}, nil
}

// Cleanup is not applicable for Redis by default.
func (s *VersionStore) Cleanup(time.Duration) {}

// Close closes the underlying Redis client only when this version store owns it.
// Shared clients are left open by default.
func (s *VersionStore) Close(context.Context) error {
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
