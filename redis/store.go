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

// VersionStore shares per-key authoritative version state across processes.
type VersionStore struct {
	rdb         goredis.UniversalClient
	closeClient bool
	versionTTL  time.Duration
	closeOnce   sync.Once
	closeErr    error
}

var (
	_ version.Store     = (*VersionStore)(nil)
	_ version.Refresher = (*VersionStore)(nil)
)

var refreshScript = goredis.NewScript(`
local current = redis.call("GET", KEYS[1])
local version_ttl_ms = tonumber(ARGV[1])

if not current then
	return 0
end

if version_ttl_ms and version_ttl_ms > 0 then
	redis.call("PEXPIRE", KEYS[1], version_ttl_ms)
else
	redis.call("PERSIST", KEYS[1])
end

return 1
`)

// ErrFenceParse reports malformed authoritative fence state read from Redis.
var ErrFenceParse = errors.New("cascache/redis: fence parse")

type VersionStoreOptions struct {
	Client      goredis.UniversalClient
	CloseClient bool
	VersionTTL  time.Duration
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

	return parseSnapshotValue(cacheKey, r)
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

	if usesSlotRouting(rdb) {
		return s.snapshotManyPipeline(ctx, rdb, cacheKeys)
	}
	return s.snapshotManyMGet(ctx, rdb, cacheKeys)
}

func (s *VersionStore) snapshotManyMGet(
	ctx context.Context,
	rdb goredis.UniversalClient,
	cacheKeys []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
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
		snap, err := parseSnapshotValue(cacheKeys[i], v)
		if err != nil {
			return nil, err
		}
		out[cacheKeys[i]] = snap
	}
	return out, nil
}

func (s *VersionStore) snapshotManyPipeline(
	ctx context.Context,
	rdb goredis.UniversalClient,
	cacheKeys []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
	cmds := make([]*goredis.StringCmd, len(cacheKeys))
	_, err := rdb.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
		for i, k := range cacheKeys {
			cmds[i] = pipe.Get(ctx, s.key(k))
		}
		return nil
	})
	return snapshotsFromStringCmds(cacheKeys, cmds, err)
}

func snapshotsFromStringCmds(
	cacheKeys []version.CacheKey,
	cmds []*goredis.StringCmd,
	pipelineErr error,
) (map[version.CacheKey]version.Snapshot, error) {
	if pipelineErr != nil && !errors.Is(pipelineErr, goredis.Nil) {
		return nil, pipelineErr
	}

	out := make(map[version.CacheKey]version.Snapshot, len(cacheKeys))
	for i, cmd := range cmds {
		if cmd == nil {
			return nil, fmt.Errorf("cascache/redis: missing pipeline command for %s", cacheKeys[i])
		}
		if err := cmd.Err(); errors.Is(err, goredis.Nil) {
			out[cacheKeys[i]] = version.Snapshot{}
			continue
		} else if err != nil {
			return nil, err
		}

		snap, err := parseSnapshotFence(cacheKeys[i], cmd.Val())
		if err != nil {
			return nil, err
		}
		out[cacheKeys[i]] = snap
	}
	return out, nil
}

func usesSlotRouting(rdb goredis.UniversalClient) bool {
	switch rdb.(type) {
	case *goredis.ClusterClient, *goredis.Ring:
		return true
	default:
		return false
	}
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

// Refresh extends or removes expiry for existing authoritative version state
// without changing its fence.
func (s *VersionStore) Refresh(ctx context.Context, cacheKey version.CacheKey) (bool, error) {
	rdb, err := s.client()
	if err != nil {
		return false, err
	}

	r, err := refreshScript.Run(
		ctx,
		rdb,
		[]string{s.key(cacheKey)},
		ttlMillis(s.versionTTL),
	).Int()
	if err != nil {
		return false, err
	}
	return r == 1, nil
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

func (s *VersionStore) client() (goredis.UniversalClient, error) {
	if s == nil || s.rdb == nil {
		return nil, ErrNilClient
	}
	return s.rdb, nil
}

func (s *VersionStore) key(cacheKey version.CacheKey) string {
	return keyutil.VersionStorageKey(keyutil.CacheKey(cacheKey.String()))
}

func parseSnapshotValue(cacheKey version.CacheKey, raw any) (version.Snapshot, error) {
	switch v := raw.(type) {
	case nil:
		return version.Snapshot{}, nil
	case string:
		return parseSnapshotFence(cacheKey, v)
	case []byte:
		return parseSnapshotFence(cacheKey, string(v))
	default:
		return version.Snapshot{}, fmt.Errorf(
			"%w at %s: unsupported redis type %T",
			ErrFenceParse,
			cacheKey,
			v,
		)
	}
}

func parseSnapshotFence(cacheKey version.CacheKey, raw string) (version.Snapshot, error) {
	f, err := version.ParseFence(raw)
	if err != nil {
		return version.Snapshot{}, fmt.Errorf("%w at %s: %w", ErrFenceParse, cacheKey, err)
	}
	return version.Snapshot{Fence: f, Exists: true}, nil
}
