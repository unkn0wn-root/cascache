package genstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
)

var ErrNilRedisClient = errors.New("redis genstore: nil client")

// RedisGenStore shares per-key generations across processes and survives restarts.
// Optionally, a TTL can be applied to generation keys to prevent unbounded growth.
// If a generation key expires, readers observe gen=0 and cache entries self-heal.
type RedisGenStore struct {
	rdb         redis.UniversalClient
	ns          string        // logical namespace; should match Options.Namespace
	ttl         time.Duration // optional TTL for generation keys; 0 disables expiry
	closeClient bool
	closeOnce   sync.Once
	closeErr    error
}

var _ GenStore = (*RedisGenStore)(nil)

type RedisGenStoreOptions struct {
	Client      redis.UniversalClient
	Namespace   string
	TTL         time.Duration
	CloseClient bool
}

// NewRedisGenStore creates a Redis-backed generation store without TTL.
func NewRedisGenStore(client redis.UniversalClient, namespace string) *RedisGenStore {
	return &RedisGenStore{rdb: client, ns: namespace}
}

// NewRedisGenStoreWithTTL creates a Redis-backed generation store with TTL.
// If ttl <= 0, keys do not expire.
func NewRedisGenStoreWithTTL(client redis.UniversalClient, namespace string, ttl time.Duration) *RedisGenStore {
	return &RedisGenStore{rdb: client, ns: namespace, ttl: ttl}
}

// NewRedisGenStoreWithOptions creates a Redis-backed generation store with
// explicit lifecycle ownership. CloseClient should be true only when this
// genstore exclusively owns the client.
func NewRedisGenStoreWithOptions(opts RedisGenStoreOptions) (*RedisGenStore, error) {
	if opts.Client == nil {
		return nil, ErrNilRedisClient
	}
	return &RedisGenStore{
		rdb:         opts.Client,
		ns:          opts.Namespace,
		ttl:         opts.TTL,
		closeClient: opts.CloseClient,
	}, nil
}

func (s *RedisGenStore) client() (redis.UniversalClient, error) {
	if s == nil || s.rdb == nil {
		return nil, ErrNilRedisClient
	}
	return s.rdb, nil
}

func (s *RedisGenStore) key(k string) string { return keyutil.GenStorageKey(s.ns, k) }

// Snapshot returns the current generation.
// Missing keys are treated as generation 0.
func (s *RedisGenStore) Snapshot(ctx context.Context, storageKey string) (uint64, error) {
	rdb, err := s.client()
	if err != nil {
		return 0, err
	}

	res, err := rdb.Get(ctx, s.key(storageKey)).Result()
	if err == redis.Nil {
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
func (s *RedisGenStore) SnapshotMany(ctx context.Context, storageKeys []string) (map[string]uint64, error) {
	if len(storageKeys) == 0 {
		return map[string]uint64{}, nil
	}

	rdb, err := s.client()
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(storageKeys))
	for i, k := range storageKeys {
		keys[i] = s.key(k)
	}
	vals, err := rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	out := make(map[string]uint64, len(storageKeys))
	for i, v := range vals {
		switch vv := v.(type) {
		case nil:
			out[storageKeys[i]] = 0
		case string:
			u, err := strconv.ParseUint(vv, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", storageKeys[i], err)
			}
			out[storageKeys[i]] = u
		case []byte:
			u, err := strconv.ParseUint(string(vv), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", storageKeys[i], err)
			}
			out[storageKeys[i]] = u
		default:
			str := fmt.Sprint(vv)
			u, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("redis gen parse at %s: %w", storageKeys[i], err)
			}
			out[storageKeys[i]] = u
		}
	}
	return out, nil
}

// Bump atomically increments the generation and (optionally) refreshes TTL.
// When ttl > 0, INCR + EXPIRE are pipelined in a single round-trip and the
// INCR result is captured from the pipeline (no extra INCR).
func (s *RedisGenStore) Bump(ctx context.Context, storageKey string) (uint64, error) {
	rdb, err := s.client()
	if err != nil {
		return 0, err
	}

	k := s.key(storageKey)

	if s.ttl <= 0 {
		v, err := rdb.Incr(ctx, k).Result()
		if err != nil {
			return 0, err
		}
		return uint64(v), nil
	}

	var incr *redis.IntCmd
	_, err = rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.Incr(ctx, k)
		p.Expire(ctx, k, s.ttl)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint64(incr.Val()), nil
}

// Cleanup is not applicable for RedisGenStore (Redis handles expiry if TTL is set).
func (s *RedisGenStore) Cleanup(time.Duration) {}

// Close closes the underlying Redis client only when this genstore owns it.
// Shared clients are left open by default.
func (s *RedisGenStore) Close(context.Context) error {
	if s == nil || !s.closeClient {
		return nil
	}

	rdb, err := s.client()
	if err != nil {
		return err
	}

	s.closeOnce.Do(func() {
		if err := rdb.Close(); err != nil && !errors.Is(err, redis.ErrClosed) {
			s.closeErr = err
		}
	})
	return s.closeErr
}
