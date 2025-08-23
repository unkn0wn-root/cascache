package genstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisGenStore shares generations across processes and survives restarts.
type RedisGenStore struct {
	rdb redis.UniversalClient
	ns  string // logical namespace to avoid collisions; match Options.Namespace
}

func NewRedisGenStore(client redis.UniversalClient, namespace string) *RedisGenStore {
	return &RedisGenStore{rdb: client, ns: namespace}
}

func (s *RedisGenStore) key(k string) string { return "gen:" + s.ns + ":" + k }

func (s *RedisGenStore) Snapshot(ctx context.Context, storageKey string) (uint64, error) {
	res, err := s.rdb.Get(ctx, s.key(storageKey)).Result()
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

func (s *RedisGenStore) SnapshotMany(ctx context.Context, storageKeys []string) (map[string]uint64, error) {
	if len(storageKeys) == 0 {
		return map[string]uint64{}, nil
	}
	keys := make([]string, len(storageKeys))
	for i, k := range storageKeys {
		keys[i] = s.key(k)
	}
	vals, err := s.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]uint64, len(storageKeys))
	for i, v := range vals {
		if v == nil {
			out[storageKeys[i]] = 0
			continue
		}
		str := fmt.Sprint(v)
		u, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis gen parse at %s: %w", storageKeys[i], err)
		}
		out[storageKeys[i]] = u
	}
	return out, nil
}

func (s *RedisGenStore) Bump(ctx context.Context, storageKey string) (uint64, error) {
	v, err := s.rdb.Incr(ctx, s.key(storageKey)).Uint64()
	return v, err
}

func (s *RedisGenStore) Cleanup(time.Duration) {} // not applicable

func (s *RedisGenStore) Close(ctx context.Context) error { return s.rdb.Close() }
