package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/genstore"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
)

var ErrNilClient = errors.New("cascache/redis: nil client")

// KeyMutator implements cascache.KeyMutator with Redis Lua scripts.
// Values and versions must live in the same Redis deployment.
type KeyMutator struct {
	client goredis.UniversalClient
}

var _ cascache.KeyMutator = (*KeyMutator)(nil)

var setIfVersionScript = goredis.NewScript(`
local current = redis.call("GET", KEYS[1])
local current_version = 0
if current then
	current_version = tonumber(current)
	if not current_version then
		return redis.error_reply("cascache: invalid stored version")
	end
end

local expected_version = tonumber(ARGV[1])
if current_version ~= expected_version then
	return 0
end

local ttl_ms = tonumber(ARGV[3])
if ttl_ms and ttl_ms > 0 then
	redis.call("SET", KEYS[2], ARGV[2], "PX", ttl_ms)
else
	redis.call("SET", KEYS[2], ARGV[2])
end

return 1
`)

var invalidateScript = goredis.NewScript(`
redis.call("INCR", KEYS[1])
redis.call("DEL", KEYS[2])
return 1
`)

// NewKeyMutator constructs the Redis-backed single-key mutation capability used
// by this package's New constructor.
func NewKeyMutator(client goredis.UniversalClient) (*KeyMutator, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	return &KeyMutator{client: client}, nil
}

func (s *KeyMutator) SetIfVersion(ctx context.Context, versionKey genstore.CacheKey, valueKey string, expected uint64, wireValue []byte, ttl time.Duration) (bool, error) {
	if s == nil || s.client == nil {
		return false, ErrNilClient
	}
	res, err := setIfVersionScript.Run(
		ctx,
		s.client,
		[]string{versionStorageKey(versionKey), valueKey},
		expected,
		wireValue,
		ttlMillis(ttl),
	).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

func (s *KeyMutator) Invalidate(ctx context.Context, versionKey genstore.CacheKey, valueKey string) error {
	if s == nil || s.client == nil {
		return ErrNilClient
	}
	return invalidateScript.Run(
		ctx,
		s.client,
		[]string{versionStorageKey(versionKey), valueKey},
	).Err()
}

func versionStorageKey(cacheKey genstore.CacheKey) string {
	return keyutil.GenStorageKey(keyutil.CacheKey(cacheKey.String()))
}

func ttlMillis(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}

	ms := ttl.Milliseconds()
	if ms == 0 {
		return 1
	}
	return ms
}
