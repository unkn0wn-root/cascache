package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache"
	keyutil "github.com/unkn0wn-root/cascache/internal/keys"
	"github.com/unkn0wn-root/cascache/internal/wire"
	"github.com/unkn0wn-root/cascache/version"
)

var ErrNilClient = errors.New("cascache/redis: nil client")

// KeyMutator implements cascache.KeyMutator with Redis Lua scripts.
// Values and authoritative version state must live in the same Redis deployment.
type KeyMutator struct {
	client     goredis.UniversalClient
	versionTTL time.Duration
}

var _ cascache.KeyMutator = (*KeyMutator)(nil)

var setIfVersionScript = goredis.NewScript(`
local current = redis.call("GET", KEYS[1])
local expect_missing = ARGV[1] == "1"
local expected_fence = ARGV[2]
local init_fence = ARGV[3]
local wire_value = ARGV[4]
local ttl_ms = tonumber(ARGV[5])
local version_ttl_ms = tonumber(ARGV[6])

if current then
	if expect_missing then
		return 0
	end
	if current ~= expected_fence then
		return 0
	end
else
	if not expect_missing then
		return 0
	end
	if version_ttl_ms and version_ttl_ms > 0 then
		redis.call("SET", KEYS[1], init_fence, "PX", version_ttl_ms)
	else
		redis.call("SET", KEYS[1], init_fence)
	end
end

if ttl_ms and ttl_ms > 0 then
	redis.call("SET", KEYS[2], wire_value, "PX", ttl_ms)
else
	redis.call("SET", KEYS[2], wire_value)
end

return 1
`)

var invalidateScript = goredis.NewScript(`
local state = ARGV[1]
local version_ttl_ms = tonumber(ARGV[2])

if version_ttl_ms and version_ttl_ms > 0 then
	redis.call("SET", KEYS[1], state, "PX", version_ttl_ms)
else
	redis.call("SET", KEYS[1], state)
end
redis.call("DEL", KEYS[2])
return 1
`)

type KeyMutatorOptions struct {
	Client     goredis.UniversalClient
	VersionTTL time.Duration
}

// NewKeyMutator constructs the Redis-backed single-key mutatator.
func NewKeyMutator(client goredis.UniversalClient) (*KeyMutator, error) {
	return NewKeyMutatorWithOptions(KeyMutatorOptions{Client: client})
}

// NewKeyMutatorWithOptions constructs the Redis-backed atomic single-key
// mutator. VersionTTL bounds how long Redis retains authoritative version state
// after create/invalidate operations; zero keeps it indefinitely.
func NewKeyMutatorWithOptions(opts KeyMutatorOptions) (*KeyMutator, error) {
	if opts.Client == nil {
		return nil, ErrNilClient
	}
	return &KeyMutator{
		client:     opts.Client,
		versionTTL: opts.VersionTTL,
	}, nil
}

func (s *KeyMutator) SetIfVersion(
	ctx context.Context,
	versionKey version.CacheKey,
	valueKey string,
	expected version.Snapshot,
	payload []byte,
	ttl time.Duration,
) (bool, error) {
	if s == nil || s.client == nil {
		return false, ErrNilClient
	}

	var (
		expectMissing string
		expectedFence string
		initFence     string
		fence         version.Fence
		err           error
	)

	if expected.Exists {
		expectMissing = "0"
		expectedFence = expected.Fence.String()
		fence = expected.Fence
	} else {
		expectMissing = "1"
		fence, err = version.NewFence()
		if err != nil {
			return false, err
		}
		initFence = fence.String()
	}

	wv, err := wire.EncodeSingle(fence, payload)
	if err != nil {
		return false, err
	}

	r, err := setIfVersionScript.Run(
		ctx,
		s.client,
		[]string{versionStorageKey(versionKey), valueKey},
		expectMissing,
		expectedFence,
		initFence,
		wv,
		ttlMillis(ttl),
		ttlMillis(s.versionTTL),
	).Int()
	if err != nil {
		return false, err
	}
	return r == 1, nil
}

func (s *KeyMutator) Invalidate(
	ctx context.Context,
	versionKey version.CacheKey,
	valueKey string,
) error {
	if s == nil || s.client == nil {
		return ErrNilClient
	}

	f, err := version.NewFence()
	if err != nil {
		return err
	}

	return invalidateScript.Run(
		ctx,
		s.client,
		[]string{versionStorageKey(versionKey), valueKey},
		f.String(),
		ttlMillis(s.versionTTL),
	).Err()
}

func versionStorageKey(cacheKey version.CacheKey) string {
	return keyutil.VersionStorageKey(keyutil.CacheKey(cacheKey.String()))
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
