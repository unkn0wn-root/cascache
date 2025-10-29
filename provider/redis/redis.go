package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	pr "github.com/unkn0wn-root/cascache/provider"
)

var ErrNilClient = errors.New("redis provider: nil client")

type Redis struct {
	rdb         goredis.UniversalClient
	closeClient bool
}

var _ pr.Provider = (*Redis)(nil)

type Config struct {
	Client      goredis.UniversalClient
	CloseClient bool // set true only if this provider exclusively owns the client
}

func New(cfg Config) (*Redis, error) {
	if cfg.Client == nil {
		return nil, ErrNilClient
	}
	return &Redis{rdb: cfg.Client, closeClient: cfg.CloseClient}, nil
}

func (p *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := p.rdb.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return nil, false, nil // miss
	}
	if err != nil {
		return nil, false, err // transport/server error
	}
	return b, true, nil
}

func (p *Redis) Set(ctx context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 0 // treat non-positive TTLs as "no expiry" per provider contract
	}

	err := p.rdb.Set(ctx, key, value, ttl).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *Redis) Del(ctx context.Context, key string) error {
	return p.rdb.Del(ctx, key).Err()
}

// Close releases the underlying redis client only when this provider owns it.
// Safe to call multiple times; repeated calls become no-ops.
func (p *Redis) Close(context.Context) error {
	if p.closeClient {
		if err := p.rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			return err
		}
	}
	return nil
}
