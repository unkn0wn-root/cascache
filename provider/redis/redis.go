// Package redis adapts Redis as a cascache value store.
package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	pr "github.com/unkn0wn-root/cascache/v4/provider"
)

var ErrNilClient = errors.New("redis provider: nil client")

// Redis is a value-only [pr.Store]. Use backend/redis when values and fences
// both live in Redis.
type Redis struct {
	rdb         goredis.UniversalClient
	closeClient bool
}

var _ pr.Store = (*Redis)(nil)

// Config configures a [Redis] store.
type Config struct {
	Client      goredis.UniversalClient
	CloseClient bool // set true only if this provider exclusively owns the client
}

// New returns a Redis-backed store.
func New(cfg Config) (*Redis, error) {
	if cfg.Client == nil {
		return nil, ErrNilClient
	}
	return &Redis{rdb: cfg.Client, closeClient: cfg.CloseClient}, nil
}

func (p *Redis) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := p.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (p *Redis) Set(ctx context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	// Store treats negative TTLs as no expiry; go-redis treats them as KeepTTL.
	if ttl < 0 {
		ttl = 0
	}

	if err := p.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (p *Redis) Del(ctx context.Context, key string) error {
	return p.rdb.Del(ctx, key).Err()
}

// Close releases the client when Config.CloseClient is set.
func (p *Redis) Close(context.Context) error {
	if p.closeClient {
		if err := p.rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			return err
		}
	}
	return nil
}
