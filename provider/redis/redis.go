package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type Provider struct {
	rdb redis.UniversalClient
}

type Config struct {
	Client redis.UniversalClient
}

func New(cfg Config) (*Provider, error) {
	if cfg.Client == nil {
		return nil, ErrNilClient
	}
	return &Provider{rdb: cfg.Client}, nil
}

var ErrNilClient = redis.Nil // re-use sentinel for brevity

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	res, err := p.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	return res, err == nil, err
}

func (p *Provider) Set(ctx context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	return true, p.rdb.Set(ctx, key, value, ttl).Err()
}

func (p *Provider) Del(ctx context.Context, key string) error {
	return p.rdb.Del(ctx, key).Err()
}

func (p *Provider) Close(ctx context.Context) error {
	return p.rdb.Close()
}
