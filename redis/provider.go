package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"

	pr "github.com/unkn0wn-root/cascache/provider"
)

// Provider implements cascache/provider.Provider on top of go-redis.
type Provider struct {
	rdb         goredis.UniversalClient
	closeClient bool
}

var _ pr.Provider = (*Provider)(nil)
var _ pr.Adder = (*Provider)(nil)

type ProviderOptions struct {
	Client      goredis.UniversalClient
	CloseClient bool // set true only if this provider exclusively owns the client
}

// NewProvider constructs the Redis value-store adapter.
// Shared clients are left open by default.
func NewProvider(client goredis.UniversalClient) (*Provider, error) {
	return NewProviderWithOptions(ProviderOptions{Client: client})
}

// NewProviderWithOptions constructs the Redis value-store adapter with
// explicit client lifecycle ownership.
func NewProviderWithOptions(opts ProviderOptions) (*Provider, error) {
	if opts.Client == nil {
		return nil, ErrNilClient
	}
	return &Provider{rdb: opts.Client, closeClient: opts.CloseClient}, nil
}

func (p *Provider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, err := p.rdb.Get(ctx, key).Bytes()
	if err == goredis.Nil {
		return nil, false, nil // miss
	}
	if err != nil {
		return nil, false, err // transport/server error
	}
	return b, true, nil
}

func (p *Provider) Set(ctx context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 0 // treat non-positive TTLs as "no expiry" per provider contract
	}

	err := p.rdb.Set(ctx, key, value, ttl).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *Provider) Add(ctx context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 0
	}

	ok, err := p.rdb.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (p *Provider) Del(ctx context.Context, key string) error {
	return p.rdb.Del(ctx, key).Err()
}

// Close releases the underlying Redis client only when this provider owns it.
// Safe to call multiple times; repeated calls become no-ops.
func (p *Provider) Close(context.Context) error {
	if p.closeClient {
		if err := p.rdb.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			return err
		}
	}
	return nil
}
