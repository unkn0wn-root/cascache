package ristretto

import (
	"context"
	"errors"
	"time"

	rc "github.com/dgraph-io/ristretto"
)

type Provider struct {
	c *rc.Cache
}

type Config struct {
	NumCounters int64
	MaxCost     int64
	BufferItems int64
	Metrics     bool
	// Cost in Ristretto is provided by the caller (cascache passes cost per Set).
}

func New(cfg Config) (*Provider, error) {
	if cfg.NumCounters <= 0 || cfg.MaxCost <= 0 || cfg.BufferItems <= 0 {
		return nil, errors.New("ristretto: invalid config")
	}
	c, err := rc.NewCache(&rc.Config{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: cfg.BufferItems,
		Metrics:     cfg.Metrics,
	})
	if err != nil {
		return nil, err
	}
	return &Provider{c: c}, nil
}

func (p *Provider) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := p.c.Get(key)
	if !ok {
		return nil, false, nil
	}
	b, _ := v.([]byte)
	if b == nil {
		// self-heal: drop unexpected entry shape
		p.c.Del(key)
		return nil, false, nil
	}
	return b, true, nil
}

func (p *Provider) Set(_ context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	return p.c.SetWithTTL(key, value, cost, ttl), nil
}

func (p *Provider) Del(_ context.Context, key string) error {
	p.c.Del(key)
	return nil
}

func (p *Provider) Close(_ context.Context) error {
	p.c.Wait()
	p.c.Close()
	return nil
}

// Helper to expose metrics if desired by the application (not part of cascache.Provider).
func (p *Provider) Metrics() *rc.Metrics { return p.c.Metrics }
