package ristretto

import (
	"context"
	"errors"
	"time"

	rc "github.com/dgraph-io/ristretto"

	pr "github.com/unkn0wn-root/cascache/provider"
)

type Ristretto struct {
	c *rc.Cache
}

var _ pr.Provider = (*Ristretto)(nil)

type Config struct {
	NumCounters int64
	MaxCost     int64
	BufferItems int64
	Metrics     bool
	// Note: cascache passes per-entry cost in Set; we don't need rc.Config.Cost.
}

func New(cfg Config) (*Ristretto, error) {
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
	return &Ristretto{c: c}, nil
}

func (p *Ristretto) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := p.c.Get(key)
	if !ok {
		return nil, false, nil
	}
	b, _ := v.([]byte)
	if b == nil {
		// Self-heal: unexpected entry shape -> delete and miss.
		p.c.Del(key)
		return nil, false, nil
	}
	return b, true, nil
}

func (p *Ristretto) Set(_ context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	// Ristretto can reject writes under pressure -> ok=false, err=nil.
	return p.c.SetWithTTL(key, value, cost, ttl), nil
}

func (p *Ristretto) Del(_ context.Context, key string) error {
	p.c.Del(key)
	return nil
}

func (p *Ristretto) Close(_ context.Context) error {
	p.c.Wait()  // flush pending sets
	p.c.Close() // release resources
	return nil
}

// Optional helper (not part of cascache.Provider).
func (p *Ristretto) Metrics() *rc.Metrics { return p.c.Metrics }
