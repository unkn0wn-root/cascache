// Package ristretto adapts Ristretto as a cascache value store.
package ristretto

import (
	"context"
	"fmt"
	"time"

	rc "github.com/dgraph-io/ristretto"

	pr "github.com/unkn0wn-root/cascache/v4/provider"
)

// Ristretto is a [pr.Store] backed by a Ristretto cache.
type Ristretto struct {
	c *rc.Cache
}

var _ pr.Store = (*Ristretto)(nil)

// Config configures the underlying cache. NumCounters, MaxCost, and BufferItems
// must be positive.
type Config struct {
	// NumCounters is how many keys to track for admission.
	NumCounters int64

	// MaxCost is the total admission budget.
	MaxCost int64

	// BufferItems is the per-goroutine access buffer size.
	BufferItems int64

	// Metrics enables the counters behind [Ristretto.Metrics].
	Metrics bool
}

// New returns a Ristretto-backed store.
func New(cfg Config) (*Ristretto, error) {
	if cfg.NumCounters <= 0 || cfg.MaxCost <= 0 || cfg.BufferItems <= 0 {
		return nil, fmt.Errorf("ristretto: NumCounters, MaxCost and BufferItems must all be > 0 (got %d, %d, %d)",
			cfg.NumCounters, cfg.MaxCost, cfg.BufferItems)
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
		// Drop values not written by this provider.
		p.c.Del(key)
		return nil, false, nil
	}
	return b, true, nil
}

func (p *Ristretto) Set(_ context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	// Ristretto rejects negative TTLs; Store treats them as no expiry.
	if ttl < 0 {
		ttl = 0
	}

	return p.c.SetWithTTL(key, value, cost, ttl), nil
}

func (p *Ristretto) Del(_ context.Context, key string) error {
	p.c.Del(key)
	return nil
}

// Wait blocks until writes already accepted have been applied.
// Ristretto buffers writes, so Set may not be visible until Wait returns.
func (p *Ristretto) Wait() { p.c.Wait() }

func (p *Ristretto) Close(_ context.Context) error {
	p.c.Wait()
	p.c.Close()
	return nil
}

// Metrics returns Ristretto's metrics.
func (p *Ristretto) Metrics() *rc.Metrics { return p.c.Metrics }
