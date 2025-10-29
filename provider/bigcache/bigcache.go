package bigcache

import (
	"context"
	"time"

	bc "github.com/allegro/bigcache/v3"

	pr "github.com/unkn0wn-root/cascache/provider"
)

// Note: BigCache has a global LifeWindow; it ignores per-entry TTLs passed to Set.
type BigCache struct {
	c *bc.BigCache
}

var _ pr.Provider = (*BigCache)(nil)

type Config struct {
	LifeWindow         time.Duration
	CleanWindow        time.Duration
	MaxEntriesInWindow int
	MaxEntrySize       int
	HardMaxCacheSizeMB int // ~ memory limit; 0 = unlimited
}

func New(cfg Config) (*BigCache, error) {
	conf := bc.DefaultConfig(cfg.LifeWindow)
	if cfg.CleanWindow > 0 {
		conf.CleanWindow = cfg.CleanWindow
	}
	if cfg.MaxEntriesInWindow > 0 {
		conf.MaxEntriesInWindow = cfg.MaxEntriesInWindow
	}
	if cfg.MaxEntrySize > 0 {
		conf.MaxEntrySize = cfg.MaxEntrySize
	}
	if cfg.HardMaxCacheSizeMB > 0 {
		conf.HardMaxCacheSize = cfg.HardMaxCacheSizeMB
	}
	c, err := bc.NewBigCache(conf)
	if err != nil {
		return nil, err
	}
	return &BigCache{c: c}, nil
}

func (p *BigCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	b, err := p.c.Get(key)
	if err == bc.ErrEntryNotFound {
		return nil, false, nil
	}
	return b, err == nil, err
}

func (p *BigCache) Set(_ context.Context, key string, value []byte, _ int64, _ time.Duration) (bool, error) {
	// Per-entry TTL not supported; LifeWindow applies.
	if err := p.c.Set(key, value); err != nil {
		return false, err
	}
	return true, nil
}

func (p *BigCache) Del(_ context.Context, key string) error {
	return p.c.Delete(key)
}

func (p *BigCache) Close(_ context.Context) error {
	return p.c.Close()
}
