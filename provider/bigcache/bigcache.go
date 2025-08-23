package bigcache

import (
	"context"
	"time"

	bc "github.com/allegro/bigcache/v3"
)

type Provider struct {
	c *bc.BigCache
}

type Config struct {
	LifeWindow         time.Duration
	CleanWindow        time.Duration
	MaxEntriesInWindow int
	MaxEntrySize       int
	HardMaxCacheSizeMB int // ~ memory limit; 0 = unlimited
}

func New(cfg Config) (*Provider, error) {
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
	return &Provider{c: c}, nil
}

func (p *Provider) Get(_ context.Context, key string) ([]byte, bool, error) {
	b, err := p.c.Get(key)
	if err == bc.ErrEntryNotFound {
		return nil, false, nil
	}
	return b, err == nil, err
}

func (p *Provider) Set(_ context.Context, key string, value []byte, _ int64, _ time.Duration) (bool, error) {
	// BigCache does not support per-entry TTL; uses global LifeWindow.
	return true, p.c.Set(key, value)
}

func (p *Provider) Del(_ context.Context, key string) error {
	return p.c.Delete(key)
}

func (p *Provider) Close(_ context.Context) error {
	return p.c.Close()
}
