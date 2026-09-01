// Package bigcache adapts BigCache as a cascache value store.
package bigcache

import (
	"context"
	"errors"
	"time"

	bc "github.com/allegro/bigcache/v3"

	pr "github.com/unkn0wn-root/cascache/v4/provider"
)

// BigCache expires entries on one global LifeWindow and ignores the per-entry
// TTLs cascache passes to Set. Size the LifeWindow as the longest a value may
// be served for.
type BigCache struct {
	c *bc.BigCache
}

var _ pr.Store = (*BigCache)(nil)

// Config sizes the underlying cache and sets its one expiry window.
type Config struct {
	// LifeWindow is how long an entry lives. It applies to every entry.
	LifeWindow time.Duration

	// CleanWindow is how often expired entries are swept.
	CleanWindow time.Duration

	// MaxEntriesInWindow and MaxEntrySize size BigCache's initial allocation.
	// Zero uses BigCache's defaults.
	MaxEntriesInWindow int
	MaxEntrySize       int

	// HardMaxCacheSizeMB caps memory. Zero is unlimited.
	HardMaxCacheSizeMB int

	// Shards must be a power of two. Zero uses BigCache's default.
	Shards int
}

// New returns a BigCache-backed store. The context bounds only construction.
func New(ctx context.Context, cfg Config) (*BigCache, error) {
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
	if cfg.Shards > 0 {
		conf.Shards = cfg.Shards
	}
	c, err := bc.New(ctx, conf)
	if err != nil {
		return nil, err
	}
	return &BigCache{c: c}, nil
}

func (p *BigCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	b, err := p.c.Get(key)
	if errors.Is(err, bc.ErrEntryNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func (p *BigCache) Set(_ context.Context, key string, value []byte, _ int64, _ time.Duration) (bool, error) {
	if err := p.c.Set(key, value); err != nil {
		return false, err
	}
	return true, nil
}

func (p *BigCache) Del(_ context.Context, key string) error {
	// Deleting a missing key satisfies the Store contract.
	if err := p.c.Delete(key); err != nil && !errors.Is(err, bc.ErrEntryNotFound) {
		return err
	}
	return nil
}

func (p *BigCache) Close(_ context.Context) error {
	return p.c.Close()
}
