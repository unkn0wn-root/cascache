package typed

import (
	"errors"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
)

// Config holds settings used by [New] and [NewRedis].
type Config struct {
	// Namespace must be unique among caches sharing a backend.
	Namespace string

	// MaxTTL is the longest a cached value lives, and the TTL a write with no
	// TTL of its own gets.
	MaxTTL time.Duration

	// MinTTL is the shortest a jittered TTL may be. Zero lets jitter reach down
	// to nothing.
	MinTTL time.Duration

	// Jitter is a fraction from 0 to 1. At 0.2, TTLs vary over 20% of the range
	// between MaxTTL and MinTTL, spreading out expiry times.
	Jitter float64

	// Disabled makes the cache a pass-through.
	Disabled bool

	// Metrics contains optional cache metrics callbacks.
	Metrics Metrics
}

// Validate checks the required fields and TTL settings.
func (cfg Config) Validate() error {
	switch {
	case cfg.Namespace == "":
		return errors.New("typed: namespace is required")
	case cfg.MaxTTL <= 0:
		return errors.New("typed: max TTL must be positive")
	case cfg.MinTTL < 0:
		return errors.New("typed: min TTL must be zero or positive")
	case cfg.MinTTL > cfg.MaxTTL:
		return errors.New("typed: min TTL must not exceed max TTL")
	case cfg.Jitter < 0 || cfg.Jitter > 1:
		return errors.New("typed: jitter must be between 0 and 1")
	}
	return nil
}

func (cfg Config) ttl() cascache.TTLFunc {
	return cascache.JitterTTL(cfg.MaxTTL, cfg.MinTTL, cfg.Jitter)
}
