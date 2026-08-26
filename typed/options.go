package typed

import (
	"errors"
	"fmt"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
)

// Options configure a [Cache]. Use keyed fields because later releases may add
// options.
type Options[K comparable, V any] struct {
	Config

	// KeyFunc must map distinct keys to distinct strings.
	KeyFunc func(K) string

	// Codec encodes and decodes cached values.
	Codec codec.Codec[V]

	// Backend is required by [New] and unused by [NewRedis]. It stays the
	// caller's to close.
	Backend backend.Backend

	// InvalidationTTL is used only by [NewRedis]. Zero uses the backend default;
	// [cascache.NoExpiration] keeps invalidation state forever. It must not be
	// shorter than MaxTTL.
	InvalidationTTL time.Duration

	// ComputeSetCost returns the admission cost of a stored frame. Nil uses 1.
	ComputeSetCost cascache.SetCostFunc

	// LoadTimeout bounds a shared loader run. Zero means no timeout; negative
	// values are invalid. Cancellation is cooperative, so loaders must honor the
	// context passed to them. After a timeout, later callers may start another
	// loader for the same key while the expired loader is still running.
	//
	// A run whose context ends before backend admission begins still returns its
	// value to waiting callers, but attempts no fill. A write already under way may
	// still commit; [backend.Backend] permits that.
	LoadTimeout time.Duration

	// Observer receives health events, alongside the ones Metrics consumes.
	Observer cascache.Observer
}

// Validate checks the fields every arrangement needs.
func (opts Options[K, V]) Validate() error {
	if err := opts.Config.Validate(); err != nil {
		return err
	}
	switch {
	case opts.KeyFunc == nil:
		return errors.New("typed: key func is required")
	case opts.Codec == nil:
		return errors.New("typed: codec is required")
	case opts.LoadTimeout < 0:
		return fmt.Errorf("typed: invalid load timeout %v", opts.LoadTimeout)
	}
	return nil
}

// Include the backend default when checking how long invalidation state lives.
func (opts Options[K, V]) checkInvalidationTTL(defaultTTL time.Duration) error {
	lifetime := opts.InvalidationTTL
	switch {
	case lifetime == cascache.NoExpiration:
		return nil
	case lifetime < 0:
		return fmt.Errorf("typed: invalid invalidation TTL %v", lifetime)
	case lifetime == 0:
		lifetime = defaultTTL
	}

	if lifetime > 0 && lifetime < opts.MaxTTL {
		return fmt.Errorf(
			"typed: invalidation TTL %v is shorter than max TTL %v, which would shorten cached values; "+
				"raise InvalidationTTL or lower MaxTTL",
			lifetime, opts.MaxTTL)
	}
	return nil
}

func (opts Options[K, V]) coreOptions(b backend.Backend) cascache.Options[V] {
	return cascache.Options[V]{
		Namespace:      opts.Namespace,
		Backend:        b,
		Codec:          opts.Codec,
		DefaultTTL:     opts.MaxTTL,
		ComputeTTL:     opts.ttl(),
		ComputeSetCost: opts.ComputeSetCost,
		LoadTimeout:    opts.LoadTimeout,
		Disabled:       opts.Disabled,
		Observer:       cascache.MultiObserver(opts.Observer, opts.Metrics.observer()),
		OnLoad:         opts.Metrics.loadFunc(),
	}
}
