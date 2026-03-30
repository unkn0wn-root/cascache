package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v3"
	c "github.com/unkn0wn-root/cascache/v3/codec"
)

// Options configures the full Redis-backed cache.
// Single-key writes and invalidation use backend-native Redis scripts.
// Batch entries remain validated-on-read rather than globally atomic.
type Options[V any] struct {
	Namespace string
	Client    goredis.UniversalClient
	Codec     c.Codec[V]

	DefaultTTL time.Duration
	BatchTTL   time.Duration
	VersionTTL time.Duration // 0 keeps authoritative version state indefinitely
	Disabled   bool

	ComputeSetCost cascache.SetCostFunc
	DisableBatch   bool
	ReadGuard      cascache.ReadGuardFunc[V]
	BatchReadGuard cascache.BatchReadGuardFunc[V]
	BatchReadSeed  cascache.BatchReadSeedMode
	BatchWriteSeed cascache.BatchWriteSeedMode
	Hooks          cascache.Hooks
	CloseClient    bool
}

// New constructs the preferred full Redis-backed cache from one shared client.
func New[V any](opts Options[V]) (cascache.CAS[V], error) {
	if opts.Client == nil {
		return nil, ErrNilClient
	}

	pr, err := NewProviderWithOptions(ProviderOptions{
		Client:      opts.Client,
		CloseClient: opts.CloseClient,
	})
	if err != nil {
		return nil, err
	}

	ver, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:     opts.Client,
		VersionTTL: opts.VersionTTL,
	})
	if err != nil {
		_ = pr.Close(context.Background())
		return nil, err
	}

	mutator, err := NewKeyMutatorWithOptions(KeyMutatorOptions{
		Client:     opts.Client,
		VersionTTL: opts.VersionTTL,
	})
	if err != nil {
		_ = ver.Close(context.Background())
		_ = pr.Close(context.Background())
		return nil, err
	}

	// Let the provider own client shutdown so cache.Close closes the shared
	// client exactly once when CloseClient is enabled.
	cache, err := cascache.New(cascache.Options[V]{
		Namespace:      opts.Namespace,
		Provider:       pr,
		Codec:          opts.Codec,
		DefaultTTL:     opts.DefaultTTL,
		BatchTTL:       opts.BatchTTL,
		Disabled:       opts.Disabled,
		ComputeSetCost: opts.ComputeSetCost,
		VersionStore:   ver,
		KeyWriter:      mutator,
		KeyInvalidator: mutator,
		DisableBatch:   opts.DisableBatch,
		ReadGuard:      opts.ReadGuard,
		BatchReadGuard: opts.BatchReadGuard,
		BatchReadSeed:  opts.BatchReadSeed,
		BatchWriteSeed: opts.BatchWriteSeed,
		Hooks:          opts.Hooks,
	})
	if err != nil {
		_ = ver.Close(context.Background())
		_ = pr.Close(context.Background())
		return nil, err
	}
	return cache, nil
}
