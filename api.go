package cascache

import (
	"context"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
)

// DefaultEntryTTL is used for writes that pass a zero TTL.
const DefaultEntryTTL = 10 * time.Minute

// NoExpiration asks for a value that never expires. A backend may shorten it so
// a value cannot outlive the invalidation state that judges it;
// [SetResult.EffectiveTTL] reports the applied TTL.
const NoExpiration = backend.NoExpiration

// Snapshot identifies the invalidation state of a key at one point in time.
// Take one with [Cache.Snapshot] before reading the source, then pass it to
// [Cache.Set]. An invalidation between those calls refuses the write.
//
// Snapshots are opaque. The zero Snapshot is invalid.
type Snapshot struct {
	_     [0]func() // Keep representation changes from becoming API breaks.
	fence backend.Fence
}

// SetCostFunc returns an entry's admission weight. raw is the full stored frame
// and must not be modified. Calls may run concurrently.
type SetCostFunc func(key string, raw []byte) int64

// TTLFunc computes the TTL for a fill. It must be safe for concurrent use.
type TTLFunc func() (time.Duration, error)

// Loader reads the source after a cache miss. Its context belongs to the shared
// loader run, not to one caller.
type Loader[V any] func(ctx context.Context) (V, error)

// SetOutcome reports what a cache write did.
type SetOutcome uint8

const (
	// SetOutcomeUnknown is the zero value and is never a successful write.
	SetOutcomeUnknown SetOutcome = iota
	// SetOutcomeStored means the value was stored.
	SetOutcomeStored
	// SetOutcomeConflict means the snapshot was no longer current at the write.
	SetOutcomeConflict
	// SetOutcomeBackendRejected means the backend declined the write.
	SetOutcomeBackendRejected
	// SetOutcomeDisabled means the cache is disabled and ignored the write.
	SetOutcomeDisabled
)

func (o SetOutcome) String() string {
	switch o {
	case SetOutcomeStored:
		return "stored"
	case SetOutcomeConflict:
		return "conflict"
	case SetOutcomeBackendRejected:
		return "backend_rejected"
	case SetOutcomeDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// SetResult reports what a cache write did. Only [SetOutcomeStored] is a fill.
// Use keyed fields because later releases may add data.
type SetResult struct {
	Outcome SetOutcome

	// EffectiveTTL is the applied TTL after defaults and backend limits. Zero
	// means the value does not expire.
	EffectiveTTL time.Duration
}

// Options configure a [Cache]. Namespace, Backend and Codec are required. Use
// keyed fields because later releases may add options.
type Options[V any] struct {
	// Namespace must be unique among caches sharing a backend.
	Namespace string

	// Backend belongs to the caller and is never closed by the cache.
	Backend backend.Backend

	// Codec encodes and decodes cached values.
	Codec codec.Codec[V]

	// DefaultTTL applies to writes that pass a zero TTL. Zero uses
	// [DefaultEntryTTL].
	DefaultTTL time.Duration

	// ComputeTTL sets the TTL of fills made by [Cache.Load]. Nil uses
	// DefaultTTL. See [JitterTTL].
	ComputeTTL TTLFunc

	// ComputeSetCost returns the admission cost of a stored frame. Nil uses 1.
	ComputeSetCost SetCostFunc

	// LoadTimeout bounds a shared loader run. Zero means no timeout.
	LoadTimeout time.Duration

	// Disabled makes the cache a pass-through.
	Disabled bool

	// OnLoad observes completed loads.
	OnLoad LoadFunc

	// Observer receives health events.
	Observer Observer
}

// Validate checks the required fields.
func (o Options[V]) Validate() error {
	switch {
	case o.Namespace == "":
		return ErrNoNamespace
	case isNil(o.Backend):
		return ErrNoBackend
	case isNil(o.Codec):
		return ErrNoCodec
	case o.DefaultTTL < 0 && o.DefaultTTL != NoExpiration:
		return ErrInvalidTTL
	}
	return nil
}
