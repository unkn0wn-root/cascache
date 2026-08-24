package backend

import (
	"context"
	"time"
)

// FenceStore is the advanced storage contract used by [Composite]. It holds the
// current fence for each key and may be shared by replicas whose values are
// stored locally. Implementations must be safe for concurrent use and follow
// the fence guarantees in [Backend].
//
// Writes refresh fence retention; reads do not. Value TTLs must be limited to
// [FenceStore.Lifetime] so a value cannot outlive its fence.
type FenceStore interface {
	// Ensure returns the current fence, installing candidate if none exists. It
	// refreshes retention without replacing a live fence.
	Ensure(ctx context.Context, key Key, candidate Fence) (Fence, error)

	// Read returns the current fence without refreshing retention.
	Read(ctx context.Context, key Key) (fence Fence, found bool, err error)

	// Retain refreshes retention only while the fence equals expected. It must
	// not recreate an expired fence.
	Retain(ctx context.Context, key Key, expected Fence) (current bool, err error)

	// Replace installs next unconditionally and refreshes retention.
	Replace(ctx context.Context, key Key, next Fence) error

	// Lifetime reports how long a fence is retained after a write. Zero means
	// fences do not expire. Callers clamp value TTLs to it; see [ClampTTL].
	Lifetime() time.Duration
}
