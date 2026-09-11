package backend

import (
	"context"
	"time"
)

// FenceStore holds current fences for [Composite] and follows [Backend]'s rules.
//
// Writes extend a fence's lifetime; reads do not. Limit value TTLs to
// [FenceStore.Lifetime] so values cannot outlive their fences.
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
