package backend

import (
	"context"
	"time"
)

// NoExpiration asks for a value or a fence that never expires.
const NoExpiration time.Duration = -1

// ReadResult is one backend read. Value is undefined when Found is false.
type ReadResult struct {
	// Value is the stored frame. Callers must not modify it.
	Value []byte
	Found bool

	// Fence is the key's current fence.
	Fence Fence

	// FenceFound reports whether Read found the key's current fence.
	// A value without a fence must not be served. On a miss, reporting the
	// fence is optional; fills may use it instead of Ensure.
	FenceFound bool
}

// StoreStatus reports what a conditional write did.
type StoreStatus uint8

const (
	// StoreUnknown is the zero value and is never a successful outcome.
	StoreUnknown StoreStatus = iota
	// StoreStored means the expected fence was current and the value was stored.
	StoreStored
	// StoreConflict means the expected fence was not current, or no longer
	// exists. The value was not stored.
	StoreConflict
	// StoreRejected means an admission policy declined a write whose expected
	// fence was current.
	StoreRejected
)

func (s StoreStatus) String() string {
	switch s {
	case StoreStored:
		return "stored"
	case StoreConflict:
		return "conflict"
	case StoreRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// StoreRequest is one conditional write. A nonpositive TTL means no expiry,
// subject to the fence lifetime.
type StoreRequest struct {
	Key Key

	// Expected is the fence the value was loaded under. The write happens only
	// while it is still current.
	Expected Fence

	// Value is the frame to store. Callers must not modify it afterwards.
	Value []byte

	// Cost is the admission weight. Backends without admission control ignore it.
	Cost int64

	// TTL is the requested lifetime. A nonpositive value means no expiry.
	TTL time.Duration
}

// StoreResult reports a conditional write.
type StoreResult struct {
	Status StoreStatus

	// EffectiveTTL is the TTL actually applied, which may be shorter than the
	// one requested. Zero means the value does not expire.
	EffectiveTTL time.Duration
}

// InvalidateResult reports an invalidation. CleanupErr means the old value was
// not deleted, but its changed fence still makes it unreadable.
type InvalidateResult struct {
	CleanupErr error
}

// Backend stores values and their fences.
//
// Read and CompareAndStore must see invalidations completed before they start.
// An old fence must never become current again. Values and fences may live in
// separate stores, but a race between stores must not allow stale reads.
//
// Errors and cancellation do not guarantee rollback. Backends do not interpret
// value bytes. Neither side may change the bytes after passing them to the other.
type Backend interface {
	// Read returns the value and the key's current fence. It must read the value
	// before or at the same time as the fence.
	Read(context.Context, Key) (ReadResult, error)

	// Ensure returns the current fence, installing candidate only if none exists.
	// Concurrent calls for a missing fence must agree. It refreshes the fence's
	// lifetime without replacing it.
	Ensure(context.Context, Key, Fence) (Fence, error)

	// CompareAndStore stores a value only while its expected fence is current.
	// Check the fence before admission policy so StoreRejected also proves it
	// was current.
	CompareAndStore(context.Context, StoreRequest) (StoreResult, error)

	// Invalidate installs a new fence and then removes the value. Failing to
	// remove the value must not restore the old fence.
	Invalidate(context.Context, Key, Fence) (InvalidateResult, error)

	// Discard removes a value only while its bytes equal rejected. It reports
	// whether it removed anything.
	Discard(context.Context, Key, []byte) (bool, error)
}

// ClampTTL limits a value TTL to its fence lifetime. A nonpositive lifetime
// means fences do not expire.
func ClampTTL(ttl, lifetime time.Duration) time.Duration {
	if lifetime <= 0 {
		return max(ttl, 0)
	}
	if ttl <= 0 {
		return lifetime
	}
	return min(ttl, lifetime)
}
