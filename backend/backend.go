package backend

import (
	"context"
	"time"
)

// NoExpiration asks for a value or a fence that never expires.
const NoExpiration time.Duration = -1

// ReadResult is one backend read. Other fields are undefined when Found is false.
type ReadResult struct {
	// Value is the stored frame. Callers must not modify it.
	Value []byte
	Found bool

	// Fence is the authoritative fence for the key.
	Fence Fence

	// FenceFound reports whether the key has a fence. A value without one must
	// not be served.
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
// # Required guarantees
//
// Implementations must guarantee:
//
//  1. Concurrent Ensure calls for a key without a fence all return the same
//     fence.
//  2. A fence installed by Invalidate is visible to every Read and
//     CompareAndStore that starts afterwards.
//  3. A retired fence never becomes current again.
//  4. Discard removes a value only while its bytes still equal the ones the
//     caller judged invalid.
//  5. Read observes the value no later than the fence.
//  6. CompareAndStore checks the fence before admission policy, so
//     StoreRejected still proves the expected fence was current.
//
// # What is not required
//
// The value and fence may live in separate stores. A read serves a value only
// when its stamped fence is current, so a cross-store race may waste work but
// must not return stale data.
//
// # Errors and ownership
//
// An error or canceled context does not imply rollback. Backends treat values
// as opaque bytes, and neither side modifies a value after passing it across
// this interface.
type Backend interface {
	// Read returns the value and the fence that judges it.
	Read(context.Context, Key) (ReadResult, error)

	// Ensure returns the current fence for a key, installing candidate when
	// none exists. It refreshes fence retention but never replaces a fence
	// that is already there.
	Ensure(context.Context, Key, Fence) (Fence, error)

	// CompareAndStore stores a value only while its expected fence is current.
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
