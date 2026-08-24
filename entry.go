package cascache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/internal/wire"
)

type entry[V any] struct {
	val V
	ok  bool
}

// Resolve the public TTL rules to the backend form.
func (c *Cache[V]) resolveTTL(ttl time.Duration) (time.Duration, error) {
	if ttl == 0 {
		ttl = c.ttl
	}
	switch {
	case ttl == NoExpiration:
		return 0, nil
	case ttl < 0:
		return 0, ErrInvalidTTL
	default:
		return ttl, nil
	}
}

func (c *Cache[V]) prepareWrite(
	key string,
	val V,
	snapshot Snapshot,
	ttl time.Duration,
) (backend.StoreRequest, error) {
	if !snapshot.fence.Valid() {
		return backend.StoreRequest{}, ErrInvalidSnapshot
	}
	ttl, err := c.resolveTTL(ttl)
	if err != nil {
		return backend.StoreRequest{}, err
	}
	payload, err := c.codec.Encode(val)
	if err != nil {
		return backend.StoreRequest{}, err
	}

	// Store the fence with the value so reads can validate the pair.
	raw, err := wire.Encode(snapshot.fence, payload)
	if err != nil {
		return backend.StoreRequest{}, err
	}

	cost := c.cost(key, raw)
	if cost <= 0 {
		return backend.StoreRequest{}, fmt.Errorf("%w: %d", ErrInvalidCost, cost)
	}

	return backend.StoreRequest{
		Key:      c.inv.space.Key(key),
		Expected: snapshot.fence,
		Value:    raw,
		Cost:     cost,
		TTL:      ttl,
	}, nil
}

func (c *Cache[V]) setResult(key string, res backend.StoreResult) (SetResult, error) {
	switch res.Status {
	case backend.StoreStored:
		return SetResult{Outcome: SetOutcomeStored, EffectiveTTL: res.EffectiveTTL}, nil
	case backend.StoreConflict:
		return SetResult{Outcome: SetOutcomeConflict}, nil
	case backend.StoreRejected:
		c.observe(Event{Type: EventStoreRejected, Op: OpSet, Key: key})
		return SetResult{Outcome: SetOutcomeBackendRejected}, nil
	default:
		return SetResult{}, c.opErr(OpSet, key, ErrBackendContract)
	}
}

// Decode and validate a read. Invalid entries become misses and are removed
// when safe.
func (c *Cache[V]) decode(ctx context.Context, key string, bkey backend.Key, r backend.ReadResult) entry[V] {
	if !r.Found {
		return entry[V]{}
	}

	fence, payload, err := wire.Decode(r.Value)
	switch {
	case errors.Is(err, wire.ErrUnsupportedFormat):
		// Leave entries from another format for the build that understands them.
		c.observe(Event{
			Type:   EventEntryRejected,
			Op:     OpGet,
			Key:    key,
			Reason: RejectUnsupportedFormat,
			Err:    err,
		})
		return entry[V]{}
	case err != nil:
		return c.reject(ctx, key, bkey, r.Value, RejectFrameCorrupt, err)
	}

	// A value without a fence cannot be proved current.
	if !r.FenceFound {
		return c.reject(ctx, key, bkey, r.Value, RejectStateMissing, nil)
	}
	if !fence.Equal(r.Fence) {
		return c.reject(ctx, key, bkey, r.Value, RejectRetired, nil)
	}

	val, err := c.codec.Decode(payload)
	if err != nil {
		return c.reject(ctx, key, bkey, r.Value, RejectValueDecode, err)
	}
	return entry[V]{val: val, ok: true}
}

// Report an unusable entry and remove those exact bytes if they are unchanged.
func (c *Cache[V]) reject(
	ctx context.Context,
	key string,
	bkey backend.Key,
	raw []byte,
	reason RejectReason,
	cause error,
) entry[V] {
	c.observe(Event{Type: EventEntryRejected, Op: OpGet, Key: key, Reason: reason, Err: cause})

	if _, err := c.inv.backend.Discard(ctx, bkey, raw); err != nil {
		c.observe(Event{Type: EventCleanupFailed, Op: OpGet, Key: key, Reason: reason, Err: err})
	}
	return entry[V]{}
}
