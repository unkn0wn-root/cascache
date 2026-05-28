package cascache

import (
	"context"
	"time"

	"github.com/unkn0wn-root/cascache/v3/internal/keys"
	"github.com/unkn0wn-root/cascache/v3/internal/wire"
	"github.com/unkn0wn-root/cascache/v3/version"
)

type singleWrite struct {
	storageKey string
	wire       []byte
	cost       int64
}

// Get looks up a single key and returns its value only if the cached entry is
// still usable.
//
// The read passes through three validation gates:
//  1. Wire: the raw bytes must parse as a valid single-entry frame.
//  2. Version: the embedded fence must match the current
//     authoritative fence in the version store.
//  3. Codec: the payload must decode with the configured codec.
//
// Failures in (1), (2), and (3) delete the provider entry and return a miss.
// If the current authoritative fence cannot be loaded, Get returns a miss without
// serving or deleting the cached value. Provider read failures are returned
// as *OpError with OpGet.
func (c *cache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var zero V
	if !c.enabled {
		return zero, false, nil
	}

	sk := c.singleKeys(key)
	storageKey := sk.Value.String()
	ckey := toVersionCacheKey(sk.Cache)

	if c.keyReader != nil {
		kr, err := c.keyReader.ReadKey(ctx, ckey, storageKey)
		if err != nil {
			return zero, false, opError(OpGet, key, err)
		}
		if !kr.Found {
			return zero, false, nil
		}
		return c.serveSingleRaw(ctx, key, storageKey, kr.Raw, kr.Snapshot, kr.SnapshotErr)
	}

	raw, ok, err := c.provider.Get(ctx, storageKey)
	if err != nil {
		return zero, false, opError(OpGet, key, err)
	}
	if !ok {
		return zero, false, nil
	}

	return c.serveSingleRawWithSnapshotLoad(ctx, key, storageKey, raw, ckey)
}

// SnapshotVersion returns the current version for one logical key.
func (c *cache[V]) SnapshotVersion(ctx context.Context, key string) (Version, error) {
	snap, err := c.loadSnapshot(ctx, c.versionKey(key))
	if err != nil {
		return Version{}, opError(OpSnapshot, key, err)
	}
	return versionFromSnapshot(snap), nil
}

// SetIfVersion writes a value only when the current version still matches the
// caller's observed version using the cache's configured default TTL.
func (c *cache[V]) SetIfVersion(
	ctx context.Context,
	key string,
	value V,
	version Version,
) (WriteResult, error) {
	return c.SetIfVersionWithTTL(ctx, key, value, version, c.defaultTTL)
}

// SetIfVersionWithTTL writes a value only when the current version still
// matches the caller's observed version. Generic backends perform a final
// version read immediately before the provider write. When KeyWriter is
// configured, the backend-native implementation collapses compare and write
// into one operation.
func (c *cache[V]) SetIfVersionWithTTL(
	ctx context.Context,
	key string,
	value V,
	version Version,
	ttl time.Duration,
) (WriteResult, error) {
	if !c.enabled {
		return WriteResult{Outcome: WriteOutcomeDisabled}, nil
	}
	if ttl == 0 {
		ttl = c.defaultTTL
	}

	payload, err := c.codec.Encode(value)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}

	sk := c.singleKeys(key)
	ckey := toVersionCacheKey(sk.Cache)

	if c.keyWriter != nil {
		s, kerr := c.keyWriter.SetIfVersion(
			ctx,
			ckey,
			sk.Value.String(),
			version.snapshot(),
			payload,
			ttl,
		)
		if kerr != nil {
			return WriteResult{}, opError(OpSet, key, kerr)
		}
		if !s {
			return WriteResult{Outcome: WriteOutcomeVersionMismatch}, nil
		}
		return WriteResult{Outcome: WriteOutcomeStored}, nil
	}

	snap, ok, err := c.checkSnapshot(ctx, ckey, version)
	if err != nil {
		return WriteResult{Outcome: WriteOutcomeSnapshotError}, opError(OpSnapshot, key, err)
	}
	if !ok {
		return WriteResult{Outcome: WriteOutcomeVersionMismatch}, nil
	}
	if !version.IsMissing() {
		var refreshed bool
		refreshed, err = c.refreshVersion(ctx, ckey)
		if err != nil {
			return WriteResult{Outcome: WriteOutcomeSnapshotError}, opError(OpSnapshot, key, err)
		}
		if !refreshed {
			return WriteResult{Outcome: WriteOutcomeVersionMismatch}, nil
		}
	}

	sw, err := c.buildSingleWrite(sk, snap.Fence, payload)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}

	s, err := c.setSingle(ctx, sw, ttl)
	if err != nil {
		return WriteResult{}, opError(OpSet, key, err)
	}
	if !s {
		return WriteResult{Outcome: WriteOutcomeProviderRejected}, nil
	}
	return WriteResult{Outcome: WriteOutcomeStored}, nil
}

// Invalidate marks a key as stale so that future readers will not serve it.
// Backends perform two operations in a specific order:
//
//  1. Advance the authoritative fence in the version store, which makes every
//     existing cached entry for this key stale because its embedded fence
//     will no longer match.
//
//  2. Delete the single entry from the provider as a courtesy so the stale
//     bytes do not linger and waste memory.
//
// The order matters. Advancing first ensures that even if the delete fails,
// readers will see the fence mismatch and self-heal on the next read.
// If we deleted first and the advance then failed, a batch entry could reseed
// the single with stale data and the fence check would still pass.
func (c *cache[V]) Invalidate(ctx context.Context, key string) error {
	if !c.enabled {
		return nil
	}

	sk := c.singleKeys(key)
	if c.keyInvalidator != nil {
		if err := c.keyInvalidator.Invalidate(
			ctx,
			toVersionCacheKey(sk.Cache),
			sk.Value.String(),
		); err != nil {
			c.hooks.InvalidateOutage(key, err, nil)
			return &InvalidateError{
				Key:        key,
				AdvanceErr: opError(OpInvalidate, key, err),
			}
		}
		return nil
	}

	_, bErr := c.advanceVersion(ctx, toVersionCacheKey(sk.Cache))
	delErr := c.provider.Del(ctx, sk.Value.String())

	if bErr != nil {
		c.hooks.InvalidateOutage(key, bErr, delErr)
		return &InvalidateError{
			Key:        key,
			AdvanceErr: opError(OpInvalidate, key, bErr),
			DelErr:     opError(OpInvalidate, key, delErr),
		}
	}
	return nil
}

func (c *cache[V]) serveSingleRaw(
	ctx context.Context,
	key string,
	storageKey string,
	raw []byte,
	snap version.Snapshot,
	snapErr error,
) (V, bool, error) {
	var zero V

	dfence, payload, ok := c.decodeSingleRaw(ctx, storageKey, raw)
	if !ok {
		return zero, false, nil
	}

	if snapErr != nil {
		c.hooks.VersionSnapshotError(1, snapErr)
		return zero, false, nil
	}
	return c.serveSingleDecoded(ctx, key, storageKey, dfence, payload, snap)
}

func (c *cache[V]) serveSingleRawWithSnapshotLoad(
	ctx context.Context,
	key string,
	storageKey string,
	raw []byte,
	ckey version.CacheKey,
) (V, bool, error) {
	var zero V

	dfence, payload, ok := c.decodeSingleRaw(ctx, storageKey, raw)
	if !ok {
		return zero, false, nil
	}

	snap, err := c.loadSnapshot(ctx, ckey)
	if err != nil {
		return zero, false, nil
	}
	return c.serveSingleDecoded(ctx, key, storageKey, dfence, payload, snap)
}

func (c *cache[V]) decodeSingleRaw(
	ctx context.Context,
	storageKey string,
	raw []byte,
) (version.Fence, []byte, bool) {
	dfence, payload, err := wire.DecodeSingle(raw)
	if err != nil {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonCorrupt)
		return version.Fence{}, nil, false
	}
	return dfence, payload, true
}

func (c *cache[V]) serveSingleDecoded(
	ctx context.Context,
	key string,
	storageKey string,
	dfence version.Fence,
	payload []byte,
	snap version.Snapshot,
) (V, bool, error) {
	var zero V

	if !snap.Exists {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonVersionMissing)
		return zero, false, nil
	}
	if !dfence.Equal(snap.Fence) {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonVersionMismatch)
		return zero, false, nil
	}

	v, err := c.codec.Decode(payload)
	if err != nil {
		c.selfHealSingle(ctx, storageKey, SelfHealReasonValueDecode)
		return zero, false, nil
	}
	if guardReason := c.guardSingleRead(ctx, key, v); guardReason != "" {
		c.selfHealSingle(ctx, storageKey, guardReason)
		return zero, false, nil
	}
	return v, true, nil
}

// selfHealSingle deletes one unusable single entry and emits the matching
// hook reason. Read paths call this after conservative validation failures.
func (c *cache[V]) selfHealSingle(ctx context.Context, storageKey string, reason SelfHealReason) {
	_ = c.provider.Del(ctx, storageKey)
	c.hooks.SelfHealSingle(storageKey, reason)
}

// buildSingleWrite builds the provider payload and admission metadata for a
// single entry write from an already encoded value payload.
func (c *cache[V]) buildSingleWrite(
	sk keys.Single,
	fence version.Fence,
	payload []byte,
) (singleWrite, error) {
	wireb, err := wire.EncodeSingle(fence, payload)
	if err != nil {
		return singleWrite{}, err
	}

	sKey := sk.Value.String()
	return singleWrite{
		storageKey: sKey,
		wire:       wireb,
		cost:       c.computeSetCost(sKey, wireb, false, 1),
	}, nil
}

// singleKeys translates a caller-facing logical key into both the canonical
// single-key identity used by the version store and the provider value key.
// Example: logical key "42" in namespace "user" becomes:
//   - Cache: "s:4:user:42"
//   - Value: "cas:v3:val:{<hash>}:s:4:user:42"
//
// The {<hash>} is a deterministic prefix derived from the cache key.
func (c *cache[V]) singleKeys(userKey string) keys.Single {
	return c.space.Single(userKey)
}

// guardSingleRead applies the configured single-key read guard and translates
// its result into the self-heal reason Get should record on rejection.
func (c *cache[V]) guardSingleRead(ctx context.Context, key string, value V) SelfHealReason {
	if c.readGuard == nil {
		return ""
	}

	ok, err := c.readGuard(ctx, key, value)
	if err != nil {
		return SelfHealReasonReadGuardError
	}
	if !ok {
		return SelfHealReasonReadGuardReject
	}
	return ""
}

// addSingle encodes one single-entry frame from an already validated batch
// member and inserts it only if the provider reports the key as missing.
func (c *cache[V]) addSingle(
	ctx context.Context,
	key string,
	fence version.Fence,
	payload []byte,
	ttl time.Duration,
) error {
	sk := c.singleKeys(key)
	sw, err := c.buildSingleWrite(sk, fence, payload)
	if err != nil {
		return err
	}
	_, err = c.adder.Add(ctx, sw.storageKey, sw.wire, sw.cost, ttl)
	return err
}

// setSingle executes a prepared single-entry provider Set and reports
// admission rejection through hooks without treating it as an error.
func (c *cache[V]) setSingle(ctx context.Context, sw singleWrite, ttl time.Duration) (bool, error) {
	ok, err := c.provider.Set(ctx, sw.storageKey, sw.wire, sw.cost, ttl)
	if err != nil {
		return false, err
	}
	if !ok {
		c.hooks.ProviderSetRejected(sw.storageKey, false)
	}
	return ok, nil
}

// writeSingle encodes one single entry frame from an already validated batch
// member and stores it through the normal provider Set path.
func (c *cache[V]) writeSingle(
	ctx context.Context,
	key string,
	fence version.Fence,
	payload []byte,
	ttl time.Duration,
) error {
	sk := c.singleKeys(key)
	sw, err := c.buildSingleWrite(sk, fence, payload)
	if err != nil {
		return err
	}
	_, err = c.setSingle(ctx, sw, ttl)
	return err
}
