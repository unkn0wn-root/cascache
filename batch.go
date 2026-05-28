package cascache

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	keyutil "github.com/unkn0wn-root/cascache/v3/internal/keys"
	"github.com/unkn0wn-root/cascache/v3/internal/wire"
	"github.com/unkn0wn-root/cascache/v3/version"
)

type batchReadAction uint8

const (
	batchReadServeAll batchReadAction = iota
	batchReadServeAcceptedMissRejected
	batchReadServeAcceptedRefetchRejected
	batchReadFallbackSingles
	batchReadMissAll
)

type batchReadPlan struct {
	action   batchReadAction
	reason   BatchRejectReason
	rejected map[string]struct{}
}

type batchHit[V any] struct {
	storageKey string
	items      map[string]wire.BatchItem
	values     map[string]V
}

type batchWriteItem[V any] struct {
	key string
	val V
	obs Version
}

type batchProjection struct {
	missing      []string
	fallbackKeys []string
}

type batchReadGuardResult struct {
	reason   BatchRejectReason
	rejected map[string]struct{}
}

// singleSeedFunc writes one validated batch member as a single entry.
type singleSeedFunc func(ctx context.Context, key string, fence version.Fence, payload []byte, ttl time.Duration) error

// GetMany retrieves multiple keys in one call, trying the most efficient
// path first and falling back as needed:
//
//  1. Look up the combined batch entry whose storage key is derived from the
//     sorted, deduplicated set of requested keys. If it exists, is valid on
//     the wire level, and every member passes the authoritative fence check, decode
//     only the requested items and return them.
//
//     Successful batch hits may also materialize the validated members as
//     individual single entries depending on BatchReadSeed. This warming is
//     optional and best-effort.
//
//  2. If the batch entry is missing, corrupt, stale, or contains items that
//     fail to decode, delete it from the provider to prevent
//     repeated failures on subsequent reads.
//
//  3. Fall back to per-key reads via Get. Results are memoized internally so
//     duplicate keys in the input do not cause duplicate lookups. If
//     BatchReadGuard rejects specific members and no ReadGuard is configured,
//     those rejected members are reported as misses for this call instead of
//     being served back from seeded singles. If BatchReadGuard errors and no
//     ReadGuard is configured, the entire batch fails closed to misses.
//
// When batch mode is disabled (DisableBatch option), step 1 is skipped
// entirely and we go straight to per-key reads.
func (c *cache[V]) GetMany(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	missing := make([]string, 0, len(keys))

	if !c.enabled {
		missing = append(missing, keys...)
		return out, missing, nil
	}

	if len(keys) == 0 {
		return out, nil, nil
	}

	if !c.batchEnabled {
		m, err := c.readSingles(ctx, keys, out)
		return out, m, err
	}

	us := sortedUnique(keys)
	hit, ok, err := c.loadBatchHit(ctx, us)
	if err != nil {
		return out, missing, opError(OpGetMany, "", err)
	}
	if !ok {
		missing, err = c.readSingles(ctx, keys, out)
		return out, missing, err
	}

	plan := c.buildBatchReadPlan(ctx, hit.values)
	if plan.action != batchReadServeAll {
		c.rejectBatch(ctx, hit.storageKey, len(us), plan.reason)
	}
	missing, err = c.applyBatchReadPlan(ctx, keys, us, hit, plan, out)
	return out, missing, err
}

// SnapshotVersions returns the current version for each unique logical key.
func (c *cache[V]) SnapshotVersions(ctx context.Context, keys []string) (map[string]Version, error) {
	keys = sortedUnique(keys)
	ss, err := c.loadSnapshots(ctx, keys)
	if err != nil {
		return nil, err
	}

	out := make(map[string]Version, len(keys))
	for i, key := range keys {
		out[key] = versionFromSnapshot(ss[i])
	}
	return out, nil
}

// SetIfVersions writes one batch entry when every version still matches using
// the cache's configured default single and batch TTLs.
func (c *cache[V]) SetIfVersions(
	ctx context.Context,
	items []VersionedValue[V],
) (BatchWriteResult, error) {
	return c.SetIfVersionsWithTTL(ctx, items, 0)
}

// SetIfVersionsWithTTL writes one batch entry when every version still
// matches. If the batch write is skipped or rejected, the cache falls back to
// checked single writes and reports that through the result.
func (c *cache[V]) SetIfVersionsWithTTL(
	ctx context.Context,
	items []VersionedValue[V],
	ttl time.Duration,
) (BatchWriteResult, error) {
	if !c.enabled {
		return BatchWriteResult{Outcome: WriteOutcomeDisabled}, nil
	}

	if len(items) == 0 {
		return BatchWriteResult{Outcome: WriteOutcomeStored}, nil
	}

	ws, ks, err := prepBatchWrite(items)
	if err != nil {
		return BatchWriteResult{}, err
	}

	sttl := ttl
	if sttl == 0 {
		sttl = c.defaultTTL
	}
	if !c.batchEnabled {
		return c.fallbackSet(ctx, WriteOutcomeDisabled, ws, sttl)
	}

	ss, err := c.loadSnapshots(ctx, ks)
	if err != nil {
		return c.fallbackSet(ctx, WriteOutcomeSnapshotError, ws, sttl)
	}
	for i := range ws {
		if !snapshotMatchesVersion(ss[i], ws[i].obs) {
			return c.fallbackSet(ctx, WriteOutcomeVersionMismatch, ws, sttl)
		}
	}
	for i := range ws {
		if !ws[i].obs.IsMissing() {
			continue
		}

		snap, created, createErr := c.createSnapshot(ctx, c.versionKey(ws[i].key))
		if createErr != nil {
			return c.fallbackSet(ctx, WriteOutcomeSnapshotError, ws, sttl)
		}
		if !created {
			return c.fallbackSet(ctx, WriteOutcomeVersionMismatch, ws, sttl)
		}
		ws[i].obs = versionFromSnapshot(snap)
	}

	bttl := ttl
	if bttl == 0 {
		bttl = c.batchTTL
	}

	wires := make([]wire.BatchItem, 0, len(ws))
	for _, w := range ws {
		payload, eErr := c.codec.Encode(w.val)
		if eErr != nil {
			return BatchWriteResult{}, opError(OpSetIfVersions, w.key, eErr)
		}
		wires = append(wires, wire.BatchItem{
			Key:     w.key,
			Fence:   w.obs.fence,
			Payload: payload,
		})
	}

	wireb, err := wire.EncodeBatch(wires)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}

	bk, err := c.batchKeySorted(ks)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}

	ok, err := c.provider.Set(
		ctx,
		bk.String(),
		wireb,
		c.computeSetCost(bk.String(), wireb, true, len(ws)),
		bttl,
	)
	if err != nil {
		return BatchWriteResult{}, opError(OpSetIfVersions, "", err)
	}
	if !ok {
		c.hooks.ProviderSetRejected(bk.String(), true)
		return c.fallbackSet(ctx, WriteOutcomeProviderRejected, ws, sttl)
	}

	if err := c.seedAfterBatch(ctx, ws, wires, sttl); err != nil {
		return BatchWriteResult{Outcome: WriteOutcomeStored}, err
	}

	return BatchWriteResult{Outcome: WriteOutcomeStored}, nil
}

// fallbackSet records that the batch path did not land and retries the write
// through checked single-key writes.
func (c *cache[V]) fallbackSet(
	ctx context.Context,
	outcome WriteOutcome,
	items []batchWriteItem[V],
	ttl time.Duration,
) (BatchWriteResult, error) {
	return BatchWriteResult{
		Outcome:       outcome,
		SeededSingles: true,
	}, c.seedSingles(ctx, items, ttl)
}

// loadBatch reads authoritative state for multiple canonical keys in one batch call.
func (c *cache[V]) loadBatch(ctx context.Context, cacheKeys []version.CacheKey) (map[version.CacheKey]version.Snapshot, error) {
	if len(cacheKeys) == 0 {
		return map[version.CacheKey]version.Snapshot{}, nil
	}

	snaps, err := c.versionStore.SnapshotMany(ctx, cacheKeys)
	if err != nil {
		c.hooks.VersionSnapshotError(len(cacheKeys), err)
		return nil, err
	}
	return snaps, nil
}

// loadSnapshots returns authoritative state for keys in the same order.
// keys must already be sorted and deduplicated.
func (c *cache[V]) loadSnapshots(ctx context.Context, keys []string) ([]version.Snapshot, error) {
	if len(keys) == 0 {
		return []version.Snapshot{}, nil
	}

	ck := c.versionKeys(keys)
	m, err := c.loadBatch(ctx, ck)
	if err == nil {
		out := make([]version.Snapshot, len(ck))
		for i, k := range ck {
			out[i] = m[k]
		}
		return out, nil
	}
	return c.loadFallback(ctx, keys, ck)
}

// loadFallback performs per-key snapshot reads after a batch snapshot fails.
// Unreadable keys are mapped to missing snapshots and returned errors are
// wrapped per logical key so strict callers can fail the whole snapshot.
func (c *cache[V]) loadFallback(
	ctx context.Context,
	keys []string,
	cacheKeys []version.CacheKey,
) ([]version.Snapshot, error) {
	out := make([]version.Snapshot, len(keys))
	errs := make([]error, 0, len(keys))
	for i, k := range keys {
		s, err := c.loadSnapshot(ctx, cacheKeys[i])
		if err != nil {
			out[i] = version.Snapshot{}
			errs = append(errs, &OpError{Op: OpSnapshot, Key: k, Err: err})
			continue
		}
		out[i] = s
	}
	return out, errors.Join(errs...)
}

// loadBatchHit reads, validates, and decodes a batch entry for one unique key
// set. Corrupt, stale, or undecodable entries are self-healed here so callers
// can treat a false hit as a normal fallback-to-singles condition.
func (c *cache[V]) loadBatchHit(ctx context.Context, sortedRequested []string) (batchHit[V], bool, error) {
	bk, err := c.batchKeySorted(sortedRequested)
	if err != nil {
		return batchHit[V]{}, false, err
	}
	sk := bk.String()

	raw, ok, err := c.provider.Get(ctx, sk)
	if err != nil {
		return batchHit[V]{}, false, err
	}
	if !ok {
		return batchHit[V]{}, false, nil
	}

	it, err := wire.DecodeBatch(raw)
	if err != nil {
		c.rejectBatch(ctx, sk, len(sortedRequested), BatchRejectReasonDecodeError)
		return batchHit[V]{}, false, nil
	}

	bm := indexBatch(it)
	reason, err := c.batchRejectByKey(ctx, sortedRequested, bm)
	if err != nil {
		return batchHit[V]{}, false, nil
	}
	if reason != "" {
		c.rejectBatch(ctx, sk, len(sortedRequested), reason)
		return batchHit[V]{}, false, nil
	}

	dec, err := c.decodeBatch(sortedRequested, bm)
	if err != nil {
		c.rejectBatch(ctx, sk, len(sortedRequested), BatchRejectReasonValueDecode)
		return batchHit[V]{}, false, nil
	}

	return batchHit[V]{
		storageKey: sk,
		items:      bm,
		values:     dec,
	}, true, nil
}

// versionKeys is the slice form of versionKey for callers that already
// have a sorted, deduplicated logical key set.
func (c *cache[V]) versionKeys(keys []string) []version.CacheKey {
	ck := make([]version.CacheKey, len(keys))
	for i, k := range keys {
		ck[i] = c.versionKey(k)
	}
	return ck
}

// batchRejectReason reports whether a stored batch entry can serve the requested
// keys. For each requested key it verifies two things: the key must be present
// in the batch, and its stored fence must match the current authoritative fence
// in the version store.
//
// Extra keys present in the batch but not in the requested set are ignored. A
// non-empty return means the entry should be rejected for this read.
func (c *cache[V]) batchRejectReason(
	ctx context.Context,
	sortedRequested []string,
	items []wire.BatchItem,
) (BatchRejectReason, error) {
	return c.batchRejectByKey(ctx, sortedRequested, indexBatch(items))
}

func (c *cache[V]) batchRejectByKey(
	ctx context.Context,
	sortedRequested []string,
	items map[string]wire.BatchItem,
) (BatchRejectReason, error) {
	ss, err := c.loadSnapshots(ctx, sortedRequested)
	if err != nil {
		return "", err
	}

	for i, k := range sortedRequested {
		it, ok := items[k]
		if !ok {
			return BatchRejectReasonIncompleteBatch, nil
		}
		snap := ss[i]
		if !snap.Exists {
			return BatchRejectReasonVersionMissing, nil
		}
		if !it.Fence.Equal(snap.Fence) {
			return BatchRejectReasonVersionMismatch, nil
		}
	}
	return "", nil
}

// buildBatchReadPlan translates guard outcomes into one internal action so
// GetMany can keep policy decisions separate from result projection and I/O.
func (c *cache[V]) buildBatchReadPlan(ctx context.Context, values map[string]V) batchReadPlan {
	guard := c.guardBatchRead(ctx, values)
	if guard.allowed() {
		return batchReadPlan{action: batchReadServeAll}
	}

	plan := batchReadPlan{
		reason:   guard.reason,
		rejected: guard.rejected,
	}
	switch {
	case c.batchReadGuard != nil && len(guard.rejected) != 0 && c.readGuard == nil:
		plan.action = batchReadServeAcceptedMissRejected
	case c.batchReadGuard != nil && len(guard.rejected) != 0 && c.readGuard != nil:
		plan.action = batchReadServeAcceptedRefetchRejected
	case c.batchReadGuard != nil && c.readGuard == nil:
		plan.action = batchReadMissAll
	default:
		plan.action = batchReadFallbackSingles
	}
	return plan
}

// applyBatchReadPlan materializes a previously chosen batch-read action back
// onto the caller's requested key order, including warming or single-key
// fallback when the plan requires it.
func (c *cache[V]) applyBatchReadPlan(
	ctx context.Context,
	keys, sortedRequested []string,
	hit batchHit[V],
	plan batchReadPlan,
	out map[string]V,
) ([]string, error) {
	switch plan.action {
	case batchReadServeAll:
		p := projectBatchValues(keys, hit.values, nil, out, false)
		c.seedBatchRead(ctx, sortedRequested, hit.items)
		return p.missing, nil
	case batchReadServeAcceptedMissRejected:
		p := projectBatchValues(keys, hit.values, plan.rejected, out, false)
		return p.missing, nil
	case batchReadServeAcceptedRefetchRejected:
		p := projectBatchValues(keys, hit.values, plan.rejected, out, true)
		m, err := c.readSingles(ctx, p.fallbackKeys, out)
		return append(p.missing, m...), err
	case batchReadMissAll:
		return slices.Clone(keys), nil
	case batchReadFallbackSingles:
	}
	return c.readSingles(ctx, keys, out)
}

// projectBatchValues maps decoded batch members back to the caller's original
// request shape. Rejected keys are either reported as misses immediately or
// collected for later single-key fallback, depending on the caller's plan.
func projectBatchValues[V any](
	keys []string,
	values map[string]V,
	rejected map[string]struct{},
	out map[string]V,
	fallbackRejected bool,
) batchProjection {
	pr := batchProjection{
		missing: make([]string, 0, len(keys)),
	}
	if fallbackRejected {
		pr.fallbackKeys = make([]string, 0, len(keys))
	}

	for _, k := range keys {
		if _, ok := rejected[k]; ok {
			if fallbackRejected {
				pr.fallbackKeys = append(pr.fallbackKeys, k)
			} else {
				pr.missing = append(pr.missing, k)
			}
			continue
		}

		v, ok := values[k]
		if !ok {
			pr.missing = append(pr.missing, k)
			continue
		}
		out[k] = v
	}
	return pr
}

// rejectBatch deletes an unusable batch blob and emits the matching
// operational hook so repeated reads do not keep reprocessing bad data.
func (c *cache[V]) rejectBatch(
	ctx context.Context,
	storageKey string,
	requested int,
	reason BatchRejectReason,
) {
	_ = c.provider.Del(ctx, storageKey)
	c.hooks.BatchRejected(c.ns, requested, reason)
}

// seedBatchRead warms single-key entries from an already validated batch hit
// according to the configured BatchReadSeed policy.
func (c *cache[V]) seedBatchRead(
	ctx context.Context,
	sortedRequested []string,
	items map[string]wire.BatchItem,
) {
	switch c.batchSeed {
	case BatchReadSeedAll:
		_ = c.seedBatch(ctx, sortedRequested, items, c.defaultTTL)
	case BatchReadSeedIfMissing:
		_ = c.seedBatchIfMissing(ctx, sortedRequested, items, c.defaultTTL)
	}
}

// readSingles reads each unique key exactly once via Get and maps the
// results back onto the caller's original key list. This avoids redundant
// provider and version-store round-trips when the input has duplicates.
//
// Hits are written into out. Keys that were not found (including entries
// that were self-healed during Get) appear in the returned missing slice,
// preserving the caller's original order and duplicates.
// self-heal events inside Get produce misses, not errors.
func (c *cache[V]) readSingles(ctx context.Context, keys []string, out map[string]V) ([]string, error) {
	type res struct {
		v   V
		ok  bool
		err error
	}

	us := sortedUnique(keys)
	tmp := make(map[string]res, len(us))
	for _, k := range us {
		v, ok, err := c.Get(ctx, k)
		tmp[k] = res{v: v, ok: ok, err: err}
	}

	m := make([]string, 0, len(keys))
	for _, k := range keys { // preserve caller order and duplicates
		r := tmp[k]
		if r.ok {
			out[k] = r.v
		} else {
			m = append(m, k)
		}
	}

	var errs []error
	for _, k := range us {
		if err := tmp[k].err; err != nil {
			errs = append(errs, err)
		}
	}
	return m, errors.Join(errs...)
}

// batchKeySorted builds the provider storage key for a batch entry from a set
// of sorted member keys. The key is a hash of the members, so the same set
// always maps to the same storage key regardless of input order.
// The sortedKeys slice must be sorted in ascending order and deduplicated.
func (c *cache[V]) batchKeySorted(sortedKeys []string) (keyutil.ValueKey, error) {
	return c.space.BatchValueSorted(sortedKeys)
}

func (r batchReadGuardResult) allowed() bool { return r.reason == "" }

// guardBatchRead applies the authoritative validation configured for batch hits.
// It prefers BatchReadGuard when present, validates any reported rejected keys,
// and otherwise falls back to per-member ReadGuard checks.
func (c *cache[V]) guardBatchRead(ctx context.Context, values map[string]V) batchReadGuardResult {
	if len(values) == 0 {
		return batchReadGuardResult{}
	}

	if c.batchReadGuard != nil {
		gv := make(map[string]V, len(values))
		maps.Copy(gv, values)
		rejected, err := c.batchReadGuard(ctx, gv)
		if err != nil {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
		}
		if len(rejected) == 0 {
			return batchReadGuardResult{}
		}

		r := make(map[string]struct{}, len(rejected))
		for k := range rejected {
			if _, ok := values[k]; !ok {
				// A guard may reject only members that were actually validated.
				return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
			}
			r[k] = struct{}{}
		}
		return batchReadGuardResult{
			reason:   BatchRejectReasonReadGuardReject,
			rejected: r,
		}
	}

	if c.readGuard == nil {
		return batchReadGuardResult{}
	}

	for k, v := range values {
		ok, err := c.readGuard(ctx, k, v)
		if err != nil {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardError}
		}
		if !ok {
			return batchReadGuardResult{reason: BatchRejectReasonReadGuardReject}
		}
	}
	return batchReadGuardResult{}
}

// decodeBatch decodes only the batch items that the caller requested.
// If any requested item fails to decode the method returns an error and no
// partial results. A decode failure on a requested item means the batch
// entry is not trustworthy and the caller should delete it and fall back
// to per-key reads.
func (c *cache[V]) decodeBatch(requested []string, items map[string]wire.BatchItem) (map[string]V, error) {
	bk := make(map[string]V, len(requested))
	for _, k := range requested {
		it, ok := items[k]
		if !ok {
			return nil, fmt.Errorf("missing batch item %q", k)
		}
		v, err := c.codec.Decode(it.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode batch item %q: %w", k, err)
		}
		bk[k] = v
	}
	return bk, nil
}

// seedFromItems materializes the requested batch members as single entries via
// write, wrapping any per-key failure as an OpError tagged with op. Requested
// keys absent from items are skipped.
func (c *cache[V]) seedFromItems(
	ctx context.Context,
	requested []string,
	items map[string]wire.BatchItem,
	ttl time.Duration,
	op Op,
	write singleSeedFunc,
) error {
	if len(requested) == 0 || len(items) == 0 {
		return nil
	}

	var errs []error
	for _, k := range requested {
		it, ok := items[k]
		if !ok {
			continue
		}
		if err := write(ctx, k, it.Fence, it.Payload, ttl); err != nil {
			errs = append(errs, &OpError{Op: op, Key: k, Err: err})
		}
	}
	return errors.Join(errs...)
}

// seedBatch materializes validated batch members as single key entries.
// It assumes the caller already established that each member fence is safe to
// serve, so it does not re-check the version store.
func (c *cache[V]) seedBatch(
	ctx context.Context,
	requested []string,
	items map[string]wire.BatchItem,
	ttl time.Duration,
) error {
	return c.seedFromItems(ctx, requested, items, ttl, OpSet, c.writeSingle)
}

// seedAfterBatch materializes singles after a successful batch
// write according to BatchWriteSeed. Unknown enum values default to Strict so
// the safest behavior wins if a caller passes an out-of-range mode.
func (c *cache[V]) seedAfterBatch(
	ctx context.Context,
	items []batchWriteItem[V],
	wireItems []wire.BatchItem,
	ttl time.Duration,
) error {
	switch c.batchWriteSeed {
	case BatchWriteSeedOff:
		return nil
	case BatchWriteSeedFast:
		return c.seedFromBatch(ctx, wireItems, ttl)
	case BatchWriteSeedStrict:
		fallthrough
	default:
		return c.seedSingles(ctx, items, ttl)
	}
}

// seedBatchIfMissing is the conditional variant of seedBatch. It only
// inserts a single when the provider reports that the key is currently absent.
func (c *cache[V]) seedBatchIfMissing(
	ctx context.Context,
	requested []string,
	items map[string]wire.BatchItem,
	ttl time.Duration,
) error {
	return c.seedFromItems(ctx, requested, items, ttl, OpAdd, c.addSingle)
}

func (c *cache[V]) seedFromBatch(
	ctx context.Context,
	items []wire.BatchItem,
	ttl time.Duration,
) error {
	if len(items) == 0 {
		return nil
	}

	var errs []error
	for _, it := range items {
		if err := c.writeSingle(ctx, it.Key, it.Fence, it.Payload, ttl); err != nil {
			errs = append(errs, &OpError{Op: OpSet, Key: it.Key, Err: err})
		}
	}
	return errors.Join(errs...)
}

// seedSingles writes each item as an individual single entry via SetIfVersion.
//
//   - When batch mode is disabled, singles are the only cache shape available.
//   - As a fallback when a batch write is skipped (version mismatch, provider
//     rejection, version-store error), it ensures that non-stale keys can still
//     be cached individually.
//
// Each call to SetIfVersionWithTTL performs its own version check, so a stale
// item in the slice is simply skipped without writing bad data.
func (c *cache[V]) seedSingles(
	ctx context.Context,
	items []batchWriteItem[V],
	ttl time.Duration,
) error {
	var errs []error
	for _, it := range items {
		if _, err := c.SetIfVersionWithTTL(ctx, it.key, it.val, it.obs, ttl); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// this builds a lookup map from a slice of stored batch items keyed by
// their logical key. Duplicate keys are resolved with last wins.
func indexBatch(items []wire.BatchItem) map[string]wire.BatchItem {
	bk := make(map[string]wire.BatchItem, len(items))
	for _, it := range items {
		bk[it.Key] = it // duplicates in stored items: last wins
	}
	return bk
}

// converts VersionedValues into internal write items sorted
// by key and returns the deduplicated sorted key slice.
func prepBatchWrite[V any](items []VersionedValue[V]) ([]batchWriteItem[V], []string, error) {
	ws := make([]batchWriteItem[V], len(items))
	for i, it := range items {
		ws[i] = batchWriteItem[V]{
			key: it.Key,
			val: it.Value,
			obs: it.Version,
		}
	}

	slices.SortFunc(ws, func(a, b batchWriteItem[V]) int { return cmp.Compare(a.key, b.key) })

	ks := make([]string, len(ws))
	for i, it := range ws {
		if i > 0 && it.key == ws[i-1].key {
			return nil, nil, fmt.Errorf("duplicate batch item key %q", it.key)
		}
		ks[i] = it.key
	}
	return ws, ks, nil
}

func sortedUnique(keys []string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	out := slices.Clone(keys)
	slices.Sort(out)
	return slices.Compact(out)
}
