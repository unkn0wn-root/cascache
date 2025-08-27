package cascache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	c "github.com/unkn0wn-root/cascache/codec"
	"github.com/unkn0wn-root/cascache/internal/wire"
	pr "github.com/unkn0wn-root/cascache/provider"
)

type memEntry struct {
	v   []byte
	exp time.Time // zero => no TTL
}

type memProvider struct {
	m map[string]memEntry
}

var _ pr.Provider = (*memProvider)(nil)

func newMemProvider() *memProvider { return &memProvider{m: make(map[string]memEntry)} }

func (p *memProvider) Get(_ context.Context, key string) ([]byte, bool, error) {
	e, ok := p.m[key]
	if !ok {
		return nil, false, nil
	}
	if !e.exp.IsZero() && time.Now().After(e.exp) {
		delete(p.m, key)
		return nil, false, nil
	}
	return e.v, true, nil
}

func (p *memProvider) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	p.m[key] = memEntry{v: value, exp: exp}
	return true, nil
}

func (p *memProvider) Del(_ context.Context, key string) error { delete(p.m, key); return nil }
func (p *memProvider) Close(_ context.Context) error           { return nil }

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newTestCache(t *testing.T, ns string, mp pr.Provider, optsOpt func(*Options[user])) CAS[user] {
	t.Helper()
	opts := Options[user]{
		Namespace: ns,
		Provider:  mp,
		Codec:     c.JSON[user]{},
	}
	if optsOpt != nil {
		optsOpt(&opts)
	}
	cc, err := New[user](opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cc
}

func mustImpl[V any](t *testing.T, c CAS[V]) *cache[V] {
	t.Helper()
	impl, ok := c.(*cache[V])
	if !ok {
		t.Fatalf("unexpected concrete type for CAS")
	}
	return impl
}

// ==============================
// Single-entry CAS tests
// ==============================

// TestSingleCASFlow verifies CAS write, read, invalidation, and stale write skip.
func TestSingleCASFlow(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	k := "u:1"
	v := user{ID: "1", Name: "Ada"}

	// Miss initially.
	if got, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get miss expected, got ok=%v err=%v val=%v", ok, err, got)
	}

	// CAS write with observed gen 0.
	obs := cc.SnapshotGen(k)
	if obs != 0 {
		t.Fatalf("SnapshotGen expected 0, got %d", obs)
	}
	if err := cc.SetWithGen(ctx, k, v, obs, 0); err != nil {
		t.Fatalf("SetWithGen: %v", err)
	}

	// Read back.
	if got, ok, err := cc.Get(ctx, k); err != nil || !ok || got != v {
		t.Fatalf("Get after set: ok=%v err=%v got=%v", ok, err, got)
	}

	// Invalidate -> bump gen & delete single.
	if err := cc.Invalidate(ctx, k); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// Miss again after invalidate.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get after invalidate should miss, ok=%v err=%v", ok, err)
	}

	// Stale write (using old observed gen 0) should be skipped.
	if err := cc.SetWithGen(ctx, k, v, 0, 0); err != nil {
		t.Fatalf("SetWithGen stale: %v", err)
	}
	if _, ok, _ := cc.Get(ctx, k); ok {
		t.Fatalf("stale write should not populate cache")
	}

	// Fresh write with observed current gen should succeed.
	obs2 := cc.SnapshotGen(k)
	if err := cc.SetWithGen(ctx, k, v, obs2, 0); err != nil {
		t.Fatalf("SetWithGen (fresh): %v", err)
	}
	if got, ok, err := cc.Get(ctx, k); err != nil || !ok || got != v {
		t.Fatalf("Get after fresh set: ok=%v err=%v got=%v", ok, err, got)
	}
}

// ==============================
// Self-heal tests (corruption/gen mismatch)
// ==============================

// TestSelfHealOnCorrupt ensures corrupt provider bytes are deleted and missed,
// and that a valid-but-stale single is rejected and removed.
func TestSelfHealOnCorrupt(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	impl := mustImpl(t, cc)

	k := "bad"
	storageKey := impl.singleKey(k)

	// Inject corrupt bytes directly into provider.
	if ok, err := impl.provider.Set(ctx, storageKey, []byte("not-wire-format"), 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject corrupt: ok=%v err=%v", ok, err)
	}

	// First Get should detect corruption, delete entry, and miss.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get on corrupt should miss, ok=%v err=%v", ok, err)
	}
	// Corrupt entry should be gone.
	if _, ok, _ := mp.Get(ctx, storageKey); ok {
		t.Fatalf("corrupt entry was not deleted by self-heal")
	}

	// Now inject a valid single with gen=0, then bump generation to make it stale.
	val := user{ID: "x", Name: "X"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry := wire.EncodeSingle(0, payload)
	if ok, err := impl.provider.Set(ctx, storageKey, wireEntry, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject valid stale: ok=%v err=%v", ok, err)
	}
	_, _ = impl.bumpGen(context.Background(), storageKey) // make it stale

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get on stale single should miss, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := mp.Get(ctx, storageKey); ok {
		t.Fatalf("stale entry was not deleted by self-heal")
	}
}

// ==============================
// Bulk behavior tests
// ==============================

// TestBulkHappyAndStale validates bulk read, then invalidation of one member causes
// bulk rejection and fallback to singles with missing reported for the invalidated key.
func TestBulkHappyAndStale(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	keys := []string{"a", "b", "c"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
		"c": {ID: "c", Name: "C"},
	}

	// Snapshot gens (all zero).
	snap := cc.SnapshotGens(keys)

	// Write bulk with gens.
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	// First GetBulk: all present, no missing.
	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetBulk expected all hit, missing=%v got=%v", missing, got)
	}

	// Invalidate "b": removes its single and bumps gen. Bulk should be rejected on next read.
	if err := cc.Invalidate(ctx, "b"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	got2, missing2, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk after invalidate: %v", err)
	}
	if len(missing2) != 1 || missing2[0] != "b" {
		t.Fatalf("expected only 'b' missing, got %v", missing2)
	}
	// 'a' and 'c' should still be present (from singles seeding).
	if _, ok := got2["a"]; !ok {
		t.Fatalf("expected 'a' present after bulk rejection")
	}
	if _, ok := got2["c"]; !ok {
		t.Fatalf("expected 'c' present after bulk rejection")
	}

	// Ensure the stale bulk was dropped from provider.
	for k := range mp.m {
		if strings.HasPrefix(k, "bulk:user:") {
			t.Fatalf("stale bulk should have been deleted, found %q", k)
		}
	}
}

// TestBulkDisabled ensures that when bulk is disabled, no bulk keys are written
// and GetBulk falls back to singles.
func TestBulkDisabled(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DisableBulk = true
	})
	defer cc.Close(ctx)

	keys := []string{"x", "y"}
	items := map[string]user{
		"x": {ID: "x", Name: "X"},
		"y": {ID: "y", Name: "Y"},
	}
	snap := cc.SnapshotGens(keys)

	// Set bulk with gens -> should seed singles only (no bulk key).
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens (bulk disabled): %v", err)
	}

	// GetBulk should return both via singles path.
	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk (bulk disabled): %v", err)
	}
	if len(missing) != 0 || len(got) != 2 {
		t.Fatalf("GetBulk (bulk disabled) expected all present, missing=%v got=%v", missing, got)
	}

	// Assert no "bulk:user:" key exists in provider.
	for k := range mp.m {
		if strings.HasPrefix(k, "bulk:user:") {
			t.Fatalf("bulk disabled but found bulk key %q written", k)
		}
	}
}

// TestBulkOrderInsensitiveHit: Same set, different order → same bulk key, bulk hit.
func TestBulkOrderInsensitiveHit(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	impl := mustImpl(t, cc)

	// Write a bulk for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := cc.SnapshotGens([]string{"u1", "u3", "u4"})
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	// Remove singles so GetBulk must rely on the bulk entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKey(k))
	}

	// Request same set, different order → should hit bulk, no missing
	got, missing, err := cc.GetBulk(ctx, []string{"u3", "u1", "u4"})
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 values, got %d (%v)", len(got), got)
	}

	// Bulk should remain (valid hit)
	foundBulk := false
	for k := range mp.m {
		if strings.HasPrefix(k, "bulk:user:") {
			foundBulk = true
			break
		}
	}
	if !foundBulk {
		t.Fatalf("expected bulk entry to remain after valid hit")
	}
}

// TestBulkDuplicateRequestHit: Request has duplicates → still hits unique-set bulk.
func TestBulkDuplicateRequestHit(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	impl := mustImpl(t, cc)

	// Write a bulk for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := cc.SnapshotGens([]string{"u1", "u3", "u4"})
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	// Remove singles so GetBulk must rely on the bulk entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKey(k))
	}

	// Request contains duplicates → should still hit the same bulk key
	req := []string{"u1", "u3", "u3", "u4"}
	got, missing, err := cc.GetBulk(ctx, req)
	if err != nil {
		t.Fatalf("GetBulk dup: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing for dup request, got %v", missing)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 unique results, got %d (%v)", len(got), got)
	}
}

// TestBulkKeyCanonicalization: equal sets (order/dups ignored) produce same bulk key.
func TestBulkKeyCanonicalization(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	impl := mustImpl(t, cc)

	k1 := impl.bulkKeySorted(uniqSorted([]string{"u3", "u1", "u4"}))
	k2 := impl.bulkKeySorted(uniqSorted([]string{"u1", "u3", "u3", "u4"}))
	if k1 != k2 {
		t.Fatalf("bulk keys differ for equivalent sets: %q vs %q", k1, k2)
	}
}

// ==============================
// Wire format tests
// ==============================

// DecodeSingle must reject trailing bytes (strict framing).
func TestWireDecodeSingleRejectsTrailing(t *testing.T) {
	b := wire.EncodeSingle(7, []byte("x"))
	b = append(b, 0xDE, 0xAD) // trailing junk
	if _, _, err := wire.DecodeSingle(b); err == nil {
		t.Fatalf("DecodeSingle should reject trailing bytes")
	}
}

// DecodeBulk must reject trailing bytes (strict framing).
func TestWireDecodeBulkRejectsTrailing(t *testing.T) {
	enc, err := wire.EncodeBulk([]wire.BulkItem{
		{Key: "k", Gen: 1, Payload: []byte("v")},
	})
	if err != nil {
		t.Fatalf("EncodeBulk: %v", err)
	}
	enc = append(enc, 0xBE, 0xEF)
	if _, err := wire.DecodeBulk(enc); err == nil {
		t.Fatalf("DecodeBulk should reject trailing bytes")
	}
}

// EncodeBulk should error on invalid key lengths (0 and > 0xFFFF),
// and succeed on boundary length 0xFFFF.
func TestEncodeBulkKeyLengthValidation(t *testing.T) {
	// Empty key -> error
	if _, err := wire.EncodeBulk([]wire.BulkItem{
		{Key: "", Gen: 1, Payload: []byte("x")},
	}); err == nil {
		t.Fatalf("EncodeBulk should error on empty key")
	}

	// Too long key (65536) -> error
	longKey := strings.Repeat("a", 0x10000)
	if _, err := wire.EncodeBulk([]wire.BulkItem{
		{Key: longKey, Gen: 1, Payload: []byte("x")},
	}); err == nil {
		t.Fatalf("EncodeBulk should error on key length > 0xFFFF")
	}

	// Boundary (65535) -> ok
	boundaryKey := strings.Repeat("b", 0xFFFF)
	if _, err := wire.EncodeBulk([]wire.BulkItem{
		{Key: boundaryKey, Gen: 1, Payload: []byte("x")},
	}); err != nil {
		t.Fatalf("EncodeBulk should succeed at 0xFFFF key length, got err: %v", err)
	}
}

// Bogus n in bulk header should not preallocate huge capacity and should error cleanly.
func TestDecodeBulkFakeNNotPrealloc(t *testing.T) {
	var buf bytes.Buffer
	// magic "CASC"
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	// version
	buf.WriteByte(1)
	// kind bulk
	buf.WriteByte(2)
	// n = 0xFFFFFFFF
	var u4 [4]byte
	binary.BigEndian.PutUint32(u4[:], ^uint32(0))
	buf.Write(u4[:])
	// no items

	if _, err := wire.DecodeBulk(buf.Bytes()); err == nil {
		t.Fatalf("DecodeBulk should fail on wrong n with insufficient bytes")
	}
}

// ==============================
// Snapshot gens tests
// ==============================

// Self-heal when a valid single has trailing bytes appended in the provider.
func TestSelfHealOnGenMismatchSingle(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	impl := mustImpl(t, cc)
	k := "gen-mismatch"
	storageKey := impl.singleKey(k)

	// GenStore has never been bumped for this key -> snapshot is 0.
	val := user{ID: "u1", Name: "Mismatch"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Write a valid frame with gen=1 (mismatches snapshot=0).
	b := wire.EncodeSingle(1, payload)
	if ok, err := impl.provider.Set(ctx, storageKey, b, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	// Get should detect gen mismatch, delete, and miss.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("expected miss on gen mismatch, ok=%v err=%v", ok, err)
	}

	// Ensure self-heal actually deleted the bad entry.
	if _, ok, _ := mp.Get(ctx, storageKey); ok {
		t.Fatalf("gen-mismatch single was not deleted by self-heal")
	}
}

func TestBulkValidTable(t *testing.T) {
	ctx := context.Background()

	newImpl := func(t *testing.T) *cache[user] {
		t.Helper()
		mp := newMemProvider()
		cc := newTestCache(t, "user", mp, nil)
		t.Cleanup(func() { _ = cc.Close(ctx) })
		return mustImpl(t, cc)
	}

	// helper: bump to exactly 'n'
	bumpTo := func(impl *cache[user], ukey string, n uint64) {
		sk := impl.singleKey(ukey)
		for i := uint64(0); i < n; i++ {
			_, _ = impl.bumpGen(ctx, sk)
		}
	}

	t.Run("valid_all_members_fresh", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"} // already sorted

		// current gens: a=1, b=1, c=1
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}

		items := []wire.BulkItem{
			{Key: "a", Gen: 1, Payload: nil},
			{Key: "b", Gen: 1, Payload: nil},
			{Key: "c", Gen: 1, Payload: nil},
		}
		if !impl.bulkValid(ctx, keys, items) {
			t.Fatalf("bulkValid should be true for fresh members")
		}
	})

	t.Run("missing_member_in_bulk", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"}

		// current gens: a=1, b=1, c=1
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}

		// omit "b" from items
		items := []wire.BulkItem{
			{Key: "a", Gen: 1, Payload: nil},
			{Key: "c", Gen: 1, Payload: nil},
		}
		if impl.bulkValid(ctx, keys, items) {
			t.Fatalf("bulkValid should be false when a requested member is missing")
		}
	})

	t.Run("stale_member_gen_mismatch", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"}

		// current gens: a=1, b=1, c=1
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}

		// Make "b" stale by putting Gen=0 (current is 1)
		items := []wire.BulkItem{
			{Key: "a", Gen: 1, Payload: nil},
			{Key: "b", Gen: 0, Payload: nil}, // stale
			{Key: "c", Gen: 1, Payload: nil},
		}
		if impl.bulkValid(ctx, keys, items) {
			t.Fatalf("bulkValid should be false when any member is stale")
		}
	})

	t.Run("extra_member_ignored", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b"}

		// current gens: a=1, b=1
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}

		// Include an extra "z" that isn't requested. Should be ignored.
		items := []wire.BulkItem{
			{Key: "a", Gen: 1, Payload: nil},
			{Key: "b", Gen: 1, Payload: nil},
			{Key: "z", Gen: 999, Payload: nil}, // extra
		}
		if !impl.bulkValid(ctx, keys, items) {
			t.Fatalf("bulkValid should be true; extras in bulk are ignored")
		}
	})
}

func equalU64(a, b map[string]uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Covers: empty input, duplicates, missing (0), and mixed bumped gens.
func TestSnapshotGensBehavior(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	t.Cleanup(func() { _ = cc.Close(ctx) })
	impl := mustImpl(t, cc)

	t.Run("empty", func(t *testing.T) {
		got := cc.SnapshotGens(nil)
		if len(got) != 0 {
			t.Fatalf("empty: expected empty map, got %v", got)
		}
	})

	t.Run("duplicates_and_zero_missing", func(t *testing.T) {
		// No bumps yet → everything is 0
		keys := []string{"dupa", "dupa", "other"}
		got := cc.SnapshotGens(keys)
		want := map[string]uint64{"dupa": 0, "other": 0}
		if !equalU64(got, want) {
			t.Fatalf("dups/zeros: got %v want %v", got, want)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		// m1 -> 1, m3 -> 3, m2 -> 0
		_, _ = impl.bumpGen(ctx, impl.singleKey("m1"))
		for i := 0; i < 3; i++ {
			_, _ = impl.bumpGen(ctx, impl.singleKey("m3"))
		}
		keys := []string{"m1", "m2", "m3", "m1"} // include duplicate
		got := cc.SnapshotGens(keys)
		want := map[string]uint64{"m1": 1, "m2": 0, "m3": 3}
		if !equalU64(got, want) {
			t.Fatalf("mixed: got %v want %v", got, want)
		}
	})
}

// ==============================
// Invalidate edge-case behavior (cluster down etc.)
// ==============================

type failingGenStore struct{ bumpErr error }

func (s *failingGenStore) Snapshot(context.Context, string) (uint64, error) { return 0, nil }
func (s *failingGenStore) SnapshotMany(context.Context, []string) (map[string]uint64, error) {
	return map[string]uint64{}, nil
}
func (s *failingGenStore) Bump(context.Context, string) (uint64, error) { return 0, s.bumpErr }
func (s *failingGenStore) Cleanup(time.Duration)                        {}
func (s *failingGenStore) Close(context.Context) error                  { return nil }

type delErrProvider struct {
	*memProvider
	err error
}

var _ pr.Provider = (*delErrProvider)(nil)

func (p *delErrProvider) Del(_ context.Context, key string) error { return p.err }

func TestInvalidateBothFailReturnsError(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	sentinelDelErr := errors.New("del failed")
	bumpFail := errors.New("bump failed")

	cc := newTestCache(t, "user", &delErrProvider{memProvider: mp, err: sentinelDelErr}, func(o *Options[user]) {
		o.GenStore = &failingGenStore{bumpErr: bumpFail}
	})
	defer cc.Close(ctx)

	err := cc.Invalidate(ctx, "k1")
	if err == nil {
		t.Fatalf("expected error when both bump and delete fail")
	}
	var ie *InvalidateError
	if !errors.As(err, &ie) {
		t.Fatalf("expected InvalidateError, got %T: %v", err, err)
	}
	// Unwrap should expose underlying delete error.
	if !errors.Is(err, sentinelDelErr) {
		t.Fatalf("expected errors.Is(err, delErr) to be true")
	}
}

func TestInvalidateBumpFailDeleteOKNoError(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = &failingGenStore{bumpErr: errors.New("bump failed")}
	})
	defer cc.Close(ctx)

	if err := cc.Invalidate(ctx, "k2"); err != nil {
		t.Fatalf("expected no error when bump fails but delete succeeds; got %v", err)
	}
}

func TestInvalidateBumpOKDeleteFailNoError(t *testing.T) {
	ctx := context.Background()
	sentinelDelErr := errors.New("del failed")
	// normal genstore (local), provider delete fails
	mp := &delErrProvider{memProvider: newMemProvider(), err: sentinelDelErr}

	cc := newTestCache(t, "user", mp, nil)
	defer cc.Close(ctx)

	// Warm a gen so bump definitely succeeds.
	impl := mustImpl(t, cc)
	_, _ = impl.bumpGen(ctx, impl.singleKey("k3"))

	if err := cc.Invalidate(ctx, "k3"); err != nil {
		t.Fatalf("expected no error when delete fails but bump succeeds; got %v", err)
	}
}
