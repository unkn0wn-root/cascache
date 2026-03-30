package cascache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	c "github.com/unkn0wn-root/cascache/v3/codec"
	"github.com/unkn0wn-root/cascache/v3/internal/wire"
	pr "github.com/unkn0wn-root/cascache/v3/provider"
	"github.com/unkn0wn-root/cascache/v3/version"
)

type memEntry struct {
	v   []byte
	exp time.Time // zero => no TTL
}

const batchValueRoot = "cas:v3:val:b:"

type memProvider struct {
	m map[string]memEntry
}

var (
	_ pr.Provider = (*memProvider)(nil)
	_ pr.Adder    = (*memProvider)(nil)
)

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

func (p *memProvider) Set(
	_ context.Context,
	key string,
	value []byte,
	_ int64,
	ttl time.Duration,
) (bool, error) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	p.m[key] = memEntry{v: value, exp: exp}
	return true, nil
}

func (p *memProvider) Add(
	_ context.Context,
	key string,
	value []byte,
	_ int64,
	ttl time.Duration,
) (bool, error) {
	if _, ok := p.m[key]; ok {
		return false, nil
	}

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	p.m[key] = memEntry{v: value, exp: exp}
	return true, nil
}

func (p *memProvider) Del(_ context.Context, key string) error { delete(p.m, key); return nil }
func (p *memProvider) Close(_ context.Context) error           { return nil }

type getErrProvider struct {
	*memProvider
	err error
}

var _ pr.Provider = (*getErrProvider)(nil)

func (p *getErrProvider) Get(_ context.Context, key string) ([]byte, bool, error) {
	return nil, false, p.err
}

type batchGetErrProvider struct {
	*memProvider
	err            error
	singleGetCalls int
}

var _ pr.Provider = (*batchGetErrProvider)(nil)

func (p *batchGetErrProvider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if strings.HasPrefix(key, batchValueRoot) {
		return nil, false, p.err
	}
	p.singleGetCalls++
	return p.memProvider.Get(ctx, key)
}

type singleGetErrProvider struct {
	*memProvider
	err            error
	singleGetCalls int
}

var _ pr.Provider = (*singleGetErrProvider)(nil)

func (p *singleGetErrProvider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if strings.HasPrefix(key, batchValueRoot) {
		return p.memProvider.Get(ctx, key)
	}
	p.singleGetCalls++
	return nil, false, p.err
}

type setErrProvider struct {
	*memProvider
	err error
}

var _ pr.Provider = (*setErrProvider)(nil)

func (p *setErrProvider) Set(
	_ context.Context,
	key string,
	value []byte,
	_ int64,
	ttl time.Duration,
) (bool, error) {
	return false, p.err
}

type plainProvider struct {
	inner *memProvider
}

var _ pr.Provider = (*plainProvider)(nil)

func (p *plainProvider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return p.inner.Get(ctx, key)
}

func (p *plainProvider) Set(
	ctx context.Context,
	key string,
	value []byte,
	cost int64,
	ttl time.Duration,
) (bool, error) {
	return p.inner.Set(ctx, key, value, cost, ttl)
}

func (p *plainProvider) Del(ctx context.Context, key string) error {
	return p.inner.Del(ctx, key)
}

func (p *plainProvider) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

type batchRejectSingleErrProvider struct {
	*memProvider
	err error
}

var _ pr.Provider = (*batchRejectSingleErrProvider)(nil)

func (p *batchRejectSingleErrProvider) Set(
	_ context.Context,
	key string,
	value []byte,
	_ int64,
	ttl time.Duration,
) (bool, error) {
	if strings.HasPrefix(key, batchValueRoot) {
		return false, nil
	}
	return false, p.err
}

type batchRejectProvider struct {
	*memProvider
}

var _ pr.Provider = (*batchRejectProvider)(nil)

func (p *batchRejectProvider) Set(
	ctx context.Context,
	key string,
	value []byte,
	cost int64,
	ttl time.Duration,
) (bool, error) {
	if strings.HasPrefix(key, batchValueRoot) {
		return false, nil
	}
	return p.memProvider.Set(ctx, key, value, cost, ttl)
}

type countingAdderProvider struct {
	*memProvider
	setCalls  int
	addCalls  int
	addStored int
}

var (
	_ pr.Provider = (*countingAdderProvider)(nil)
	_ pr.Adder    = (*countingAdderProvider)(nil)
)

func (p *countingAdderProvider) Set(
	ctx context.Context,
	key string,
	value []byte,
	cost int64,
	ttl time.Duration,
) (bool, error) {
	p.setCalls++
	return p.memProvider.Set(ctx, key, value, cost, ttl)
}

func (p *countingAdderProvider) Add(
	ctx context.Context,
	key string,
	value []byte,
	cost int64,
	ttl time.Duration,
) (bool, error) {
	p.addCalls++
	stored, err := p.memProvider.Add(ctx, key, value, cost, ttl)
	if stored {
		p.addStored++
	}
	return stored, err
}

type countingVersionStore struct {
	inner             version.Store
	snapshotCalls     int
	snapshotManyCalls int
}

var _ version.Store = (*countingVersionStore)(nil)

func (s *countingVersionStore) Snapshot(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, error) {
	s.snapshotCalls++
	return s.inner.Snapshot(ctx, k)
}

func (s *countingVersionStore) SnapshotMany(
	ctx context.Context,
	ks []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
	s.snapshotManyCalls++
	return s.inner.SnapshotMany(ctx, ks)
}

func (s *countingVersionStore) CreateIfMissing(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, bool, error) {
	return s.inner.CreateIfMissing(ctx, k)
}

func (s *countingVersionStore) Advance(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, error) {
	return s.inner.Advance(ctx, k)
}

func (s *countingVersionStore) Cleanup(retention time.Duration) {
	s.inner.Cleanup(retention)
}

func (s *countingVersionStore) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

type advanceAfterSnapshotManyVersionStore struct {
	inner            version.Store
	advanceKey       version.CacheKey
	advanceOnCall    int
	snapshotManyCall int
	advanced         bool
}

var _ version.Store = (*advanceAfterSnapshotManyVersionStore)(nil)

func (s *advanceAfterSnapshotManyVersionStore) Snapshot(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, error) {
	return s.inner.Snapshot(ctx, k)
}

func (s *advanceAfterSnapshotManyVersionStore) SnapshotMany(
	ctx context.Context,
	ks []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
	got, err := s.inner.SnapshotMany(ctx, ks)
	s.snapshotManyCall++
	if err != nil || s.advanced || s.advanceKey == (version.CacheKey{}) ||
		s.snapshotManyCall != s.advanceOnCall {
		return got, err
	}
	s.advanced = true
	if _, advanceErr := s.inner.Advance(ctx, s.advanceKey); advanceErr != nil {
		return nil, advanceErr
	}
	return got, nil
}

func (s *advanceAfterSnapshotManyVersionStore) CreateIfMissing(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, bool, error) {
	return s.inner.CreateIfMissing(ctx, k)
}

func (s *advanceAfterSnapshotManyVersionStore) Advance(
	ctx context.Context,
	k version.CacheKey,
) (version.Snapshot, error) {
	return s.inner.Advance(ctx, k)
}

func (s *advanceAfterSnapshotManyVersionStore) Cleanup(retention time.Duration) {
	s.inner.Cleanup(retention)
}

func (s *advanceAfterSnapshotManyVersionStore) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

type recordingKeyAdapter struct {
	setStored       bool
	setErr          error
	invalidateErr   error
	setCalls        int
	invalidateCalls int
	lastVersionKey  version.CacheKey
	lastValueKey    string
	lastExpected    version.Snapshot
	lastPayload     []byte
	lastTTL         time.Duration
}

var _ KeyMutator = (*recordingKeyAdapter)(nil)

func (s *recordingKeyAdapter) SetIfVersion(
	_ context.Context,
	versionKey version.CacheKey,
	valueKey string,
	expected version.Snapshot,
	payload []byte,
	ttl time.Duration,
) (bool, error) {
	s.setCalls++
	s.lastVersionKey = versionKey
	s.lastValueKey = valueKey
	s.lastExpected = expected
	s.lastPayload = append([]byte(nil), payload...)
	s.lastTTL = ttl
	return s.setStored, s.setErr
}

func (s *recordingKeyAdapter) Invalidate(
	_ context.Context,
	versionKey version.CacheKey,
	valueKey string,
) error {
	s.invalidateCalls++
	s.lastVersionKey = versionKey
	s.lastValueKey = valueKey
	return s.invalidateErr
}

type user struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type testCache[V any] struct {
	CAS[V]
}

func newTestCache(
	t *testing.T,
	ns string,
	mp pr.Provider,
	optsOpt func(*Options[user]),
) *testCache[user] {
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
	return &testCache[user]{
		CAS: cc,
	}
}

func mustSnapshotVersion[V any](t *testing.T, ctx context.Context, c CAS[V], key string) Version {
	t.Helper()
	version, err := c.SnapshotVersion(ctx, key)
	if err != nil {
		t.Fatalf("SnapshotVersion(%q): %v", key, err)
	}
	return version
}

func mustSnapshotVersions[V any](
	t *testing.T,
	ctx context.Context,
	c CAS[V],
	keys []string,
) map[string]Version {
	t.Helper()
	versions, err := c.SnapshotVersions(ctx, keys)
	if err != nil {
		t.Fatalf("SnapshotVersions(%v): %v", keys, err)
	}
	return versions
}

func setIfVersionsMap[V any](
	ctx context.Context,
	c CAS[V],
	items map[string]V,
	observed map[string]Version,
	ttl time.Duration,
) error {
	var err error
	if ttl == 0 {
		_, err = c.SetIfVersions(ctx, versionedValues(items, observed))
	} else {
		_, err = c.SetIfVersionsWithTTL(ctx, versionedValues(items, observed), ttl)
	}
	return err
}

func versionedValues[V any](items map[string]V, observed map[string]Version) []VersionedValue[V] {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	keys = sortedUnique(keys)

	values := make([]VersionedValue[V], 0, len(keys))
	for _, key := range keys {
		values = append(values, VersionedValue[V]{
			Key:     key,
			Value:   items[key],
			Version: observed[key],
		})
	}
	return values
}

func missingVersions(keys []string) map[string]Version {
	out := make(map[string]Version, len(keys))
	for _, key := range sortedUnique(keys) {
		out[key] = Version{}
	}
	return out
}

func testFence(id uint64) version.Fence {
	var token [16]byte
	token[0] = 0xA5
	binary.BigEndian.PutUint64(token[8:], id)
	fence, err := version.ParseFenceBinary(token[:])
	if err != nil {
		panic(err)
	}
	return fence
}

func mutateFence(f version.Fence, mutate func([]byte)) version.Fence {
	b, err := f.MarshalBinary()
	if err != nil {
		panic(err)
	}
	mutate(b)
	mutated, err := version.ParseFenceBinary(b)
	if err != nil {
		panic(err)
	}
	return mutated
}

func closeTest(t *testing.T, ctx context.Context, c interface{ Close(context.Context) error }) {
	t.Helper()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func assertExpiryBefore(
	t *testing.T,
	got time.Time,
	before time.Time,
	limit time.Duration,
	label string,
) {
	t.Helper()
	if got.IsZero() {
		t.Fatalf("%s should have a TTL", label)
	}
	if !got.Before(before.Add(limit)) {
		t.Fatalf("%s expiry too far out: got %v, want before %v", label, got, before.Add(limit))
	}
}

func mustImpl(t *testing.T, c interface{}) *cache[user] {
	t.Helper()
	switch cc := c.(type) {
	case *cache[user]:
		return cc
	case *testCache[user]:
		impl, ok := cc.CAS.(*cache[user])
		if !ok {
			t.Fatalf("unexpected concrete type for CAS")
		}
		return impl
	}
	impl, ok := c.(*cache[user])
	if !ok {
		t.Fatalf("unexpected concrete type for CAS")
	}
	return impl
}

func batchValuePrefix(namespace string) string {
	return fmt.Sprintf("%s%d:%s:", batchValueRoot, len(namespace), namespace)
}

func loadSnapshotsByKey[V any](
	t *testing.T,
	ctx context.Context,
	c *cache[V],
	keys []string,
) map[string]version.Snapshot {
	t.Helper()

	ss, err := c.loadSnapshots(ctx, keys)
	if err != nil {
		t.Fatalf("loadSnapshots: %v", err)
	}

	out := make(map[string]version.Snapshot, len(keys))
	for i, key := range keys {
		out[key] = ss[i]
	}
	return out
}

// ==============================
// Single-entry CAS tests
// ==============================

// TestSingleCASFlow verifies CAS write, read, invalidation, and stale write skip.
func TestSingleCASFlow(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	k := "u:1"
	v := user{ID: "1", Name: "Ada"}

	// Miss initially.
	if got, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get miss expected, got ok=%v err=%v val=%v", ok, err, got)
	}

	// CAS write with an observed missing version.
	obs := mustSnapshotVersion(t, ctx, cc, k)
	if !obs.IsMissing() {
		t.Fatalf("SnapshotVersion should be missing before first write, got %+v", obs)
	}
	result, err := cc.SetIfVersion(ctx, k, v, obs)
	if err != nil {
		t.Fatalf("SetIfVersion: %v", err)
	}
	if result.Outcome != WriteOutcomeStored {
		t.Fatalf("SetIfVersion outcome=%q want %q", result.Outcome, WriteOutcomeStored)
	}

	// Read back.
	if got, ok, err := cc.Get(ctx, k); err != nil || !ok || got != v {
		t.Fatalf("Get after set: ok=%v err=%v got=%v", ok, err, got)
	}

	// Invalidate -> advance version and delete single.
	if err := cc.Invalidate(ctx, k); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// Miss again after invalidate.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get after invalidate should miss, ok=%v err=%v", ok, err)
	}

	// Stale write using the original observed version should be skipped.
	result, err = cc.SetIfVersion(ctx, k, v, obs)
	if err != nil {
		t.Fatalf("SetIfVersion stale: %v", err)
	}
	if result.Outcome != WriteOutcomeVersionMismatch {
		t.Fatalf("SetIfVersion stale outcome=%q want %q", result.Outcome, WriteOutcomeVersionMismatch)
	}
	if _, ok, _ := cc.Get(ctx, k); ok {
		t.Fatalf("stale write should not populate cache")
	}

	// Fresh write with the current observed version should succeed.
	obs2 := mustSnapshotVersion(t, ctx, cc, k)
	result, err = cc.SetIfVersion(ctx, k, v, obs2)
	if err != nil {
		t.Fatalf("SetIfVersion(fresh): %v", err)
	}
	if result.Outcome != WriteOutcomeStored {
		t.Fatalf("SetIfVersion(fresh) outcome=%q want %q", result.Outcome, WriteOutcomeStored)
	}
	if got, ok, err := cc.Get(ctx, k); err != nil || !ok || got != v {
		t.Fatalf("Get after fresh set: ok=%v err=%v got=%v", ok, err, got)
	}
}

func TestGetReadGuardRejectsAndDeletesEntry(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(context.Context, string, user) (bool, error) { return false, nil }
	})
	defer closeTest(t, ctx, cc)

	k := "u:guard"
	v := user{ID: "guard", Name: "Ada"}
	if _, err := cc.SetIfVersion(ctx, k, v, mustSnapshotVersion(t, ctx, cc, k)); err != nil {
		t.Fatalf("SetIfVersion: %v", err)
	}

	got, ok, err := cc.Get(ctx, k)
	if err != nil || ok {
		t.Fatalf("Get should miss after read-guard rejection, ok=%v err=%v got=%v", ok, err, got)
	}

	impl := mustImpl(t, cc)
	sk := impl.singleKeys(k)
	if _, found, err := mp.Get(ctx, sk.Value.String()); err != nil || found {
		t.Fatalf("read-guard rejection should delete stored single, found=%v err=%v", found, err)
	}
}

func TestGetReadGuardErrorMissesAndDeletesEntry(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("guard failed")
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(context.Context, string, user) (bool, error) { return false, sentinel }
	})
	defer closeTest(t, ctx, cc)

	k := "u:guard-error"
	v := user{ID: "guard-error", Name: "Ada"}
	if _, err := cc.SetIfVersion(ctx, k, v, mustSnapshotVersion(t, ctx, cc, k)); err != nil {
		t.Fatalf("SetIfVersion: %v", err)
	}

	got, ok, err := cc.Get(ctx, k)
	if err != nil || ok {
		t.Fatalf("Get should miss after read-guard error, ok=%v err=%v got=%v", ok, err, got)
	}

	impl := mustImpl(t, cc)
	sk := impl.singleKeys(k)
	if _, found, err := mp.Get(ctx, sk.Value.String()); err != nil || found {
		t.Fatalf("read-guard error should delete stored single, found=%v err=%v", found, err)
	}
}

func TestSingleKeyNamespaceFramingAvoidsCollisions(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := version.NewLocalWithCleanup(time.Hour, time.Hour)

	left := newTestCache(t, "app:prod", mp, func(o *Options[user]) {
		o.VersionStore = gs
	})
	defer closeTest(t, ctx, left)

	right := newTestCache(t, "app", mp, func(o *Options[user]) {
		o.VersionStore = gs
	})
	defer closeTest(t, ctx, right)

	leftImpl := mustImpl(t, left)
	rightImpl := mustImpl(t, right)
	leftKeys := leftImpl.singleKeys("users:42")
	rightKeys := rightImpl.singleKeys("prod:users:42")

	if leftKeys.Cache == rightKeys.Cache {
		t.Fatalf("single cache keys collided: %q", leftKeys.Cache)
	}
	if leftKeys.Value == rightKeys.Value {
		t.Fatalf("single value keys collided: %q", leftKeys.Value)
	}

	leftVal := user{ID: "left", Name: "Left"}
	rightVal := user{ID: "right", Name: "Right"}
	if _, err := left.SetIfVersionWithTTL(
		ctx,
		"users:42",
		leftVal,
		mustSnapshotVersion(t, ctx, left, "users:42"),
		time.Minute,
	); err != nil {
		t.Fatalf("left SetIfVersion: %v", err)
	}
	if _, err := right.SetIfVersionWithTTL(
		ctx,
		"prod:users:42",
		rightVal,
		mustSnapshotVersion(t, ctx, right, "prod:users:42"),
		time.Minute,
	); err != nil {
		t.Fatalf("right SetIfVersion: %v", err)
	}

	if len(mp.m) != 2 {
		keys := make([]string, 0, len(mp.m))
		for k := range mp.m {
			keys = append(keys, k)
		}
		t.Fatalf("expected 2 distinct provider keys, got %d: %v", len(mp.m), keys)
	}

	gotLeft, ok, err := left.Get(ctx, "users:42")
	if err != nil || !ok || gotLeft != leftVal {
		t.Fatalf("left Get: got=%+v ok=%v err=%v", gotLeft, ok, err)
	}
	gotRight, ok, err := right.Get(ctx, "prod:users:42")
	if err != nil || !ok || gotRight != rightVal {
		t.Fatalf("right Get: got=%+v ok=%v err=%v", gotRight, ok, err)
	}

	if err := left.Invalidate(ctx, "users:42"); err != nil {
		t.Fatalf("left Invalidate: %v", err)
	}
	if _, ok, err := left.Get(ctx, "users:42"); err != nil || ok {
		t.Fatalf("left Get after invalidate: ok=%v err=%v", ok, err)
	}

	gotRight, ok, err = right.Get(ctx, "prod:users:42")
	if err != nil || !ok || gotRight != rightVal {
		t.Fatalf("right Get after left invalidate: got=%+v ok=%v err=%v", gotRight, ok, err)
	}
}

func TestSetIfVersionSnapshotErrorReturnsErrorAndSkipsWrite(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	sentinel := errors.New("snapshot failed")

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = &failingVersionStore{snapshotErr: sentinel}
	})
	defer closeTest(t, ctx, cc)

	result, err := cc.SetIfVersionWithTTL(ctx, "u:1", user{ID: "1", Name: "Ada"}, Version{}, time.Minute)
	if err == nil {
		t.Fatalf("SetIfVersion should return an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetIfVersion error mismatch: %v", err)
	}
	if result.Outcome != WriteOutcomeSnapshotError {
		t.Fatalf("SetIfVersion outcome=%q want %q", result.Outcome, WriteOutcomeSnapshotError)
	}

	impl := mustImpl(t, cc)
	if _, ok, _ := mp.Get(ctx, impl.singleKeys("u:1").Value.String()); ok {
		t.Fatalf("SetIfVersion should skip writes when snapshot fails")
	}
}

func TestGetSnapshotErrorTreatsInjectedEntryAsMiss(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = &failingVersionStore{snapshotErr: errors.New("snapshot failed")}
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	k := "u:1"
	payload, err := c.JSON[user]{}.Encode(user{ID: "1", Name: "Ada"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeSingle(testFence(0), payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		impl.singleKeys(k).Value.String(),
		wireEntry,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get should miss when snapshot fails, ok=%v err=%v", ok, err)
	}
}

func TestGetProviderErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("get failed")
	cc := newTestCache(
		t,
		"user",
		&getErrProvider{memProvider: newMemProvider(), err: sentinel},
		nil,
	)
	defer closeTest(t, ctx, cc)

	_, ok, err := cc.Get(ctx, "u:1")
	if err == nil {
		t.Fatalf("Get should return an error")
	}
	if ok {
		t.Fatalf("Get should not return a hit when provider get fails")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("Get error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("Get error should be *OpError, got %T", err)
	}
	if oe.Op != OpGet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpGet)
	}
	if oe.Key != "u:1" {
		t.Fatalf("OpError.Key = %q, want %q", oe.Key, "u:1")
	}
}

func TestSetIfVersionProviderErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("set failed")
	cc := newTestCache(
		t,
		"user",
		&setErrProvider{memProvider: newMemProvider(), err: sentinel},
		nil,
	)
	defer closeTest(t, ctx, cc)

	_, err := cc.SetIfVersionWithTTL(ctx, "u:1", user{ID: "1", Name: "Ada"}, Version{}, time.Minute)
	if err == nil {
		t.Fatalf("SetIfVersion should return an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetIfVersion error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetIfVersion error should be *OpError, got %T", err)
	}
	if oe.Op != OpSet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSet)
	}
	if oe.Key != "u:1" {
		t.Fatalf("OpError.Key = %q, want %q", oe.Key, "u:1")
	}
}

func TestSnapshotVersionReturnsOpError(t *testing.T) {
	ctx := context.Background()
	snapshotSentinel := errors.New("snapshot failed")
	cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
		o.VersionStore = &failingVersionStore{snapshotErr: snapshotSentinel}
	})
	defer closeTest(t, ctx, cc)

	got, err := cc.SnapshotVersion(ctx, "u:1")
	if err == nil {
		t.Fatalf("SnapshotVersion should return an error")
	}
	if !got.IsMissing() {
		t.Fatalf("SnapshotVersion should return a missing version on error, got %+v", got)
	}
	if !errors.Is(err, snapshotSentinel) {
		t.Fatalf("SnapshotVersion error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SnapshotVersion error should be *OpError, got %T", err)
	}
	if oe.Op != OpSnapshot {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSnapshot)
	}
	if oe.Key != "u:1" {
		t.Fatalf("OpError.Key = %q, want %q", oe.Key, "u:1")
	}
}

func TestOpErrorErrorContract(t *testing.T) {
	t.Run("nil_receiver", func(t *testing.T) {
		var oe *OpError
		if got := oe.Error(); got != "<nil>" {
			t.Fatalf("OpError(nil).Error() = %q, want %q", got, "<nil>")
		}
	})

	t.Run("nil_err_panics", func(t *testing.T) {
		tests := []struct {
			name string
			err  *OpError
		}{
			{name: "zero_value", err: &OpError{}},
			{name: "op_only", err: &OpError{Op: OpGet}},
			{name: "key_only", err: &OpError{Key: "k"}},
			{name: "op_and_key", err: &OpError{Op: OpGet, Key: "k"}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				defer func() {
					if recover() == nil {
						t.Fatalf("%s: OpError.Error should panic when Err is nil", tt.name)
					}
				}()
				_ = tt.err.Error()
			})
		}
	})
}

func TestSnapshotVersionsFallbackAndStrictBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("fallback_to_single_snapshots", func(t *testing.T) {
		cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
			o.VersionStore = &failingVersionStore{snapshotManyErr: errors.New("snapshot many failed")}
		})
		defer closeTest(t, ctx, cc)

		got, err := cc.SnapshotVersions(ctx, []string{"b", "a", "a"})
		if err != nil {
			t.Fatalf("SnapshotVersions: %v", err)
		}
		want := missingVersions([]string{"a", "b"})
		if !equalVersions(got, want) {
			t.Fatalf("SnapshotVersions got=%v want=%v", got, want)
		}
	})

	t.Run("returns_error_when_single_snapshot_fails", func(t *testing.T) {
		snapshotSentinel := errors.New("snapshot failed")
		cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
			o.VersionStore = &failingVersionStore{
				snapshotManyErr: errors.New("snapshot many failed"),
				snapshotErr:     snapshotSentinel,
			}
		})
		defer closeTest(t, ctx, cc)

		got, err := cc.SnapshotVersions(ctx, []string{"a", "b"})
		if err == nil {
			t.Fatalf("SnapshotVersions should return an error")
		}
		if got != nil {
			t.Fatalf("SnapshotVersions should not return partial results on error, got=%v", got)
		}
		var oe *OpError
		if !errors.As(err, &oe) {
			t.Fatalf("SnapshotVersions error should be *OpError, got %T", err)
		}
		if oe.Op != OpSnapshot {
			t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSnapshot)
		}
		if !errors.Is(err, snapshotSentinel) {
			t.Fatalf("OpError should wrap snapshot sentinel")
		}

		got, err = cc.CAS.SnapshotVersions(ctx, []string{"a", "b"})
		if err == nil {
			t.Fatalf("SnapshotVersions should keep returning an error while single snapshots fail")
		}
		if got != nil {
			t.Fatalf("SnapshotVersions should not return partial results on error, got=%v", got)
		}
	})
}

// ==============================
// Self-heal tests (corruption/version mismatch)
// ==============================

// TestSelfHealOnCorrupt ensures corrupt provider bytes are deleted and missed,
// and that a valid-but-stale single is rejected and removed.
func TestSelfHealOnCorrupt(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	k := "bad"
	sk := impl.singleKeys(k)

	// Inject corrupt bytes directly into provider.
	if ok, err := impl.provider.Set(
		ctx,
		sk.Value.String(),
		[]byte("not-wire-format"),
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject corrupt: ok=%v err=%v", ok, err)
	}

	// First Get should detect corruption, delete entry, and miss.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get on corrupt should miss, ok=%v err=%v", ok, err)
	}
	// Corrupt entry should be gone.
	if _, ok, _ := mp.Get(ctx, sk.Value.String()); ok {
		t.Fatalf("corrupt entry was not deleted by self-heal")
	}

	// Now inject a valid single, then advance the authoritative fence to make it stale.
	val := user{ID: "x", Name: "X"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeSingle(testFence(0), payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		sk.Value.String(),
		wireEntry,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject valid stale: ok=%v err=%v", ok, err)
	}
	_, _ = impl.advanceVersion(context.Background(), toVersionCacheKey(sk.Cache)) // make it stale

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get on stale single should miss, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := mp.Get(ctx, sk.Value.String()); ok {
		t.Fatalf("stale entry was not deleted by self-heal")
	}
}

func TestLocalVersionStoreCloseIdempotent(t *testing.T) {
	s := version.NewLocalWithCleanup(50*time.Millisecond, time.Second)
	defer closeTest(t, context.Background(), s)

	// Do some advances to exercise the map while cleanup may run
	for i := range 100 {
		_, _ = s.Advance(context.Background(), version.NewCacheKey(fmt.Sprintf("k%d", i)))
	}

	// Close many times
	for range 5 {
		_ = s.Close(context.Background())
	}
}

func TestLocalVersionStoreNoLeakOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	s := version.NewLocalWithCleanup(10*time.Millisecond, time.Second)
	_ = s.Close(context.Background())
	time.Sleep(20 * time.Millisecond) // give it a moment to exit
	after := runtime.NumGoroutine()
	if after > before+1 { // allow a little noise
		t.Fatalf("goroutines leaked: before=%d after=%d", before, after)
	}
}

// ==============================
// Batch behavior tests
// ==============================

// TestBatchHappyAndStale validates batch read, then invalidation of one member causes
// batch rejection and fallback to singles with missing reported for the invalidated key.
func TestBatchHappyAndStale(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b", "c"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
		"c": {ID: "c", Name: "C"},
	}

	// Snapshot versions (all missing).
	snap := mustSnapshotVersions(t, ctx, cc, keys)

	// Write the batch using the observed versions.
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	// First GetMany: all present, no missing.
	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetMany expected all hit, missing=%v got=%v", missing, got)
	}

	// Invalidate "b": removes its single and advances its fence. The batch should be rejected.
	if err := cc.Invalidate(ctx, "b"); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	got2, missing2, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany after invalidate: %v", err)
	}
	if len(missing2) != 1 || missing2[0] != "b" {
		t.Fatalf("expected only 'b' missing, got %v", missing2)
	}
	// 'a' and 'c' should still be present (from singles seeding).
	if _, ok := got2["a"]; !ok {
		t.Fatalf("expected 'a' present after batch rejection")
	}
	if _, ok := got2["c"]; !ok {
		t.Fatalf("expected 'c' present after batch rejection")
	}

	// Ensure the stale batch was dropped from provider.
	for k := range mp.m {
		if strings.HasPrefix(k, batchValuePrefix("user")) {
			t.Fatalf("stale batch should have been deleted, found %q", k)
		}
	}
}

// TestBatchDisabled ensures that when batch is disabled, no batch keys are written
// and GetMany falls back to singles.
func TestBatchDisabled(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DisableBatch = true
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"x", "y"}
	items := map[string]user{
		"x": {ID: "x", Name: "X"},
		"y": {ID: "y", Name: "Y"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, keys)

	// SetIfVersions should seed singles only when batch writes are disabled.
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions (batch disabled): %v", err)
	}

	// GetMany should return both via singles path.
	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany (batch disabled): %v", err)
	}
	if len(missing) != 0 || len(got) != 2 {
		t.Fatalf("GetMany (batch disabled) expected all present, missing=%v got=%v", missing, got)
	}

	// Assert no batch key exists in provider.
	for k := range mp.m {
		if strings.HasPrefix(k, batchValuePrefix("user")) {
			t.Fatalf("batch disabled but found batch key %q written", k)
		}
	}
}

func TestBatchDefaultNoSeed(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetMany expected all hit, missing=%v got=%v", missing, got)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); ok {
			t.Fatalf("default batch hit should not seed single %q", k)
		}
	}
}

func TestNewBatchSeedIfMissingNeedsSupport(t *testing.T) {
	_, err := New[user](Options[user]{
		Namespace:     "user",
		Provider:      &plainProvider{inner: newMemProvider()},
		Codec:         c.JSON[user]{},
		BatchReadSeed: BatchReadSeedIfMissing,
	})
	if !errors.Is(err, ErrBatchReadSeedNeedsAdder) {
		t.Fatalf("New error mismatch: %v", err)
	}
}

func TestBatchSeedAllUsesBatchSnapshot(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingVersionStore{inner: version.NewLocalWithCleanup(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
		o.BatchReadSeed = BatchReadSeedAll
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b", "c"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
		"c": {ID: "c", Name: "C"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetMany expected all hit, missing=%v got=%v", missing, got)
	}
	if gs.snapshotCalls != 0 {
		t.Fatalf("batch-hit warming should not do per-key Snapshot calls, got %d", gs.snapshotCalls)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf(
			"batch-hit validation should use one SnapshotMany call, got %d",
			gs.snapshotManyCalls,
		)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); !ok {
			t.Fatalf("checked batch-hit warming should seed single %q", k)
		}
	}
}

func TestSetIfVersionsSeedsSinglesWhenBatchReadSeedOff(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadSeed = BatchReadSeedOff
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	keys := []string{"a", "b"}
	observed := mustSnapshotVersions(t, ctx, cc, keys)

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}
	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); !ok {
			t.Fatalf("batch write should seed single %q even when BatchReadSeedOff", k)
		}
	}
}

func TestBatchSeedIfMissing(t *testing.T) {
	ctx := context.Background()
	mp := &countingAdderProvider{memProvider: newMemProvider()}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadSeed = BatchReadSeedIfMissing
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}
	mp.setCalls = 0
	mp.addCalls = 0
	mp.addStored = 0

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetMany expected all hit, missing=%v got=%v", missing, got)
	}
	if mp.setCalls != 0 {
		t.Fatalf("if-absent warming should not call Set, got %d calls", mp.setCalls)
	}
	if mp.addCalls != len(keys) {
		t.Fatalf("if-missing warming should call Add once per key, got %d", mp.addCalls)
	}
	if mp.addStored != len(keys) {
		t.Fatalf("if-missing warming should insert all missing singles, stored=%d", mp.addStored)
	}
}

func TestGetBatchReadGuardRejectsBatchAndMissesRejectedKeysWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"b": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 1 || missing[0] != "b" {
		t.Fatalf("GetMany missing=%v want [b]", missing)
	}
	if len(got) != 1 || got["a"] != items["a"] {
		t.Fatalf("GetMany got=%v want only a=%v", got, items["a"])
	}
	if _, ok := got["b"]; ok {
		t.Fatalf("GetMany should not serve batch-read-guard rejection from singles")
	}

	impl := mustImpl(t, cc)
	bk, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf(
			"batch read-guard rejection should delete stored batch, found=%v err=%v",
			found,
			err,
		)
	}
}

func TestGetBatchReadGuardRejectWithoutReadGuardPreservesDuplicateMissing(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"b": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{"a", "b"})
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if !reflect.DeepEqual(missing, []string{"b", "b"}) {
		t.Fatalf("GetMany missing=%v want [b b]", missing)
	}
	if len(got) != 1 || got["a"] != items["a"] {
		t.Fatalf("GetMany got=%v want only a=%v", got, items["a"])
	}
}

func TestGetBatchReadGuardRejectFallsBackToSinglesWithReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(_ context.Context, _ string, cached user) (bool, error) {
			return cached.Name != "stale", nil
		}
		o.BatchReadGuard = func(_ context.Context, cached map[string]user) (map[string]struct{}, error) {
			rejected := make(map[string]struct{})
			for key, value := range cached {
				if value.Name == "stale" {
					rejected[key] = struct{}{}
				}
			}
			return rejected, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "stale"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	freshB := user{ID: "b", Name: "fresh"}
	if _, err := cc.SetIfVersion(
		ctx,
		"b",
		freshB,
		mustSnapshotVersion(t, ctx, cc, "b"),
	); err != nil {
		t.Fatalf("SetIfVersion fresh single: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetMany missing=%v want none", missing)
	}
	want := map[string]user{
		"a": items["a"],
		"b": freshB,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetMany got=%v want=%v", got, want)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf(
			"batch read-guard rejection should delete stored batch, found=%v err=%v",
			found,
			err,
		)
	}
}

func TestGetBatchReadGuardErrorFailsClosedWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("batch guard failed")
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return nil, sentinel
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany got=%v want empty on batch-read-guard error without single guard", got)
	}
	if !reflect.DeepEqual(missing, keys) {
		t.Fatalf("GetMany missing=%v want=%v", missing, keys)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("batch read-guard error should delete stored batch, found=%v err=%v", found, err)
	}
}

func TestGetBatchReadGuardInvalidRejectedKeyFailsClosedWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"ghost": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany got=%v want empty on invalid batch-read-guard result", got)
	}
	if !reflect.DeepEqual(missing, keys) {
		t.Fatalf("GetMany missing=%v want=%v", missing, keys)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf(
			"invalid batch read-guard result should delete stored batch, found=%v err=%v",
			found,
			err,
		)
	}
}

func TestGetBatchReadGuardErrorFallsBackToSinglesWithReadGuard(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("batch guard failed")
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(context.Context, string, user) (bool, error) { return true, nil }
		o.BatchReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return nil, sentinel
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetMany missing=%v want none", missing)
	}
	if !reflect.DeepEqual(got, items) {
		t.Fatalf("GetMany got=%v want=%v", got, items)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("batch read-guard error should delete stored batch, found=%v err=%v", found, err)
	}
}

func TestGetBatchPropagatesSingleErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("get failed")
	mp := &getErrProvider{memProvider: newMemProvider(), err: sentinel}

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DisableBatch = true
	})
	defer closeTest(t, ctx, cc)

	got, missing, err := cc.GetMany(ctx, []string{"a", "b"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetMany error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("GetMany error should be *OpError, got %T", err)
	}
	if oe.Op != OpGet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpGet)
	}
	if oe.Key != "a" && oe.Key != "b" {
		t.Fatalf("OpError.Key = %q, want %q or %q", oe.Key, "a", "b")
	}
	if len(got) != 0 {
		t.Fatalf("GetMany should not return values on provider error, got %v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("GetMany missing mismatch: %v", missing)
	}
}

func TestGetBatchBatchReadErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("batch get failed")
	mp := &batchGetErrProvider{memProvider: newMemProvider(), err: sentinel}
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{"a", "b"})
	if err := setIfVersionsMap(ctx, cc, items, observed, time.Minute); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	got, missing, err := cc.GetMany(ctx, []string{"a", "b"})
	if err == nil {
		t.Fatalf("GetMany should return an error on batch provider read failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetMany error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("GetMany error should be *OpError, got %T", err)
	}
	if oe.Op != OpGetMany {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpGetMany)
	}
	if oe.Key != "" {
		t.Fatalf("OpError.Key = %q, want empty", oe.Key)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany should not return values when batch read fails, got %v", got)
	}
	if len(missing) != 0 {
		t.Fatalf("GetMany missing should stay empty on batch read failure, got %v", missing)
	}
	if mp.singleGetCalls != 0 {
		t.Fatalf(
			"GetMany should not fall back to singles on batch read failure, got %d single reads",
			mp.singleGetCalls,
		)
	}
}

func TestBatchValueDecodeFallsBackToSinglesAndDeletesBatch(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, []string{"a"})
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	batchKey, err := impl.batchKeySorted(sortedUnique([]string{"a"}))
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	entry, ok := mp.m[batchKey.String()]
	if !ok {
		t.Fatalf("expected batch entry %q", batchKey)
	}
	corrupt := append([]byte(nil), entry.v...)
	corrupt[len(corrupt)-1] = 0xFF
	entry.v = corrupt
	mp.m[batchKey.String()] = entry

	got, missing, err := cc.GetMany(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetMany should fall back to singles, missing=%v", missing)
	}
	if got["a"] != items["a"] {
		t.Fatalf("GetMany fallback mismatch: got=%v want=%v", got["a"], items["a"])
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); ok {
		t.Fatalf("undecodable batch should be deleted")
	}
}

func TestBatchFallbackPropagatesSingleErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("single get failed")
	mp := &singleGetErrProvider{memProvider: newMemProvider(), err: sentinel}
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{"a", "b"})
	if err := setIfVersionsMap(ctx, cc, items, observed, time.Minute); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	batchKey, err := impl.batchKeySorted(sortedUnique([]string{"a", "b"}))
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	entry, ok := mp.m[batchKey.String()]
	if !ok {
		t.Fatalf("expected batch entry %q", batchKey)
	}
	corrupt := append([]byte(nil), entry.v...)
	corrupt[len(corrupt)-1] = 0xFF
	entry.v = corrupt
	mp.m[batchKey.String()] = entry

	got, missing, err := cc.GetMany(ctx, []string{"a", "b"})
	if err == nil {
		t.Fatalf("GetMany should return an error when fallback singles fail")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetMany error mismatch: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany should not return values on fallback single error, got %v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("GetMany missing mismatch: %v", missing)
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); ok {
		t.Fatalf("undecodable batch should be deleted")
	}
	if mp.singleGetCalls != 2 {
		t.Fatalf("expected one fallback single read per unique key, got %d", mp.singleGetCalls)
	}
}

func TestBatchSnapshotManyFallbackPreservesBatchPath(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = &failingVersionStore{
			inner:           version.NewLocalWithCleanup(time.Hour, time.Hour),
			snapshotManyErr: errors.New("snapshot many failed"),
		}
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{"a", "b"})
	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	batchKey, err := impl.batchKeySorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); !ok {
		t.Fatalf("expected batch entry %q", batchKey)
	}

	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	got, missing, err := cc.GetMany(ctx, []string{"b", "a"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if got["a"] != items["a"] || got["b"] != items["b"] {
		t.Fatalf("GetMany mismatch: got=%v want=%v", got, items)
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); !ok {
		t.Fatalf("expected batch entry to remain after fallback validation")
	}
}

func TestBatchIgnoresUndecodableExtras(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	snap, created, err := impl.createSnapshot(ctx, impl.versionKey("a"))
	if err != nil {
		t.Fatalf("createSnapshot: %v", err)
	}
	if !created {
		t.Fatalf("expected createSnapshot to create authoritative state")
	}
	payload, err := c.JSON[user]{}.Encode(user{ID: "a", Name: "A"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: "a", Fence: snap.Fence, Payload: payload},
		{Key: "z", Fence: testFence(0), Payload: []byte{0xFF}},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	batchKey, err := impl.batchKeySorted([]string{"a"})
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		batchKey.String(),
		wireEntry,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject batch: ok=%v err=%v", ok, err)
	}

	got, missing, err := cc.GetMany(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if got["a"] != (user{ID: "a", Name: "A"}) {
		t.Fatalf("GetMany mismatch: got=%v", got["a"])
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); !ok {
		t.Fatalf("expected batch entry to remain when only extras are undecodable")
	}
}

func TestSetIfVersionsRejectsDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	_, err := cc.SetIfVersionsWithTTL(ctx, []VersionedValue[user]{
		{Key: "a", Value: user{ID: "a", Name: "A"}, Version: Version{}},
		{Key: "a", Value: user{ID: "a2", Name: "A2"}, Version: Version{}},
	}, time.Minute)
	if err == nil {
		t.Fatalf("SetIfVersions should reject duplicate keys")
	}
	if len(mp.m) != 0 {
		t.Fatalf("SetIfVersions should not write anything on caller error, provider=%v", mp.m)
	}
}

func TestSetIfVersionsFallbackPropagatesSingleErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("set failed")
	mp := &batchRejectSingleErrProvider{memProvider: newMemProvider(), err: sentinel}

	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := missingVersions([]string{"a", "b"})

	err := setIfVersionsMap(ctx, cc, items, observed, time.Minute)
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetIfVersions error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetIfVersions error should be *OpError, got %T", err)
	}
	if oe.Op != OpSet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSet)
	}
}

func TestSetIfVersionsBatchWriteErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("batch set failed")
	cc := newTestCache(
		t,
		"user",
		&setErrProvider{memProvider: newMemProvider(), err: sentinel},
		nil,
	)
	defer closeTest(t, ctx, cc)

	err := setIfVersionsMap(ctx, cc, map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}, missingVersions([]string{"a", "b"}), time.Minute)
	if err == nil {
		t.Fatalf("SetIfVersions should return an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetIfVersions error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetIfVersions error should be *OpError, got %T", err)
	}
	if oe.Op != OpSetIfVersions {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSetIfVersions)
	}
	if oe.Key != "" {
		t.Fatalf("OpError.Key = %q, want empty", oe.Key)
	}
}

func TestSetIfVersionsStrictUsesBatchSnapshotAndPerKeyRechecks(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingVersionStore{inner: version.NewLocalWithCleanup(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}
	if gs.snapshotCalls != len(keys) {
		t.Fatalf(
			"strict batch-write seeding should recheck each key once, got %d",
			gs.snapshotCalls,
		)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf(
			"validated batch write should use one SnapshotMany call, got %d",
			gs.snapshotManyCalls,
		)
	}
}

func TestSetIfVersionsFastUsesBatchSnapshotOnly(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingVersionStore{inner: version.NewLocalWithCleanup(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
		o.BatchWriteSeed = BatchWriteSeedFast
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}
	if gs.snapshotCalls != 0 {
		t.Fatalf(
			"fast batch-write seeding should not do per-key Snapshot calls, got %d",
			gs.snapshotCalls,
		)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf(
			"validated batch write should use one SnapshotMany call, got %d",
			gs.snapshotManyCalls,
		)
	}
}

func TestSetIfVersionsTTLOverrideAppliesToSeededSingles(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DefaultTTL = time.Hour
		o.BatchTTL = 2 * time.Hour
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	override := 50 * time.Millisecond
	before := time.Now()

	if err := setIfVersionsMap(ctx, cc, items, observed, override); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	batchKey, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	batchEntry, ok := mp.m[batchKey.String()]
	if !ok {
		t.Fatalf("expected batch entry to be written")
	}
	assertExpiryBefore(t, batchEntry.exp, before, 5*time.Second, "batch entry")

	for _, k := range keys {
		entry, ok := mp.m[impl.singleKeys(k).Value.String()]
		if !ok {
			t.Fatalf("expected single entry for %q", k)
		}
		assertExpiryBefore(t, entry.exp, before, 5*time.Second, fmt.Sprintf("single %q", k))
	}
}

func TestSetIfVersionsTTLOverrideAppliesToFallbackSingles(t *testing.T) {
	ctx := context.Background()
	mp := &batchRejectProvider{memProvider: newMemProvider()}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DefaultTTL = time.Hour
		o.BatchTTL = 2 * time.Hour
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)
	override := 50 * time.Millisecond
	before := time.Now()

	if err := setIfVersionsMap(ctx, cc, items, observed, override); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	for _, k := range keys {
		entry, ok := mp.m[impl.singleKeys(k).Value.String()]
		if !ok {
			t.Fatalf("expected fallback single entry for %q", k)
		}
		assertExpiryBefore(
			t,
			entry.exp,
			before,
			5*time.Second,
			fmt.Sprintf("fallback single %q", k),
		)
	}
}

func TestSetIfVersionsOffSkipsSinglesAfterSuccessfulBatchWrite(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BatchWriteSeed = BatchWriteSeedOff
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, keys)

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); ok {
			t.Fatalf(
				"BatchWriteSeedOff should not materialize single %q on successful batch write",
				k,
			)
		}
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetMany expected all hit, missing=%v got=%v", missing, got)
	}
}

func TestSetIfVersionsStrictSkipsSingleAfterBatchValidationRace(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &advanceAfterSnapshotManyVersionStore{
		inner:         version.NewLocalWithCleanup(time.Hour, time.Hour),
		advanceOnCall: 2,
	}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	key := "a"
	gs.advanceKey = toVersionCacheKey(impl.singleKeys(key).Cache)

	items := map[string]user{
		key: {ID: key, Name: "A"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{key})

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	if _, ok, _ := mp.Get(ctx, impl.singleKeys(key).Value.String()); ok {
		t.Fatalf("strict post-batch seeding should skip stale singles after the race")
	}
	if _, ok, err := cc.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get should miss when no checked single landed, ok=%v err=%v", ok, err)
	}
}

func TestSetIfVersionsFastRejectsFirstWriteAfterBatchValidationRace(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &advanceAfterSnapshotManyVersionStore{
		inner:         version.NewLocalWithCleanup(time.Hour, time.Hour),
		advanceOnCall: 2,
	}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
		o.BatchWriteSeed = BatchWriteSeedFast
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	key := "a"
	gs.advanceKey = toVersionCacheKey(impl.singleKeys(key).Cache)

	items := map[string]user{
		key: {ID: key, Name: "A"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{key})

	if err := setIfVersionsMap(ctx, cc, items, observed, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	if _, ok, err := mp.Get(ctx, impl.singleKeys(key).Value.String()); err != nil || ok {
		t.Fatalf("first-write race should prevent stale single from landing, ok=%v err=%v", ok, err)
	}
	if _, ok, err := cc.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get should miss after first-write race rejection, ok=%v err=%v", ok, err)
	}
}

// TestBatchOrderInsensitiveHit: Same set, different order → same batch key, batch hit.
func TestBatchOrderInsensitiveHit(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	// Write a batch for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, []string{"u1", "u3", "u4"})
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	// Remove singles so GetMany must rely on the batch entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	// Request same set, different order → should hit batch, no missing
	got, missing, err := cc.GetMany(ctx, []string{"u3", "u1", "u4"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 values, got %d (%v)", len(got), got)
	}

	// Batch should remain (valid hit)
	foundBatch := false
	for k := range mp.m {
		if strings.HasPrefix(k, batchValuePrefix("user")) {
			foundBatch = true
			break
		}
	}
	if !foundBatch {
		t.Fatalf("expected batch entry to remain after valid hit")
	}
}

// TestBatchDuplicateRequestHit: Request has duplicates → still hits unique-set batch.
func TestBatchDuplicateRequestHit(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	// Write a batch for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := mustSnapshotVersions(t, ctx, cc, []string{"u1", "u3", "u4"})
	if err := setIfVersionsMap(ctx, cc, items, snap, 0); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	// Remove singles so GetMany must rely on the batch entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	// Request contains duplicates → should still hit the same batch key
	req := []string{"u1", "u3", "u3", "u4"}
	got, missing, err := cc.GetMany(ctx, req)
	if err != nil {
		t.Fatalf("GetMany dup: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing for dup request, got %v", missing)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 unique results, got %d (%v)", len(got), got)
	}
}

// TestBatchKeyCanonicalization: equal sets (order/dups ignored) produce same batch key.
func TestBatchKeyCanonicalization(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	k1, err := impl.batchKeySorted(sortedUnique([]string{"u3", "u1", "u4"}))
	if err != nil {
		t.Fatalf("batchKeySorted k1: %v", err)
	}
	k2, err := impl.batchKeySorted(sortedUnique([]string{"u1", "u3", "u3", "u4"}))
	if err != nil {
		t.Fatalf("batchKeySorted k2: %v", err)
	}
	if k1 != k2 {
		t.Fatalf("batch keys differ for equivalent sets: %q vs %q", k1, k2)
	}
}

// ==============================
// Wire format tests
// ==============================

// DecodeSingle must reject trailing bytes (strict framing).
func TestWireDecodeSingleRejectsTrailing(t *testing.T) {
	b, err := wire.EncodeSingle(testFence(7), []byte("x"))
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	b = append(b, 0xDE, 0xAD) // trailing junk
	if _, _, err := wire.DecodeSingle(b); err == nil {
		t.Fatalf("DecodeSingle should reject trailing bytes")
	}
}

// DecodeBatch must reject trailing bytes (strict framing).
func TestWireDecodeBatchRejectsTrailing(t *testing.T) {
	enc, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: "k", Fence: testFence(1), Payload: []byte("v")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	enc = append(enc, 0xBE, 0xEF)
	if _, err := wire.DecodeBatch(enc); err == nil {
		t.Fatalf("DecodeBatch should reject trailing bytes")
	}
}

// EncodeBatch should error on invalid key lengths (0 and > 0xFFFF),
// and succeed on boundary length 0xFFFF.
func TestEncodeBatchKeyLengthValidation(t *testing.T) {
	// Empty key -> error
	if _, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: "", Fence: testFence(1), Payload: []byte("x")},
	}); err == nil {
		t.Fatalf("EncodeBatch should error on empty key")
	}

	// Too long key (65536) -> error
	longKey := strings.Repeat("a", 0x10000)
	if _, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: longKey, Fence: testFence(1), Payload: []byte("x")},
	}); err == nil {
		t.Fatalf("EncodeBatch should error on key length > 0xFFFF")
	}

	// Boundary (65535) -> ok
	boundaryKey := strings.Repeat("b", 0xFFFF)
	if _, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: boundaryKey, Fence: testFence(1), Payload: []byte("x")},
	}); err != nil {
		t.Fatalf("EncodeBatch should succeed at 0xFFFF key length, got err: %v", err)
	}
}

// Bogus n in batch header should not preallocate huge capacity and should error cleanly.
func TestDecodeBatchFakeNNotPrealloc(t *testing.T) {
	var buf bytes.Buffer
	// magic "CASC"
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	// version
	buf.WriteByte(1)
	// kind batch
	buf.WriteByte(2)
	// n = 0xFFFFFFFF
	var u4 [4]byte
	binary.BigEndian.PutUint32(u4[:], ^uint32(0))
	buf.Write(u4[:])
	// no items

	if _, err := wire.DecodeBatch(buf.Bytes()); err == nil {
		t.Fatalf("DecodeBatch should fail on wrong n with insufficient bytes")
	}
}

// ==============================
// Wire and version-validation tests
// ==============================

// Self-heal when a valid single has trailing bytes appended in the provider.
func TestSelfHealOnVersionMismatchSingle(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	k := "version-mismatch"
	storageKey := impl.singleKeys(k).Value

	// VersionStore has never been initialized for this key, so any live fence is stale.
	val := user{ID: "u1", Name: "Mismatch"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Write a valid frame with a fence that does not match authoritative state.
	b, err := wire.EncodeSingle(testFence(1), payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		storageKey.String(),
		b,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	// Get should detect the version mismatch, delete, and miss.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("expected miss on version mismatch, ok=%v err=%v", ok, err)
	}

	// Ensure self-heal actually deleted the bad entry.
	if _, ok, _ := mp.Get(ctx, storageKey.String()); ok {
		t.Fatalf("version-mismatch single was not deleted by self-heal")
	}
}

func TestSelfHealOnEpochMismatchSingle(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	k := "epoch-mismatch"
	storageKey := impl.singleKeys(k).Value

	snap, created, err := impl.createSnapshot(ctx, impl.versionKey(k))
	if err != nil {
		t.Fatalf("createSnapshot: %v", err)
	}
	if !created {
		t.Fatal("expected createSnapshot to create authoritative state")
	}

	val := user{ID: "u2", Name: "EpochMismatch"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	staleFence := mutateFence(snap.Fence, func(b []byte) {
		b[0] ^= 0xFF
	})
	b, err := wire.EncodeSingle(staleFence, payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		storageKey.String(),
		b,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("expected miss on epoch mismatch, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := mp.Get(ctx, storageKey.String()); ok {
		t.Fatalf("epoch-mismatch single was not deleted by self-heal")
	}
}

func TestBatchRejectReasonFreshnessChecks(t *testing.T) {
	ctx := context.Background()

	newImpl := func(t *testing.T) *cache[user] {
		t.Helper()
		mp := newMemProvider()
		cc := newTestCache(t, "user", mp, nil)
		t.Cleanup(func() { _ = cc.Close(ctx) })
		return mustImpl(t, cc)
	}

	// helper: advance to exactly 'n'
	bumpTo := func(impl *cache[user], ukey string, n uint64) {
		sk := impl.singleKeys(ukey).Cache
		for range n {
			_, _ = impl.advanceVersion(ctx, toVersionCacheKey(sk))
		}
	}

	t.Run("valid_all_members_fresh", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"} // already sorted

		// Advance each key once so every member has live authoritative state.
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}
		snaps := loadSnapshotsByKey(t, ctx, impl, keys)

		items := []wire.BatchItem{
			{Key: "a", Fence: snaps["a"].Fence, Payload: nil},
			{Key: "b", Fence: snaps["b"].Fence, Payload: nil},
			{Key: "c", Fence: snaps["c"].Fence, Payload: nil},
		}
		reason, err := impl.batchRejectReason(ctx, keys, items)
		if err != nil {
			t.Fatalf("batchRejectReason: %v", err)
		}
		if reason != "" {
			t.Fatalf("batchRejectReason = %q, want empty for fresh members", reason)
		}
	})

	t.Run("missing_member_in_batch", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"}

		// Advance each key once so every member has live authoritative state.
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}
		snaps := loadSnapshotsByKey(t, ctx, impl, keys)

		// omit "b" from items
		items := []wire.BatchItem{
			{Key: "a", Fence: snaps["a"].Fence, Payload: nil},
			{Key: "c", Fence: snaps["c"].Fence, Payload: nil},
		}
		reason, err := impl.batchRejectReason(ctx, keys, items)
		if err != nil {
			t.Fatalf("batchRejectReason: %v", err)
		}
		if reason != BatchRejectReasonIncompleteBatch {
			t.Fatalf(
				"batchRejectReason = %q, want %q when a requested member is missing",
				reason,
				BatchRejectReasonIncompleteBatch,
			)
		}
	})

	t.Run("stale_member_version_mismatch", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b", "c"}

		// Advance each key once so every member has live authoritative state.
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}
		snaps := loadSnapshotsByKey(t, ctx, impl, keys)

		staleFence := mutateFence(snaps["b"].Fence, func(b []byte) {
			b[len(b)-1] ^= 0x01
		})
		items := []wire.BatchItem{
			{Key: "a", Fence: snaps["a"].Fence, Payload: nil},
			{Key: "b", Fence: staleFence, Payload: nil}, // stale
			{Key: "c", Fence: snaps["c"].Fence, Payload: nil},
		}
		reason, err := impl.batchRejectReason(ctx, keys, items)
		if err != nil {
			t.Fatalf("batchRejectReason: %v", err)
		}
		if reason != BatchRejectReasonVersionMismatch {
			t.Fatalf(
				"batchRejectReason = %q, want %q when any member is stale",
				reason,
				BatchRejectReasonVersionMismatch,
			)
		}
	})

	t.Run("extra_member_ignored", func(t *testing.T) {
		impl := newImpl(t)
		keys := []string{"a", "b"}

		// Advance each key once so every member has live authoritative state.
		for _, k := range keys {
			bumpTo(impl, k, 1)
		}
		snaps := loadSnapshotsByKey(t, ctx, impl, keys)

		// Include an extra "z" that isn't requested. Should be ignored.
		items := []wire.BatchItem{
			{Key: "a", Fence: snaps["a"].Fence, Payload: nil},
			{Key: "b", Fence: snaps["b"].Fence, Payload: nil},
			{Key: "z", Fence: testFence(999), Payload: nil}, // extra
		}
		reason, err := impl.batchRejectReason(ctx, keys, items)
		if err != nil {
			t.Fatalf("batchRejectReason: %v", err)
		}
		if reason != "" {
			t.Fatalf("batchRejectReason = %q, want empty when extras are ignored", reason)
		}
	})
}

func TestGetManyRejectsEpochMismatchBatch(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}

	snapA, created, err := impl.createSnapshot(ctx, impl.versionKey("a"))
	if err != nil {
		t.Fatalf("createSnapshot a: %v", err)
	}
	if !created {
		t.Fatal("expected createSnapshot to create a")
	}
	snapB, created, err := impl.createSnapshot(ctx, impl.versionKey("b"))
	if err != nil {
		t.Fatalf("createSnapshot b: %v", err)
	}
	if !created {
		t.Fatal("expected createSnapshot to create b")
	}

	payloadA, err := c.JSON[user]{}.Encode(user{ID: "a", Name: "A"})
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	payloadB, err := c.JSON[user]{}.Encode(user{ID: "b", Name: "B"})
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}

	staleFence := mutateFence(snapB.Fence, func(b []byte) {
		b[0] ^= 0xFF
	})
	batchWire, err := wire.EncodeBatch([]wire.BatchItem{
		{Key: "a", Fence: snapA.Fence, Payload: payloadA},
		{Key: "b", Fence: staleFence, Payload: payloadB},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	batchKey, err := impl.batchKeySorted(keys)
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	if ok, err := impl.provider.Set(
		ctx,
		batchKey.String(),
		batchWire,
		1,
		time.Minute,
	); err != nil ||
		!ok {
		t.Fatalf("inject batch: ok=%v err=%v", ok, err)
	}

	got, missing, err := cc.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany should not serve mismatched batch, got=%v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("missing = %v, want [a b]", missing)
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); ok {
		t.Fatalf("epoch-mismatch batch was not deleted")
	}
}

func TestBatchValidationSnapshotErrorLeavesBatchEntry(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &failingVersionStore{inner: version.NewLocalWithCleanup(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = gs
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := mustSnapshotVersions(t, ctx, cc, []string{"a", "b"})
	if err := setIfVersionsMap(ctx, cc, items, observed, time.Minute); err != nil {
		t.Fatalf("SetIfVersions: %v", err)
	}

	batchKey, err := impl.batchKeySorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("batchKeySorted: %v", err)
	}
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	gs.snapshotManyErr = errors.New("snapshot many failed")
	gs.snapshotErr = errors.New("snapshot failed")

	got, missing, err := cc.GetMany(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetMany should not serve values during snapshot outage, got=%v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("missing = %v, want [a b]", missing)
	}
	if _, ok, _ := mp.Get(ctx, batchKey.String()); !ok {
		t.Fatalf("batch entry should remain when version validation is unavailable")
	}
}

func equalVersions(a, b map[string]Version) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		other, ok := b[k]
		if !ok || !v.Equal(other) {
			return false
		}
	}
	return true
}

// Covers: empty input, duplicates, missing versions, and mixed live snapshots.
func TestSnapshotVersionsBehavior(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	t.Cleanup(func() { _ = cc.Close(ctx) })
	impl := mustImpl(t, cc)

	t.Run("empty", func(t *testing.T) {
		got := mustSnapshotVersions(t, ctx, cc, nil)
		if len(got) != 0 {
			t.Fatalf("empty: expected empty map, got %v", got)
		}
	})

	t.Run("duplicates_and_missing", func(t *testing.T) {
		// No authoritative state exists yet, so every key is missing.
		keys := []string{"dupa", "dupa", "other"}
		got := mustSnapshotVersions(t, ctx, cc, keys)
		want := missingVersions(keys)
		if !equalVersions(got, want) {
			t.Fatalf("duplicates/missing: got %v want %v", got, want)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		// m1 and m3 should be live; m2 should still be missing.
		_, _ = impl.advanceVersion(ctx, toVersionCacheKey(impl.singleKeys("m1").Cache))
		for range 3 {
			_, _ = impl.advanceVersion(ctx, toVersionCacheKey(impl.singleKeys("m3").Cache))
		}
		keys := []string{"m1", "m2", "m3", "m1"} // include duplicate
		got := mustSnapshotVersions(t, ctx, cc, keys)
		if got["m1"].IsMissing() || !got["m2"].IsMissing() || got["m3"].IsMissing() {
			t.Fatalf("mixed: got %v, want m1/live m2/missing m3/live", got)
		}
	})
}

func TestNewUsesConfiguredKeyWriterForSetIfVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mp := newMemProvider()
	keyAdapter := &recordingKeyAdapter{}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.KeyWriter = keyAdapter
	})
	defer closeTest(t, ctx, cc)

	key := "u:1"
	value := user{ID: "1", Name: "Ada"}
	result, err := cc.CAS.SetIfVersionWithTTL(ctx, key, value, Version{}, 5*time.Second)
	if err != nil {
		t.Fatalf("SetIfVersion: %v", err)
	}
	if result.Outcome != WriteOutcomeVersionMismatch {
		t.Fatalf("SetIfVersion outcome = %q, want %q", result.Outcome, WriteOutcomeVersionMismatch)
	}
	if keyAdapter.setCalls != 1 {
		t.Fatalf("SetIfVersion should call configured KeyWriter once, got %d", keyAdapter.setCalls)
	}
	if len(mp.m) != 0 {
		t.Fatalf("provider should not be written directly when KeyWriter is configured")
	}

	impl := mustImpl(t, cc)
	sk := impl.singleKeys(key)
	if keyAdapter.lastVersionKey != toVersionCacheKey(sk.Cache) {
		t.Fatalf(
			"version key mismatch: got %q want %q",
			keyAdapter.lastVersionKey,
			toVersionCacheKey(sk.Cache),
		)
	}
	if keyAdapter.lastValueKey != sk.Value.String() {
		t.Fatalf("value key mismatch: got %q want %q", keyAdapter.lastValueKey, sk.Value.String())
	}
	if keyAdapter.lastExpected.Exists {
		t.Fatalf("expected missing observed version, got %+v", keyAdapter.lastExpected)
	}
	if len(keyAdapter.lastPayload) == 0 {
		t.Fatalf("expected encoded payload to be passed to KeyWriter")
	}
}

func TestNewUsesConfiguredKeyInvalidatorForInvalidate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sentinel := errors.New("atomic invalidate failed")
	keyAdapter := &recordingKeyAdapter{invalidateErr: sentinel}
	cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
		o.KeyInvalidator = keyAdapter
	})
	defer closeTest(t, ctx, cc)

	key := "u:invalidate"
	err := cc.Invalidate(ctx, key)
	if err == nil {
		t.Fatal("Invalidate should return the KeyInvalidator error")
	}
	var invErr *InvalidateError
	if !errors.As(err, &invErr) {
		t.Fatalf("Invalidate error should wrap InvalidateError, got %T", err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("Invalidate error should wrap configured KeyInvalidator error, got %v", err)
	}
	if keyAdapter.invalidateCalls != 1 {
		t.Fatalf(
			"Invalidate should call configured KeyInvalidator once, got %d",
			keyAdapter.invalidateCalls,
		)
	}

	impl := mustImpl(t, cc)
	sk := impl.singleKeys(key)
	if keyAdapter.lastVersionKey != toVersionCacheKey(sk.Cache) {
		t.Fatalf(
			"version key mismatch: got %q want %q",
			keyAdapter.lastVersionKey,
			toVersionCacheKey(sk.Cache),
		)
	}
	if keyAdapter.lastValueKey != sk.Value.String() {
		t.Fatalf("value key mismatch: got %q want %q", keyAdapter.lastValueKey, sk.Value.String())
	}
}

// ==============================
// Invalidate edge-case behavior (cluster down etc.)
// ==============================

type failingVersionStore struct {
	inner           version.Store
	snapshotErr     error
	snapshotManyErr error
	advanceErr      error
}

func (s *failingVersionStore) Snapshot(
	ctx context.Context,
	key version.CacheKey,
) (version.Snapshot, error) {
	if s.snapshotErr != nil {
		return version.Snapshot{}, s.snapshotErr
	}
	if s.inner != nil {
		return s.inner.Snapshot(ctx, key)
	}
	return version.Snapshot{}, nil
}

func (s *failingVersionStore) SnapshotMany(
	ctx context.Context,
	keys []version.CacheKey,
) (map[version.CacheKey]version.Snapshot, error) {
	if s.snapshotManyErr != nil {
		return nil, s.snapshotManyErr
	}
	if s.inner != nil {
		return s.inner.SnapshotMany(ctx, keys)
	}
	return map[version.CacheKey]version.Snapshot{}, nil
}

func (s *failingVersionStore) CreateIfMissing(
	ctx context.Context,
	key version.CacheKey,
) (version.Snapshot, bool, error) {
	if s.snapshotErr != nil {
		return version.Snapshot{}, false, s.snapshotErr
	}
	if s.inner != nil {
		return s.inner.CreateIfMissing(ctx, key)
	}
	return version.Snapshot{}, true, nil
}

func (s *failingVersionStore) Advance(
	ctx context.Context,
	key version.CacheKey,
) (version.Snapshot, error) {
	if s.advanceErr != nil {
		return version.Snapshot{}, s.advanceErr
	}
	if s.inner != nil {
		return s.inner.Advance(ctx, key)
	}
	return version.Snapshot{}, nil
}
func (s *failingVersionStore) Cleanup(time.Duration) {}
func (s *failingVersionStore) Close(ctx context.Context) error {
	if s.inner != nil {
		return s.inner.Close(ctx)
	}
	return nil
}

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
	advanceFail := errors.New("advance failed")

	cc := newTestCache(
		t,
		"user",
		&delErrProvider{memProvider: mp, err: sentinelDelErr},
		func(o *Options[user]) {
			o.VersionStore = &failingVersionStore{advanceErr: advanceFail}
		},
	)
	defer closeTest(t, ctx, cc)

	err := cc.Invalidate(ctx, "k1")
	if err == nil {
		t.Fatalf("expected error when both advance and delete fail")
	}
	var ie *InvalidateError
	if !errors.As(err, &ie) {
		t.Fatalf("expected InvalidateError, got %T: %v", err, err)
	}
	var advanceOpErr *OpError
	if !errors.As(ie.AdvanceErr, &advanceOpErr) {
		t.Fatalf("expected advance error to be *OpError, got %T", ie.AdvanceErr)
	}
	if advanceOpErr.Op != OpInvalidate || advanceOpErr.Key != "k1" {
		t.Fatalf("unexpected advance OpError: %+v", advanceOpErr)
	}
	var delOpErr *OpError
	if !errors.As(ie.DelErr, &delOpErr) {
		t.Fatalf("expected delete error to be *OpError, got %T", ie.DelErr)
	}
	if delOpErr.Op != OpInvalidate || delOpErr.Key != "k1" {
		t.Fatalf("unexpected delete OpError: %+v", delOpErr)
	}
	// Unwrap should expose underlying delete error.
	if !errors.Is(err, sentinelDelErr) {
		t.Fatalf("expected errors.Is(err, delErr) to be true")
	}
	if !errors.Is(err, advanceFail) {
		t.Fatalf("expected errors.Is(err, advanceErr) to be true")
	}
}

func TestInvalidateAdvanceFailDeleteOKReturnsError(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	sentinel := errors.New("advance failed")

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.VersionStore = &failingVersionStore{advanceErr: sentinel}
	})
	defer closeTest(t, ctx, cc)

	err := cc.Invalidate(ctx, "k2")
	if err == nil {
		t.Fatalf("expected error when advance fails, even if delete succeeds")
	}
	var ie *InvalidateError
	if !errors.As(err, &ie) {
		t.Fatalf("expected InvalidateError, got %T: %v", err, err)
	}
	if ie.DelErr != nil {
		t.Fatalf("expected delete error to be nil, got %v", ie.DelErr)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected errors.Is(err, advanceErr) to be true")
	}
}

func TestInvalidateAdvanceOKDeleteFailNoError(t *testing.T) {
	ctx := context.Background()
	sentinelDelErr := errors.New("del failed")
	// normal version store (local), provider delete fails
	mp := &delErrProvider{memProvider: newMemProvider(), err: sentinelDelErr}

	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	// Warm a version so advance definitely succeeds.
	impl := mustImpl(t, cc)
	_, _ = impl.advanceVersion(ctx, toVersionCacheKey(impl.singleKeys("k3").Cache))

	if err := cc.Invalidate(ctx, "k3"); err != nil {
		t.Fatalf("expected no error when delete fails but advance succeeds; got %v", err)
	}
}
