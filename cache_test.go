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

	c "github.com/unkn0wn-root/cascache/codec"
	"github.com/unkn0wn-root/cascache/genstore"
	"github.com/unkn0wn-root/cascache/internal/wire"
	pr "github.com/unkn0wn-root/cascache/provider"
)

type memEntry struct {
	v   []byte
	exp time.Time // zero => no TTL
}

const bulkValueRoot = "cas:v1:val:b:"

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

func (p *memProvider) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	p.m[key] = memEntry{v: value, exp: exp}
	return true, nil
}

func (p *memProvider) Add(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
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

type bulkGetErrProvider struct {
	*memProvider
	err            error
	singleGetCalls int
}

var _ pr.Provider = (*bulkGetErrProvider)(nil)

func (p *bulkGetErrProvider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if strings.HasPrefix(key, bulkValueRoot) {
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
	if strings.HasPrefix(key, bulkValueRoot) {
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

func (p *setErrProvider) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	return false, p.err
}

type plainProvider struct {
	inner *memProvider
}

var _ pr.Provider = (*plainProvider)(nil)

func (p *plainProvider) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return p.inner.Get(ctx, key)
}

func (p *plainProvider) Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	return p.inner.Set(ctx, key, value, cost, ttl)
}

func (p *plainProvider) Del(ctx context.Context, key string) error {
	return p.inner.Del(ctx, key)
}

func (p *plainProvider) Close(ctx context.Context) error {
	return p.inner.Close(ctx)
}

type bulkRejectSingleErrProvider struct {
	*memProvider
	err error
}

var _ pr.Provider = (*bulkRejectSingleErrProvider)(nil)

func (p *bulkRejectSingleErrProvider) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if strings.HasPrefix(key, bulkValueRoot) {
		return false, nil
	}
	return false, p.err
}

type bulkRejectProvider struct {
	*memProvider
}

var _ pr.Provider = (*bulkRejectProvider)(nil)

func (p *bulkRejectProvider) Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	if strings.HasPrefix(key, bulkValueRoot) {
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

func (p *countingAdderProvider) Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	p.setCalls++
	return p.memProvider.Set(ctx, key, value, cost, ttl)
}

func (p *countingAdderProvider) Add(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (bool, error) {
	p.addCalls++
	stored, err := p.memProvider.Add(ctx, key, value, cost, ttl)
	if stored {
		p.addStored++
	}
	return stored, err
}

type countingGenStore struct {
	inner             genstore.GenStore
	snapshotCalls     int
	snapshotManyCalls int
}

var _ genstore.GenStore = (*countingGenStore)(nil)

func (s *countingGenStore) Snapshot(ctx context.Context, k genstore.CacheKey) (uint64, error) {
	s.snapshotCalls++
	return s.inner.Snapshot(ctx, k)
}

func (s *countingGenStore) SnapshotMany(ctx context.Context, ks []genstore.CacheKey) (map[genstore.CacheKey]uint64, error) {
	s.snapshotManyCalls++
	return s.inner.SnapshotMany(ctx, ks)
}

func (s *countingGenStore) Bump(ctx context.Context, k genstore.CacheKey) (uint64, error) {
	return s.inner.Bump(ctx, k)
}

func (s *countingGenStore) Cleanup(retention time.Duration) {
	s.inner.Cleanup(retention)
}

func (s *countingGenStore) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

type bumpAfterSnapshotManyGenStore struct {
	inner            genstore.GenStore
	bumpKey          genstore.CacheKey
	bumpOnCall       int
	snapshotManyCall int
	bumped           bool
}

var _ genstore.GenStore = (*bumpAfterSnapshotManyGenStore)(nil)

func (s *bumpAfterSnapshotManyGenStore) Snapshot(ctx context.Context, k genstore.CacheKey) (uint64, error) {
	return s.inner.Snapshot(ctx, k)
}

func (s *bumpAfterSnapshotManyGenStore) SnapshotMany(ctx context.Context, ks []genstore.CacheKey) (map[genstore.CacheKey]uint64, error) {
	got, err := s.inner.SnapshotMany(ctx, ks)
	s.snapshotManyCall++
	if err != nil || s.bumped || s.bumpKey == (genstore.CacheKey{}) || s.snapshotManyCall != s.bumpOnCall {
		return got, err
	}
	s.bumped = true
	if _, bumpErr := s.inner.Bump(ctx, s.bumpKey); bumpErr != nil {
		return nil, bumpErr
	}
	return got, nil
}

func (s *bumpAfterSnapshotManyGenStore) Bump(ctx context.Context, k genstore.CacheKey) (uint64, error) {
	return s.inner.Bump(ctx, k)
}

func (s *bumpAfterSnapshotManyGenStore) Cleanup(retention time.Duration) {
	s.inner.Cleanup(retention)
}

func (s *bumpAfterSnapshotManyGenStore) Close(ctx context.Context) error {
	return s.inner.Close(ctx)
}

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

func closeTest(t *testing.T, ctx context.Context, c interface{ Close(context.Context) error }) {
	t.Helper()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func assertExpiryBefore(t *testing.T, got time.Time, before time.Time, limit time.Duration, label string) {
	t.Helper()
	if got.IsZero() {
		t.Fatalf("%s should have a TTL", label)
	}
	if !got.Before(before.Add(limit)) {
		t.Fatalf("%s expiry too far out: got %v, want before %v", label, got, before.Add(limit))
	}
}

func mustImpl[V any](t *testing.T, c CAS[V]) *cache[V] {
	t.Helper()
	impl, ok := c.(*cache[V])
	if !ok {
		t.Fatalf("unexpected concrete type for CAS")
	}
	return impl
}

func bulkValuePrefix(namespace string) string {
	return fmt.Sprintf("%s%d:%s:", bulkValueRoot, len(namespace), namespace)
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

	// CAS write with observed gen 0.
	obs := cc.SnapshotGen(ctx, k)
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
	obs2 := cc.SnapshotGen(ctx, k)
	if err := cc.SetWithGen(ctx, k, v, obs2, 0); err != nil {
		t.Fatalf("SetWithGen (fresh): %v", err)
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
	if err := cc.SetWithGen(ctx, k, v, cc.SnapshotGen(ctx, k), 0); err != nil {
		t.Fatalf("SetWithGen: %v", err)
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
	if err := cc.SetWithGen(ctx, k, v, cc.SnapshotGen(ctx, k), 0); err != nil {
		t.Fatalf("SetWithGen: %v", err)
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
	gs := genstore.NewLocalGenStore(time.Hour, time.Hour)

	left := newTestCache(t, "app:prod", mp, func(o *Options[user]) {
		o.GenStore = gs
	})
	defer closeTest(t, ctx, left)

	right := newTestCache(t, "app", mp, func(o *Options[user]) {
		o.GenStore = gs
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
	if err := left.SetWithGen(ctx, "users:42", leftVal, 0, time.Minute); err != nil {
		t.Fatalf("left SetWithGen: %v", err)
	}
	if err := right.SetWithGen(ctx, "prod:users:42", rightVal, 0, time.Minute); err != nil {
		t.Fatalf("right SetWithGen: %v", err)
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

func TestSetWithGenSnapshotErrorSkipsWrite(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = &failingGenStore{snapshotErr: errors.New("snapshot failed")}
	})
	defer closeTest(t, ctx, cc)

	if err := cc.SetWithGen(ctx, "u:1", user{ID: "1", Name: "Ada"}, 0, time.Minute); err != nil {
		t.Fatalf("SetWithGen: %v", err)
	}

	impl := mustImpl(t, cc)
	if _, ok, _ := mp.Get(ctx, impl.singleKeys("u:1").Value.String()); ok {
		t.Fatalf("SetWithGen should skip writes when snapshot fails")
	}
}

func TestGetSnapshotErrorTreatsGenZeroEntryAsMiss(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = &failingGenStore{snapshotErr: errors.New("snapshot failed")}
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	k := "u:1"
	payload, err := c.JSON[user]{}.Encode(user{ID: "1", Name: "Ada"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeSingle(0, payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(ctx, impl.singleKeys(k).Value.String(), wireEntry, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get should miss when snapshot fails, ok=%v err=%v", ok, err)
	}
}

func TestGetProviderErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("get failed")
	cc := newTestCache(t, "user", &getErrProvider{memProvider: newMemProvider(), err: sentinel}, nil)
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

func TestSetWithGenProviderErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("set failed")
	cc := newTestCache(t, "user", &setErrProvider{memProvider: newMemProvider(), err: sentinel}, nil)
	defer closeTest(t, ctx, cc)

	err := cc.SetWithGen(ctx, "u:1", user{ID: "1", Name: "Ada"}, 0, time.Minute)
	if err == nil {
		t.Fatalf("SetWithGen should return an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetWithGen error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetWithGen error should be *OpError, got %T", err)
	}
	if oe.Op != OpSet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSet)
	}
	if oe.Key != "u:1" {
		t.Fatalf("OpError.Key = %q, want %q", oe.Key, "u:1")
	}
}

func TestTrySnapshotGenReturnsError(t *testing.T) {
	ctx := context.Background()
	snapshotSentinel := errors.New("snapshot failed")
	cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
		o.GenStore = &failingGenStore{snapshotErr: snapshotSentinel}
	})
	defer closeTest(t, ctx, cc)

	got, err := cc.TrySnapshotGen(ctx, "u:1")
	if err == nil {
		t.Fatalf("TrySnapshotGen should return an error")
	}
	if got != 0 {
		t.Fatalf("TrySnapshotGen got=%d want=0", got)
	}
	if !errors.Is(err, snapshotSentinel) {
		t.Fatalf("TrySnapshotGen error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("TrySnapshotGen error should be *OpError, got %T", err)
	}
	if oe.Op != OpSnapshot {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSnapshot)
	}
	if oe.Key != "u:1" {
		t.Fatalf("OpError.Key = %q, want %q", oe.Key, "u:1")
	}
	if got := cc.SnapshotGen(ctx, "u:1"); got != 0 {
		t.Fatalf("SnapshotGen got=%d want=0", got)
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

func TestTrySnapshotGensFallbackAndStrictBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("fallback_to_single_snapshots", func(t *testing.T) {
		cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
			o.GenStore = &failingGenStore{snapshotManyErr: errors.New("snapshot many failed")}
		})
		defer closeTest(t, ctx, cc)

		got, err := cc.TrySnapshotGens(ctx, []string{"b", "a", "a"})
		if err != nil {
			t.Fatalf("TrySnapshotGens: %v", err)
		}
		want := map[string]uint64{"a": 0, "b": 0}
		if !equalU64(got, want) {
			t.Fatalf("TrySnapshotGens got=%v want=%v", got, want)
		}
	})

	t.Run("returns_error_when_single_snapshot_fails", func(t *testing.T) {
		snapshotSentinel := errors.New("snapshot failed")
		cc := newTestCache(t, "user", newMemProvider(), func(o *Options[user]) {
			o.GenStore = &failingGenStore{
				snapshotManyErr: errors.New("snapshot many failed"),
				snapshotErr:     snapshotSentinel,
			}
		})
		defer closeTest(t, ctx, cc)

		got, err := cc.TrySnapshotGens(ctx, []string{"a", "b"})
		if err == nil {
			t.Fatalf("TrySnapshotGens should return an error")
		}
		if got != nil {
			t.Fatalf("TrySnapshotGens should not return partial results on error, got=%v", got)
		}
		var oe *OpError
		if !errors.As(err, &oe) {
			t.Fatalf("TrySnapshotGens error should be *OpError, got %T", err)
		}
		if oe.Op != OpSnapshot {
			t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSnapshot)
		}
		if !errors.Is(err, snapshotSentinel) {
			t.Fatalf("OpError should wrap snapshot sentinel")
		}

		want := map[string]uint64{"a": 0, "b": 0}
		if got := cc.SnapshotGens(ctx, []string{"a", "b"}); !equalU64(got, want) {
			t.Fatalf("SnapshotGens got=%v want=%v", got, want)
		}
	})
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
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	k := "bad"
	sk := impl.singleKeys(k)

	// Inject corrupt bytes directly into provider.
	if ok, err := impl.provider.Set(ctx, sk.Value.String(), []byte("not-wire-format"), 1, time.Minute); err != nil || !ok {
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

	// Now inject a valid single with gen=0, then bump generation to make it stale.
	val := user{ID: "x", Name: "X"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeSingle(0, payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(ctx, sk.Value.String(), wireEntry, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject valid stale: ok=%v err=%v", ok, err)
	}
	_, _ = impl.bumpGen(context.Background(), toGenStoreKey(sk.Cache)) // make it stale

	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("Get on stale single should miss, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := mp.Get(ctx, sk.Value.String()); ok {
		t.Fatalf("stale entry was not deleted by self-heal")
	}
}

func TestLocalGenStoreCloseIdempotent(t *testing.T) {
	s := genstore.NewLocalGenStore(50*time.Millisecond, time.Second)
	defer closeTest(t, context.Background(), s)

	// Do some bumps to exercise the map while cleanup may run
	for i := range 100 {
		_, _ = s.Bump(context.Background(), genstore.NewCacheKey(fmt.Sprintf("k%d", i)))
	}

	// Close many times
	for range 5 {
		_ = s.Close(context.Background())
	}
}

func TestLocalGenStoreNoLeakOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	s := genstore.NewLocalGenStore(10*time.Millisecond, time.Second)
	_ = s.Close(context.Background())
	time.Sleep(20 * time.Millisecond) // give it a moment to exit
	after := runtime.NumGoroutine()
	if after > before+1 { // allow a little noise
		t.Fatalf("goroutines leaked: before=%d after=%d", before, after)
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
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b", "c"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
		"c": {ID: "c", Name: "C"},
	}

	// Snapshot gens (all zero).
	snap := cc.SnapshotGens(ctx, keys)

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
		if strings.HasPrefix(k, bulkValuePrefix("user")) {
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
	defer closeTest(t, ctx, cc)

	keys := []string{"x", "y"}
	items := map[string]user{
		"x": {ID: "x", Name: "X"},
		"y": {ID: "y", Name: "Y"},
	}
	snap := cc.SnapshotGens(ctx, keys)

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

	// Assert no bulk key exists in provider.
	for k := range mp.m {
		if strings.HasPrefix(k, bulkValuePrefix("user")) {
			t.Fatalf("bulk disabled but found bulk key %q written", k)
		}
	}
}

func TestBulkDefaultNoSeed(t *testing.T) {
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
	snap := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetBulk expected all hit, missing=%v got=%v", missing, got)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); ok {
			t.Fatalf("default bulk hit should not seed single %q", k)
		}
	}
}

func TestNewBulkSeedIfMissingNeedsSupport(t *testing.T) {
	_, err := New[user](Options[user]{
		Namespace: "user",
		Provider:  &plainProvider{inner: newMemProvider()},
		Codec:     c.JSON[user]{},
		BulkSeed:  BulkSeedIfMissing,
	})
	if !errors.Is(err, ErrBulkSeedNeedsAdder) {
		t.Fatalf("New error mismatch: %v", err)
	}
}

func TestBulkSeedAllUsesBatchGen(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingGenStore{inner: genstore.NewLocalGenStore(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = gs
		o.BulkSeed = BulkSeedAll
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b", "c"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
		"c": {ID: "c", Name: "C"},
	}
	snap := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetBulk expected all hit, missing=%v got=%v", missing, got)
	}
	if gs.snapshotCalls != 0 {
		t.Fatalf("bulk-hit warming should not do per-key Snapshot calls, got %d", gs.snapshotCalls)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf("bulk-hit validation should use one SnapshotMany call, got %d", gs.snapshotManyCalls)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); !ok {
			t.Fatalf("checked bulk-hit warming should seed single %q", k)
		}
	}
}

func TestSetBulkWithGensSeedsSinglesWhenBulkSeedOff(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkSeed = BulkSeedOff
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	keys := []string{"a", "b"}
	observed := cc.SnapshotGens(ctx, keys)

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}
	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); !ok {
			t.Fatalf("bulk write should seed single %q even when BulkSeedOff", k)
		}
	}
}

func TestBulkSeedIfMissing(t *testing.T) {
	ctx := context.Background()
	mp := &countingAdderProvider{memProvider: newMemProvider()}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkSeed = BulkSeedIfMissing
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	snap := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	for _, k := range keys {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}
	mp.setCalls = 0
	mp.addCalls = 0
	mp.addStored = 0

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetBulk expected all hit, missing=%v got=%v", missing, got)
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

func TestGetBulkReadGuardRejectsBulkAndMissesRejectedKeysWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"b": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 1 || missing[0] != "b" {
		t.Fatalf("GetBulk missing=%v want [b]", missing)
	}
	if len(got) != 1 || got["a"] != items["a"] {
		t.Fatalf("GetBulk got=%v want only a=%v", got, items["a"])
	}
	if _, ok := got["b"]; ok {
		t.Fatalf("GetBulk should not serve bulk-read-guard rejection from singles")
	}

	impl := mustImpl(t, cc)
	bk, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("bulk read-guard rejection should delete stored bulk, found=%v err=%v", found, err)
	}
}

func TestGetBulkReadGuardRejectWithoutReadGuardPreservesDuplicateMissing(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"b": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, []string{"a", "b"})
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if !reflect.DeepEqual(missing, []string{"b", "b"}) {
		t.Fatalf("GetBulk missing=%v want [b b]", missing)
	}
	if len(got) != 1 || got["a"] != items["a"] {
		t.Fatalf("GetBulk got=%v want only a=%v", got, items["a"])
	}
}

func TestGetBulkReadGuardRejectFallsBackToSinglesWithReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(_ context.Context, _ string, cached user) (bool, error) {
			return cached.Name != "stale", nil
		}
		o.BulkReadGuard = func(_ context.Context, cached map[string]user) (map[string]struct{}, error) {
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
	observed := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	freshB := user{ID: "b", Name: "fresh"}
	if err := cc.SetWithGen(ctx, "b", freshB, cc.SnapshotGen(ctx, "b"), 0); err != nil {
		t.Fatalf("SetWithGen fresh single: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetBulk missing=%v want none", missing)
	}
	want := map[string]user{
		"a": items["a"],
		"b": freshB,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetBulk got=%v want=%v", got, want)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("bulk read-guard rejection should delete stored bulk, found=%v err=%v", found, err)
	}
}

func TestGetBulkReadGuardErrorFailsClosedWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("bulk guard failed")
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return nil, sentinel
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetBulk got=%v want empty on bulk-read-guard error without single guard", got)
	}
	if !reflect.DeepEqual(missing, keys) {
		t.Fatalf("GetBulk missing=%v want=%v", missing, keys)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("bulk read-guard error should delete stored bulk, found=%v err=%v", found, err)
	}
}

func TestGetBulkReadGuardInvalidRejectedKeyFailsClosedWithoutReadGuard(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return map[string]struct{}{"ghost": {}}, nil
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetBulk got=%v want empty on invalid bulk-read-guard result", got)
	}
	if !reflect.DeepEqual(missing, keys) {
		t.Fatalf("GetBulk missing=%v want=%v", missing, keys)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("invalid bulk read-guard result should delete stored bulk, found=%v err=%v", found, err)
	}
}

func TestGetBulkReadGuardErrorFallsBackToSinglesWithReadGuard(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("bulk guard failed")
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.ReadGuard = func(context.Context, string, user) (bool, error) { return true, nil }
		o.BulkReadGuard = func(context.Context, map[string]user) (map[string]struct{}, error) {
			return nil, sentinel
		}
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetBulk missing=%v want none", missing)
	}
	if !reflect.DeepEqual(got, items) {
		t.Fatalf("GetBulk got=%v want=%v", got, items)
	}

	impl := mustImpl(t, cc)
	bk, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, found, err := mp.Get(ctx, bk.String()); err != nil || found {
		t.Fatalf("bulk read-guard error should delete stored bulk, found=%v err=%v", found, err)
	}
}

func TestGetBulkPropagatesSingleErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("get failed")
	mp := &getErrProvider{memProvider: newMemProvider(), err: sentinel}

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DisableBulk = true
	})
	defer closeTest(t, ctx, cc)

	got, missing, err := cc.GetBulk(ctx, []string{"a", "b"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetBulk error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("GetBulk error should be *OpError, got %T", err)
	}
	if oe.Op != OpGet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpGet)
	}
	if oe.Key != "a" && oe.Key != "b" {
		t.Fatalf("OpError.Key = %q, want %q or %q", oe.Key, "a", "b")
	}
	if len(got) != 0 {
		t.Fatalf("GetBulk should not return values on provider error, got %v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("GetBulk missing mismatch: %v", missing)
	}
}

func TestGetBulkBulkReadErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("bulk get failed")
	mp := &bulkGetErrProvider{memProvider: newMemProvider(), err: sentinel}
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, []string{"a", "b"})
	if err := cc.SetBulkWithGens(ctx, items, observed, time.Minute); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	got, missing, err := cc.GetBulk(ctx, []string{"a", "b"})
	if err == nil {
		t.Fatalf("GetBulk should return an error on bulk provider read failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetBulk error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("GetBulk error should be *OpError, got %T", err)
	}
	if oe.Op != OpGetBulk {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpGetBulk)
	}
	if oe.Key != "" {
		t.Fatalf("OpError.Key = %q, want empty", oe.Key)
	}
	if len(got) != 0 {
		t.Fatalf("GetBulk should not return values when bulk read fails, got %v", got)
	}
	if len(missing) != 0 {
		t.Fatalf("GetBulk missing should stay empty on bulk read failure, got %v", missing)
	}
	if mp.singleGetCalls != 0 {
		t.Fatalf("GetBulk should not fall back to singles on bulk read failure, got %d single reads", mp.singleGetCalls)
	}
}

func TestBulkValueDecodeFallsBackToSinglesAndDeletesBulk(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
	}
	snap := cc.SnapshotGens(ctx, []string{"a"})
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	bulkKey, err := impl.bulkKeySorted(uniqSorted([]string{"a"}))
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	entry, ok := mp.m[bulkKey.String()]
	if !ok {
		t.Fatalf("expected bulk entry %q", bulkKey)
	}
	corrupt := append([]byte(nil), entry.v...)
	corrupt[len(corrupt)-1] = 0xFF
	entry.v = corrupt
	mp.m[bulkKey.String()] = entry

	got, missing, err := cc.GetBulk(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("GetBulk should fall back to singles, missing=%v", missing)
	}
	if got["a"] != items["a"] {
		t.Fatalf("GetBulk fallback mismatch: got=%v want=%v", got["a"], items["a"])
	}
	if _, ok, _ := mp.Get(ctx, bulkKey.String()); ok {
		t.Fatalf("undecodable bulk should be deleted")
	}
}

func TestBulkFallbackPropagatesSingleErrors(t *testing.T) {
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
	observed := cc.SnapshotGens(ctx, []string{"a", "b"})
	if err := cc.SetBulkWithGens(ctx, items, observed, time.Minute); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	bulkKey, err := impl.bulkKeySorted(uniqSorted([]string{"a", "b"}))
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	entry, ok := mp.m[bulkKey.String()]
	if !ok {
		t.Fatalf("expected bulk entry %q", bulkKey)
	}
	corrupt := append([]byte(nil), entry.v...)
	corrupt[len(corrupt)-1] = 0xFF
	entry.v = corrupt
	mp.m[bulkKey.String()] = entry

	got, missing, err := cc.GetBulk(ctx, []string{"a", "b"})
	if err == nil {
		t.Fatalf("GetBulk should return an error when fallback singles fail")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetBulk error mismatch: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("GetBulk should not return values on fallback single error, got %v", got)
	}
	if len(missing) != 2 || missing[0] != "a" || missing[1] != "b" {
		t.Fatalf("GetBulk missing mismatch: %v", missing)
	}
	if _, ok, _ := mp.Get(ctx, bulkKey.String()); ok {
		t.Fatalf("undecodable bulk should be deleted")
	}
	if mp.singleGetCalls != 2 {
		t.Fatalf("expected one fallback single read per unique key, got %d", mp.singleGetCalls)
	}
}

func TestBulkSnapshotManyFallbackPreservesBulkPath(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = &failingGenStore{snapshotManyErr: errors.New("snapshot many failed")}
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, []string{"a", "b"})
	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	bulkKey, err := impl.bulkKeySorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if _, ok, _ := mp.Get(ctx, bulkKey.String()); !ok {
		t.Fatalf("expected bulk entry %q", bulkKey)
	}

	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
	}

	got, missing, err := cc.GetBulk(ctx, []string{"b", "a"})
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if got["a"] != items["a"] || got["b"] != items["b"] {
		t.Fatalf("GetBulk mismatch: got=%v want=%v", got, items)
	}
	if _, ok, _ := mp.Get(ctx, bulkKey.String()); !ok {
		t.Fatalf("expected bulk entry to remain after fallback validation")
	}
}

func TestBulkIgnoresUndecodableExtras(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	payload, err := c.JSON[user]{}.Encode(user{ID: "a", Name: "A"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	wireEntry, err := wire.EncodeBulk([]wire.BulkItem{
		{Key: "a", Gen: 0, Payload: payload},
		{Key: "z", Gen: 0, Payload: []byte{0xFF}},
	})
	if err != nil {
		t.Fatalf("EncodeBulk: %v", err)
	}

	bulkKey, err := impl.bulkKeySorted([]string{"a"})
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	if ok, err := impl.provider.Set(ctx, bulkKey.String(), wireEntry, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject bulk: ok=%v err=%v", ok, err)
	}

	got, missing, err := cc.GetBulk(ctx, []string{"a"})
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("expected no missing, got %v", missing)
	}
	if got["a"] != (user{ID: "a", Name: "A"}) {
		t.Fatalf("GetBulk mismatch: got=%v", got["a"])
	}
	if _, ok, _ := mp.Get(ctx, bulkKey.String()); !ok {
		t.Fatalf("expected bulk entry to remain when only extras are undecodable")
	}
}

func TestSetBulkWithGensMissingObservedGensReturnsError(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := map[string]uint64{
		"a": 0,
	}

	err := cc.SetBulkWithGens(ctx, items, observed, time.Minute)
	if !errors.Is(err, ErrMissingObservedGens) {
		t.Fatalf("SetBulkWithGens error mismatch: %v", err)
	}

	var moe *MissingObservedGensError
	if !errors.As(err, &moe) {
		t.Fatalf("expected MissingObservedGensError, got %T", err)
	}
	if len(moe.Missing) != 1 || moe.Missing[0] != "b" {
		t.Fatalf("unexpected missing keys: %v", moe.Missing)
	}
	if len(mp.m) != 0 {
		t.Fatalf("SetBulkWithGens should not write anything on caller error, provider=%v", mp.m)
	}
}

func TestSetBulkWithGensFallbackPropagatesSingleErrors(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("set failed")
	mp := &bulkRejectSingleErrProvider{memProvider: newMemProvider(), err: sentinel}

	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := map[string]uint64{
		"a": 0,
		"b": 0,
	}

	err := cc.SetBulkWithGens(ctx, items, observed, time.Minute)
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetBulkWithGens error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetBulkWithGens error should be *OpError, got %T", err)
	}
	if oe.Op != OpSet {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSet)
	}
}

func TestSetBulkWithGensBulkWriteErrorReturnsOpError(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("bulk set failed")
	cc := newTestCache(t, "user", &setErrProvider{memProvider: newMemProvider(), err: sentinel}, nil)
	defer closeTest(t, ctx, cc)

	err := cc.SetBulkWithGens(ctx, map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}, map[string]uint64{
		"a": 0,
		"b": 0,
	}, time.Minute)
	if err == nil {
		t.Fatalf("SetBulkWithGens should return an error")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("SetBulkWithGens error mismatch: %v", err)
	}
	var oe *OpError
	if !errors.As(err, &oe) {
		t.Fatalf("SetBulkWithGens error should be *OpError, got %T", err)
	}
	if oe.Op != OpSetBulk {
		t.Fatalf("OpError.Op = %q, want %q", oe.Op, OpSetBulk)
	}
	if oe.Key != "" {
		t.Fatalf("OpError.Key = %q, want empty", oe.Key)
	}
}

func TestSetBulkWithGensStrictUsesBatchGenAndPerKeyRechecks(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingGenStore{inner: genstore.NewLocalGenStore(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = gs
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}
	if gs.snapshotCalls != len(keys) {
		t.Fatalf("strict bulk-write seeding should recheck each key once, got %d", gs.snapshotCalls)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf("validated bulk write should use one SnapshotMany call, got %d", gs.snapshotManyCalls)
	}
}

func TestSetBulkWithGensFastUsesBatchGenOnly(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &countingGenStore{inner: genstore.NewLocalGenStore(time.Hour, time.Hour)}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = gs
		o.BulkWriteSeed = BulkWriteSeedFast
	})
	defer closeTest(t, ctx, cc)

	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	gs.snapshotCalls = 0
	gs.snapshotManyCalls = 0

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}
	if gs.snapshotCalls != 0 {
		t.Fatalf("fast bulk-write seeding should not do per-key Snapshot calls, got %d", gs.snapshotCalls)
	}
	if gs.snapshotManyCalls != 1 {
		t.Fatalf("validated bulk write should use one SnapshotMany call, got %d", gs.snapshotManyCalls)
	}
}

func TestSetBulkWithGensTTLOverrideAppliesToSeededSingles(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DefaultTTL = time.Hour
		o.BulkTTL = 2 * time.Hour
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	override := 50 * time.Millisecond
	before := time.Now()

	if err := cc.SetBulkWithGens(ctx, items, observed, override); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	bulkKey, err := impl.bulkKeySorted(keys)
	if err != nil {
		t.Fatalf("bulkKeySorted: %v", err)
	}
	bulkEntry, ok := mp.m[bulkKey.String()]
	if !ok {
		t.Fatalf("expected bulk entry to be written")
	}
	assertExpiryBefore(t, bulkEntry.exp, before, 5*time.Second, "bulk entry")

	for _, k := range keys {
		entry, ok := mp.m[impl.singleKeys(k).Value.String()]
		if !ok {
			t.Fatalf("expected single entry for %q", k)
		}
		assertExpiryBefore(t, entry.exp, before, 5*time.Second, fmt.Sprintf("single %q", k))
	}
}

func TestSetBulkWithGensTTLOverrideAppliesToFallbackSingles(t *testing.T) {
	ctx := context.Background()
	mp := &bulkRejectProvider{memProvider: newMemProvider()}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.DefaultTTL = time.Hour
		o.BulkTTL = 2 * time.Hour
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)
	override := 50 * time.Millisecond
	before := time.Now()

	if err := cc.SetBulkWithGens(ctx, items, observed, override); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	for _, k := range keys {
		entry, ok := mp.m[impl.singleKeys(k).Value.String()]
		if !ok {
			t.Fatalf("expected fallback single entry for %q", k)
		}
		assertExpiryBefore(t, entry.exp, before, 5*time.Second, fmt.Sprintf("fallback single %q", k))
	}
}

func TestSetBulkWithGensOffSkipsSinglesAfterSuccessfulBulkWrite(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.BulkWriteSeed = BulkWriteSeedOff
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	keys := []string{"a", "b"}
	items := map[string]user{
		"a": {ID: "a", Name: "A"},
		"b": {ID: "b", Name: "B"},
	}
	observed := cc.SnapshotGens(ctx, keys)

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	for _, k := range keys {
		if _, ok, _ := mp.Get(ctx, impl.singleKeys(k).Value.String()); ok {
			t.Fatalf("BulkWriteSeedOff should not materialize single %q on successful bulk write", k)
		}
	}

	got, missing, err := cc.GetBulk(ctx, keys)
	if err != nil {
		t.Fatalf("GetBulk: %v", err)
	}
	if len(missing) != 0 || len(got) != len(items) {
		t.Fatalf("GetBulk expected all hit, missing=%v got=%v", missing, got)
	}
}

func TestSetBulkWithGensStrictSkipsSingleAfterBatchValidationRace(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &bumpAfterSnapshotManyGenStore{
		inner:      genstore.NewLocalGenStore(time.Hour, time.Hour),
		bumpOnCall: 2,
	}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = gs
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	key := "a"
	gs.bumpKey = toGenStoreKey(impl.singleKeys(key).Cache)

	items := map[string]user{
		key: {ID: key, Name: "A"},
	}
	observed := cc.SnapshotGens(ctx, []string{key})

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	if _, ok, _ := mp.Get(ctx, impl.singleKeys(key).Value.String()); ok {
		t.Fatalf("strict post-bulk seeding should skip stale singles after the race")
	}
	if _, ok, err := cc.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get should miss when no checked single landed, ok=%v err=%v", ok, err)
	}
}

func TestSetBulkWithGensFastStaleSingleSelfHealsAfterBatchValidationRace(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	gs := &bumpAfterSnapshotManyGenStore{
		inner:      genstore.NewLocalGenStore(time.Hour, time.Hour),
		bumpOnCall: 2,
	}
	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = gs
		o.BulkWriteSeed = BulkWriteSeedFast
	})
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	key := "a"
	gs.bumpKey = toGenStoreKey(impl.singleKeys(key).Cache)

	items := map[string]user{
		key: {ID: key, Name: "A"},
	}
	observed := cc.SnapshotGens(ctx, []string{key})

	if err := cc.SetBulkWithGens(ctx, items, observed, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	raw, ok, err := mp.Get(ctx, impl.singleKeys(key).Value.String())
	if err != nil || !ok {
		t.Fatalf("expected stale single to land, ok=%v err=%v", ok, err)
	}
	gotGen, _, err := wire.DecodeSingle(raw)
	if err != nil {
		t.Fatalf("DecodeSingle: %v", err)
	}
	if gotGen != observed[key] {
		t.Fatalf("stale single gen=%d want observed=%d", gotGen, observed[key])
	}

	if _, ok, err := cc.Get(ctx, key); err != nil || ok {
		t.Fatalf("Get should reject stale single, ok=%v err=%v", ok, err)
	}
	if _, ok, _ := mp.Get(ctx, impl.singleKeys(key).Value.String()); ok {
		t.Fatalf("stale single should be deleted by self-heal")
	}
}

// TestBulkOrderInsensitiveHit: Same set, different order → same bulk key, bulk hit.
func TestBulkOrderInsensitiveHit(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()
	cc := newTestCache(t, "user", mp, nil)
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	// Write a bulk for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := cc.SnapshotGens(ctx, []string{"u1", "u3", "u4"})
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	// Remove singles so GetBulk must rely on the bulk entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
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
		if strings.HasPrefix(k, bulkValuePrefix("user")) {
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
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	// Write a bulk for {u1,u3,u4}
	items := map[string]user{
		"u1": {ID: "u1", Name: "A"},
		"u3": {ID: "u3", Name: "B"},
		"u4": {ID: "u4", Name: "C"},
	}
	snap := cc.SnapshotGens(ctx, []string{"u1", "u3", "u4"})
	if err := cc.SetBulkWithGens(ctx, items, snap, 0); err != nil {
		t.Fatalf("SetBulkWithGens: %v", err)
	}

	// Remove singles so GetBulk must rely on the bulk entry
	for k := range items {
		_ = impl.provider.Del(ctx, impl.singleKeys(k).Value.String())
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
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)

	k1, err := impl.bulkKeySorted(uniqSorted([]string{"u3", "u1", "u4"}))
	if err != nil {
		t.Fatalf("bulkKeySorted k1: %v", err)
	}
	k2, err := impl.bulkKeySorted(uniqSorted([]string{"u1", "u3", "u3", "u4"}))
	if err != nil {
		t.Fatalf("bulkKeySorted k2: %v", err)
	}
	if k1 != k2 {
		t.Fatalf("bulk keys differ for equivalent sets: %q vs %q", k1, k2)
	}
}

// ==============================
// Wire format tests
// ==============================

// DecodeSingle must reject trailing bytes (strict framing).
func TestWireDecodeSingleRejectsTrailing(t *testing.T) {
	b, err := wire.EncodeSingle(7, []byte("x"))
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
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
	defer closeTest(t, ctx, cc)

	impl := mustImpl(t, cc)
	k := "gen-mismatch"
	storageKey := impl.singleKeys(k).Value

	// GenStore has never been bumped for this key -> snapshot is 0.
	val := user{ID: "u1", Name: "Mismatch"}
	payload, err := c.JSON[user]{}.Encode(val)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Write a valid frame with gen=1 (mismatches snapshot=0).
	b, err := wire.EncodeSingle(1, payload)
	if err != nil {
		t.Fatalf("EncodeSingle: %v", err)
	}
	if ok, err := impl.provider.Set(ctx, storageKey.String(), b, 1, time.Minute); err != nil || !ok {
		t.Fatalf("inject single: ok=%v err=%v", ok, err)
	}

	// Get should detect gen mismatch, delete, and miss.
	if _, ok, err := cc.Get(ctx, k); err != nil || ok {
		t.Fatalf("expected miss on gen mismatch, ok=%v err=%v", ok, err)
	}

	// Ensure self-heal actually deleted the bad entry.
	if _, ok, _ := mp.Get(ctx, storageKey.String()); ok {
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
		sk := impl.singleKeys(ukey).Cache
		for range n {
			_, _ = impl.bumpGen(ctx, toGenStoreKey(sk))
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
		got := cc.SnapshotGens(ctx, nil)
		if len(got) != 0 {
			t.Fatalf("empty: expected empty map, got %v", got)
		}
	})

	t.Run("duplicates_and_zero_missing", func(t *testing.T) {
		// No bumps yet → everything is 0
		keys := []string{"dupa", "dupa", "other"}
		got := cc.SnapshotGens(ctx, keys)
		want := map[string]uint64{"dupa": 0, "other": 0}
		if !equalU64(got, want) {
			t.Fatalf("dups/zeros: got %v want %v", got, want)
		}
	})

	t.Run("mixed", func(t *testing.T) {
		// m1 -> 1, m3 -> 3, m2 -> 0
		_, _ = impl.bumpGen(ctx, toGenStoreKey(impl.singleKeys("m1").Cache))
		for range 3 {
			_, _ = impl.bumpGen(ctx, toGenStoreKey(impl.singleKeys("m3").Cache))
		}
		keys := []string{"m1", "m2", "m3", "m1"} // include duplicate
		got := cc.SnapshotGens(ctx, keys)
		want := map[string]uint64{"m1": 1, "m2": 0, "m3": 3}
		if !equalU64(got, want) {
			t.Fatalf("mixed: got %v want %v", got, want)
		}
	})
}

// ==============================
// Invalidate edge-case behavior (cluster down etc.)
// ==============================

type failingGenStore struct {
	snapshotErr     error
	snapshotManyErr error
	bumpErr         error
}

func (s *failingGenStore) Snapshot(context.Context, genstore.CacheKey) (uint64, error) {
	if s.snapshotErr != nil {
		return 0, s.snapshotErr
	}
	return 0, nil
}

func (s *failingGenStore) SnapshotMany(context.Context, []genstore.CacheKey) (map[genstore.CacheKey]uint64, error) {
	if s.snapshotManyErr != nil {
		return nil, s.snapshotManyErr
	}
	return map[genstore.CacheKey]uint64{}, nil
}

func (s *failingGenStore) Bump(context.Context, genstore.CacheKey) (uint64, error) {
	return 0, s.bumpErr
}
func (s *failingGenStore) Cleanup(time.Duration)       {}
func (s *failingGenStore) Close(context.Context) error { return nil }

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
	defer closeTest(t, ctx, cc)

	err := cc.Invalidate(ctx, "k1")
	if err == nil {
		t.Fatalf("expected error when both bump and delete fail")
	}
	var ie *InvalidateError
	if !errors.As(err, &ie) {
		t.Fatalf("expected InvalidateError, got %T: %v", err, err)
	}
	var bumpOpErr *OpError
	if !errors.As(ie.BumpErr, &bumpOpErr) {
		t.Fatalf("expected bump error to be *OpError, got %T", ie.BumpErr)
	}
	if bumpOpErr.Op != OpInvalidate || bumpOpErr.Key != "k1" {
		t.Fatalf("unexpected bump OpError: %+v", bumpOpErr)
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
	if !errors.Is(err, bumpFail) {
		t.Fatalf("expected errors.Is(err, bumpErr) to be true")
	}
}

func TestInvalidateBumpFailDeleteOKNoError(t *testing.T) {
	ctx := context.Background()
	mp := newMemProvider()

	cc := newTestCache(t, "user", mp, func(o *Options[user]) {
		o.GenStore = &failingGenStore{bumpErr: errors.New("bump failed")}
	})
	defer closeTest(t, ctx, cc)

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
	defer closeTest(t, ctx, cc)

	// Warm a gen so bump definitely succeeds.
	impl := mustImpl(t, cc)
	_, _ = impl.bumpGen(ctx, toGenStoreKey(impl.singleKeys("k3").Cache))

	if err := cc.Invalidate(ctx, "k3"); err != nil {
		t.Fatalf("expected no error when delete fails but bump succeeds; got %v", err)
	}
}
