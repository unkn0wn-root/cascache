package redis

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v3/codec"
	"github.com/unkn0wn-root/cascache/v3/version"
)

type fakeClient struct {
	goredis.UniversalClient
}

type closableClient struct {
	goredis.UniversalClient
	closeCalls int
}

func (c *closableClient) Close() error {
	c.closeCalls++
	return nil
}

type fakeUniversalClient struct {
	goredis.UniversalClient
	closeCalls atomic.Int32
}

func (c *fakeUniversalClient) Close() error {
	c.closeCalls.Add(1)
	return nil
}

type snapshotCmdClient struct {
	goredis.UniversalClient
	getFn  func(context.Context, string) *goredis.StringCmd
	mgetFn func(context.Context, ...string) *goredis.SliceCmd
}

func (c *snapshotCmdClient) Get(ctx context.Context, key string) *goredis.StringCmd {
	if c.getFn != nil {
		return c.getFn(ctx, key)
	}
	return goredis.NewStringResult("", nil)
}

func (c *snapshotCmdClient) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
	if c.mgetFn != nil {
		return c.mgetFn(ctx, keys...)
	}
	return goredis.NewSliceResult(nil, nil)
}

type scriptCmdClient struct {
	goredis.UniversalClient
	evalShaCalls int
	evalCalls    int
	script       string
	keys         []string
	args         []any
	result       int64
	resultSet    bool
}

type fakeRedisError string

func (e fakeRedisError) Error() string { return string(e) }
func (fakeRedisError) RedisError()     {}

func (c *scriptCmdClient) EvalSha(
	context.Context,
	string,
	[]string,
	...any,
) *goredis.Cmd {
	c.evalShaCalls++
	return goredis.NewCmdResult(nil, fakeRedisError("NOSCRIPT no matching script"))
}

func (c *scriptCmdClient) Eval(
	_ context.Context,
	script string,
	keys []string,
	args ...any,
) *goredis.Cmd {
	c.evalCalls++
	c.script = script
	c.keys = append([]string(nil), keys...)
	c.args = append([]any(nil), args...)
	result := int64(1)
	if c.resultSet {
		result = c.result
	}
	return goredis.NewCmdResult(result, nil)
}

func TestNewKeyMutatorNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewKeyMutator(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewKeyMutator error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewKeyMutatorWithOptionsStoresVersionTTL(t *testing.T) {
	t.Parallel()

	mutator, err := NewKeyMutatorWithOptions(KeyMutatorOptions{
		Client:     &fakeClient{},
		VersionTTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewKeyMutatorWithOptions: %v", err)
	}
	if mutator.versionTTL != 2*time.Minute {
		t.Fatalf("versionTTL = %v, want %v", mutator.versionTTL, 2*time.Minute)
	}
}

func TestNewProviderNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewProvider(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewProvider error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewVersionStoreNilClient(t *testing.T) {
	t.Parallel()

	if _, err := NewVersionStore(nil); !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewVersionStore error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewVersionStoreWithOptionsStoresVersionTTL(t *testing.T) {
	t.Parallel()

	store, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:     &fakeClient{},
		VersionTTL: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}
	if store.versionTTL != 90*time.Second {
		t.Fatalf("versionTTL = %v, want %v", store.versionTTL, 90*time.Second)
	}
}

func TestVersionStoreRefreshPExpire(t *testing.T) {
	t.Parallel()

	client := &scriptCmdClient{}
	store, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:     client,
		VersionTTL: 90 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}

	key := version.NewCacheKey("k")
	refreshed, err := store.Refresh(context.Background(), key)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !refreshed {
		t.Fatalf("Refresh refreshed=false, want true")
	}
	if len(client.keys) != 1 || client.keys[0] != versionStorageKey(key) {
		t.Fatalf("Refresh keys=%v, want [%q]", client.keys, versionStorageKey(key))
	}
	if len(client.args) != 1 || client.args[0] != int64((90*time.Second).Milliseconds()) {
		t.Fatalf("Refresh args=%v, want ttl ms", client.args)
	}
	if !strings.Contains(client.script, "GET") ||
		!strings.Contains(client.script, "PEXPIRE") ||
		!strings.Contains(client.script, "PERSIST") {
		t.Fatalf("refresh script should check existence and refresh/persist:\n%s", client.script)
	}
}

func TestVersionStoreRefreshPersist(t *testing.T) {
	t.Parallel()

	client := &scriptCmdClient{}
	store, err := NewVersionStoreWithOptions(VersionStoreOptions{Client: client})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}

	key := version.NewCacheKey("k")
	refreshed, err := store.Refresh(context.Background(), key)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if !refreshed {
		t.Fatalf("Refresh refreshed=false, want true")
	}
	if len(client.args) != 1 || client.args[0] != int64(0) {
		t.Fatalf("Refresh args=%v, want zero ttl", client.args)
	}
}

func TestVersionStoreRefreshMissing(t *testing.T) {
	t.Parallel()

	client := &scriptCmdClient{resultSet: true}
	store, err := NewVersionStoreWithOptions(VersionStoreOptions{Client: client})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}

	refreshed, err := store.Refresh(context.Background(), version.NewCacheKey("k"))
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if refreshed {
		t.Fatalf("Refresh refreshed=true, want false for missing version state")
	}
}

func TestKeyMutatorSetVersionTTL(t *testing.T) {
	t.Parallel()

	client := &scriptCmdClient{}
	mutator, err := NewKeyMutatorWithOptions(KeyMutatorOptions{
		Client:     client,
		VersionTTL: 1500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewKeyMutatorWithOptions: %v", err)
	}
	fence, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}

	stored, err := mutator.SetIfVersion(
		context.Background(),
		version.NewCacheKey("k"),
		"value-key",
		version.Snapshot{Fence: fence, Exists: true},
		[]byte("payload"),
		time.Minute,
	)
	if err != nil || !stored {
		t.Fatalf("SetIfVersion: stored=%v err=%v", stored, err)
	}
	if client.evalShaCalls != 1 || client.evalCalls != 1 {
		t.Fatalf("script calls evalsha=%d eval=%d, want 1/1", client.evalShaCalls, client.evalCalls)
	}
	if len(client.args) != 6 {
		t.Fatalf("script args len=%d, want 6", len(client.args))
	}
	if got, want := client.args[5], int64(1500); got != want {
		t.Fatalf("version ttl script arg=%v (%T), want %v", got, got, want)
	}
	if !strings.Contains(client.script, "PEXPIRE") || !strings.Contains(client.script, "PERSIST") {
		t.Fatalf("script should refresh or persist existing version state:\n%s", client.script)
	}
}

func TestKeyMutatorSetZeroVersionTTL(t *testing.T) {
	t.Parallel()

	client := &scriptCmdClient{}
	mutator, err := NewKeyMutatorWithOptions(KeyMutatorOptions{Client: client})
	if err != nil {
		t.Fatalf("NewKeyMutatorWithOptions: %v", err)
	}
	fence, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}

	stored, err := mutator.SetIfVersion(
		context.Background(),
		version.NewCacheKey("k"),
		"value-key",
		version.Snapshot{Fence: fence, Exists: true},
		[]byte("payload"),
		time.Minute,
	)
	if err != nil || !stored {
		t.Fatalf("SetIfVersion: stored=%v err=%v", stored, err)
	}
	if len(client.args) != 6 {
		t.Fatalf("script args len=%d, want 6", len(client.args))
	}
	if got, want := client.args[5], int64(0); got != want {
		t.Fatalf("version ttl script arg=%v (%T), want %v", got, got, want)
	}
}

func TestNewNilClient(t *testing.T) {
	t.Parallel()

	if _, err := New(Options[struct{}]{
		Namespace: "user",
		Codec:     codec.JSON[struct{}]{},
	}); !errors.Is(err, ErrNilClient) {
		t.Fatalf("New error = %v, want %v", err, ErrNilClient)
	}
}

func TestNewConstructsCache(t *testing.T) {
	t.Parallel()

	cache, err := New(Options[struct{}]{
		Namespace: "user",
		Client:    &fakeClient{},
		Codec:     codec.JSON[struct{}]{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if cache == nil {
		t.Fatal("New returned nil cache")
	}
}

func TestCloseClientOwnershipDisabledLeavesClientOpen(t *testing.T) {
	t.Parallel()

	client := &closableClient{}
	cache, err := New(Options[struct{}]{
		Namespace:   "user",
		Client:      client,
		Codec:       codec.JSON[struct{}]{},
		CloseClient: false,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.closeCalls != 0 {
		t.Fatalf("close calls = %d, want 0 when CloseClient is false", client.closeCalls)
	}
}

func TestCloseClientOwnershipEnabledClosesClientOnce(t *testing.T) {
	t.Parallel()

	client := &closableClient{}
	cache, err := New(Options[struct{}]{
		Namespace:   "user",
		Client:      client,
		Codec:       codec.JSON[struct{}]{},
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := cache.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if client.closeCalls != 1 {
		t.Fatalf("close calls = %d, want 1 when CloseClient is true", client.closeCalls)
	}
}

func TestVersionStoreCloseSharedClientNoop(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewVersionStore(client)
	if err != nil {
		t.Fatalf("NewVersionStore: %v", err)
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := client.closeCalls.Load(); got != 0 {
		t.Fatalf("shared client should not be closed, got %d close calls", got)
	}
}

func TestVersionStoreCloseOwnedClientOnce(t *testing.T) {
	t.Parallel()

	client := &fakeUniversalClient{}
	store, err := NewVersionStoreWithOptions(VersionStoreOptions{
		Client:      client,
		CloseClient: true,
	})
	if err != nil {
		t.Fatalf("NewVersionStoreWithOptions: %v", err)
	}

	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close first: %v", err)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close second: %v", err)
	}
	if got := client.closeCalls.Load(); got != 1 {
		t.Fatalf("owned client should be closed once, got %d close calls", got)
	}
}

func TestVersionStoreWithOptionsRejectsNilClient(t *testing.T) {
	t.Parallel()

	_, err := NewVersionStoreWithOptions(VersionStoreOptions{})
	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("NewVersionStoreWithOptions error mismatch: %v", err)
	}
}

func TestVersionStoreNilReceiverReturnsErrNilClient(t *testing.T) {
	t.Parallel()

	var store *VersionStore
	key := version.NewCacheKey("k")

	if _, err := store.Snapshot(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Snapshot error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.SnapshotMany(
		context.Background(),
		[]version.CacheKey{key},
	); !errors.Is(
		err,
		ErrNilClient,
	) {
		t.Fatalf("SnapshotMany error = %v, want %v", err, ErrNilClient)
	}
	if _, err := store.Advance(context.Background(), key); !errors.Is(err, ErrNilClient) {
		t.Fatalf("Advance error = %v, want %v", err, ErrNilClient)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close error = %v, want nil", err)
	}
}

func TestParseFenceRoundTrip(t *testing.T) {
	t.Parallel()

	want, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}

	got, err := version.ParseFence(want.String())
	if err != nil {
		t.Fatalf("ParseFence: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("fence = %s, want %s", got, want)
	}
}

func TestParseFenceRejectsLegacyCounter(t *testing.T) {
	t.Parallel()

	if _, err := version.ParseFence("7"); err == nil {
		t.Fatal("expected legacy counter format to be rejected")
	}
}

func TestVersionStoreSnapshotBadFence(t *testing.T) {
	t.Parallel()

	key := version.NewCacheKey("k")
	store, err := NewVersionStore(&snapshotCmdClient{
		getFn: func(context.Context, string) *goredis.StringCmd {
			return goredis.NewStringResult("7", nil)
		},
	})
	if err != nil {
		t.Fatalf("NewVersionStore: %v", err)
	}

	_, err = store.Snapshot(context.Background(), key)
	if !errors.Is(err, ErrFenceParse) {
		t.Fatalf("Snapshot error = %v, want errors.Is(_, %v)", err, ErrFenceParse)
	}
	if !strings.Contains(err.Error(), key.String()) {
		t.Fatalf("Snapshot error = %q, want key %q in message", err, key)
	}
}

func TestVersionStoreSnapshotManyBadFence(t *testing.T) {
	t.Parallel()

	key := version.NewCacheKey("k")
	store, err := NewVersionStore(&snapshotCmdClient{
		mgetFn: func(context.Context, ...string) *goredis.SliceCmd {
			return goredis.NewSliceResult([]any{"7"}, nil)
		},
	})
	if err != nil {
		t.Fatalf("NewVersionStore: %v", err)
	}

	_, err = store.SnapshotMany(context.Background(), []version.CacheKey{key})
	if !errors.Is(err, ErrFenceParse) {
		t.Fatalf("SnapshotMany error = %v, want errors.Is(_, %v)", err, ErrFenceParse)
	}
	if !strings.Contains(err.Error(), key.String()) {
		t.Fatalf("SnapshotMany error = %q, want key %q in message", err, key)
	}
}

func TestKeyMutatorReadKeyHit(t *testing.T) {
	t.Parallel()

	fence, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}
	raw := []byte("wire")
	key := version.NewCacheKey("k")
	mutator, err := NewKeyMutator(&snapshotCmdClient{
		mgetFn: func(_ context.Context, keys ...string) *goredis.SliceCmd {
			if len(keys) != 2 {
				t.Fatalf("MGet keys len=%d, want 2", len(keys))
			}
			if keys[1] != versionStorageKey(key) {
				t.Fatalf("version key = %q, want %q", keys[1], versionStorageKey(key))
			}
			return goredis.NewSliceResult([]any{string(raw), fence.String()}, nil)
		},
	})
	if err != nil {
		t.Fatalf("NewKeyMutator: %v", err)
	}

	got, err := mutator.ReadKey(context.Background(), key, "value-key")
	if err != nil {
		t.Fatalf("ReadKey: %v", err)
	}
	if !got.Found || !bytes.Equal(got.Raw, raw) {
		t.Fatalf("ReadKey value = found %v raw %q, want found raw %q", got.Found, got.Raw, raw)
	}
	if got.SnapshotErr != nil {
		t.Fatalf("SnapshotErr = %v", got.SnapshotErr)
	}
	if !got.Snapshot.Exists || !got.Snapshot.Fence.Equal(fence) {
		t.Fatalf("Snapshot = %+v, want fence %s", got.Snapshot, fence)
	}
}

func TestKeyMutatorReadKeyBadFence(t *testing.T) {
	t.Parallel()

	key := version.NewCacheKey("k")
	mutator, err := NewKeyMutator(&snapshotCmdClient{
		mgetFn: func(context.Context, ...string) *goredis.SliceCmd {
			return goredis.NewSliceResult([]any{nil, "bad-fence"}, nil)
		},
	})
	if err != nil {
		t.Fatalf("NewKeyMutator: %v", err)
	}

	got, err := mutator.ReadKey(context.Background(), key, "value-key")
	if err != nil {
		t.Fatalf("ReadKey: %v", err)
	}
	if got.Found {
		t.Fatalf("Found = true, want false")
	}
	if !errors.Is(got.SnapshotErr, ErrFenceParse) {
		t.Fatalf("SnapshotErr = %v, want ErrFenceParse", got.SnapshotErr)
	}
}

func TestSnapshotsFromStringCmds(t *testing.T) {
	t.Parallel()

	fence, err := version.NewFence()
	if err != nil {
		t.Fatalf("NewFence: %v", err)
	}
	keys := []version.CacheKey{version.NewCacheKey("a"), version.NewCacheKey("b")}
	cmds := []*goredis.StringCmd{
		goredis.NewStringResult(fence.String(), nil),
		goredis.NewStringResult("", goredis.Nil),
	}

	got, err := snapshotsFromStringCmds(keys, cmds, goredis.Nil)
	if err != nil {
		t.Fatalf("snapshotsFromStringCmds: %v", err)
	}
	if !got[keys[0]].Exists || !got[keys[0]].Fence.Equal(fence) {
		t.Fatalf("first snapshot = %+v, want %s", got[keys[0]], fence)
	}
	if got[keys[1]].Exists {
		t.Fatalf("second snapshot exists, want missing")
	}

	_, err = snapshotsFromStringCmds(
		[]version.CacheKey{version.NewCacheKey("bad")},
		[]*goredis.StringCmd{goredis.NewStringResult("bad-fence", nil)},
		nil,
	)
	if !errors.Is(err, ErrFenceParse) {
		t.Fatalf("bad fence error = %v, want ErrFenceParse", err)
	}
}

func TestUsesSlotRouting(t *testing.T) {
	t.Parallel()

	if !usesSlotRouting(&goredis.ClusterClient{}) {
		t.Fatal("ClusterClient should use slot-safe SnapshotMany path")
	}
	if !usesSlotRouting(&goredis.Ring{}) {
		t.Fatal("Ring should use slot-safe SnapshotMany path")
	}
	if usesSlotRouting(&goredis.Client{}) {
		t.Fatal("single Client should keep MGET SnapshotMany path")
	}
}
