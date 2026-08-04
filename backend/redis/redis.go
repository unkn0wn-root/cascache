package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/internal/typednil"
	"github.com/unkn0wn-root/cascache/v4/provider"
)

// DefaultInvalidationTTL is how long invalidation state lives by default.
const DefaultInvalidationTTL = 24 * time.Hour

var (
	// ErrNilClient reports a missing or typed-nil client.
	ErrNilClient = errors.New("cascache/redis: nil client")

	// ErrReplicaReads reports a client configured to read from replicas.
	ErrReplicaReads = errors.New("cascache/redis: replica reads cannot observe an invalidation in time")

	// ErrFenceParse reports malformed fence data in Redis.
	ErrFenceParse = errors.New("cascache/redis: fence parse")
)

var (
	errReplyLength = errors.New("cascache/redis: unexpected reply length")
	errReplyType   = errors.New("cascache/redis: unexpected reply type")
	errStoreReply  = errors.New("cascache/redis: unexpected compare-and-store reply")
	errScriptReply = errors.New("cascache/redis: unexpected script reply")
)

// Options configures Redis backends. The client stays caller-owned.
type Options struct {
	// InvalidationTTL is how long a key's invalidation state lives after a
	// write to it. Zero uses [DefaultInvalidationTTL]; [backend.NoExpiration]
	// keeps it forever.
	// Value TTLs are limited to this duration.
	InvalidationTTL time.Duration

	// AllowReplicaReads permits reads that may be served by a replica. Enabling
	// it gives up the guarantee that a completed invalidation is immediately
	// visible to later reads. It is not recommended for freshness-critical
	// caches.
	AllowReplicaReads bool
}

func (o Options) resolve() (time.Duration, error) {
	switch {
	case o.InvalidationTTL == backend.NoExpiration:
		return 0, nil
	case o.InvalidationTTL < 0:
		return 0, fmt.Errorf("cascache/redis: invalid invalidation TTL %v", o.InvalidationTTL)
	case o.InvalidationTTL == 0:
		return DefaultInvalidationTTL, nil
	default:
		return o.InvalidationTTL, nil
	}
}

// Backend stores values and invalidation state atomically in Redis.
type Backend struct {
	client   goredis.UniversalClient
	fenceTTL time.Duration
}

var _ backend.Backend = (*Backend)(nil)

// New returns a Redis backend. It does not take ownership of the client.
func New(client goredis.UniversalClient, opts Options) (*Backend, error) {
	fenceTTL, err := check(client, opts)
	if err != nil {
		return nil, err
	}
	return &Backend{client: client, fenceTTL: fenceTTL}, nil
}

type fenceStore struct {
	client   goredis.UniversalClient
	fenceTTL time.Duration
}

var _ backend.FenceStore = (*fenceStore)(nil)

func newFenceStore(client goredis.UniversalClient, opts Options) (*fenceStore, error) {
	fenceTTL, err := check(client, opts)
	if err != nil {
		return nil, err
	}
	return &fenceStore{client: client, fenceTTL: fenceTTL}, nil
}

// Shared keeps values in a caller-owned store and invalidation state in Redis.
// Reads never leave the process, and an invalidation still reaches every
// replica.
type Shared struct {
	backend *backend.Composite
}

var _ backend.Backend = (*Shared)(nil)

// NewShared returns a backend whose invalidation state lives in Redis. It takes
// ownership of neither the store nor the client.
func NewShared(
	values provider.Store,
	client goredis.UniversalClient,
	opts Options,
) (*Shared, error) {
	fences, err := newFenceStore(client, opts)
	if err != nil {
		return nil, err
	}
	b, err := backend.NewComposite(values, fences)
	if err != nil {
		return nil, err
	}
	return &Shared{backend: b}, nil
}

func (b *Shared) Read(ctx context.Context, key backend.Key) (backend.ReadResult, error) {
	return b.backend.Read(ctx, key)
}

func (b *Shared) Ensure(
	ctx context.Context,
	key backend.Key,
	candidate backend.Fence,
) (backend.Fence, error) {
	return b.backend.Ensure(ctx, key, candidate)
}

func (b *Shared) CompareAndStore(
	ctx context.Context,
	req backend.StoreRequest,
) (backend.StoreResult, error) {
	return b.backend.CompareAndStore(ctx, req)
}

func (b *Shared) Invalidate(
	ctx context.Context,
	key backend.Key,
	next backend.Fence,
) (backend.InvalidateResult, error) {
	return b.backend.Invalidate(ctx, key, next)
}

func (b *Shared) Discard(ctx context.Context, key backend.Key, rejected []byte) (bool, error) {
	return b.backend.Discard(ctx, key, rejected)
}

func check(client goredis.UniversalClient, opts Options) (time.Duration, error) {
	if err := checkClient(client); err != nil {
		return 0, err
	}
	if !opts.AllowReplicaReads && readsFromReplicas(client) {
		return 0, ErrReplicaReads
	}
	return opts.resolve()
}

// Replica lag can return an old value with its matching old fence. Reject known
// replica-read configurations unless the caller allows them.
func readsFromReplicas(client goredis.UniversalClient) bool {
	c, ok := client.(*goredis.ClusterClient)
	return ok && c.Options().ReadOnly
}

func (f *fenceStore) Lifetime() time.Duration { return f.fenceTTL }

// Shared argument checks for the Redis backends.
func checkClient(client goredis.UniversalClient) error {
	if typednil.Is(client) {
		return ErrNilClient
	}
	return nil
}

func checkKey(client goredis.UniversalClient, key backend.Key) error {
	if err := checkClient(client); err != nil {
		return err
	}
	return backend.CheckKey(key)
}

func checkKeyFence(client goredis.UniversalClient, key backend.Key, fence backend.Fence) error {
	if err := checkClient(client); err != nil {
		return err
	}
	return backend.CheckKeyFence(key, fence)
}

// Lua treats zero as no expiry, so round positive TTLs up to one millisecond.
func ttlMillis(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	return max(ttl.Milliseconds(), 1)
}

func ensureFence(
	ctx context.Context,
	client goredis.UniversalClient,
	fenceTTL time.Duration,
	key backend.Key,
	candidate backend.Fence,
) (backend.Fence, error) {
	reply, err := ensureScript.Run(
		ctx,
		client,
		[]string{backend.FenceKey(key)},
		candidate.Bytes(),
		ttlMillis(fenceTTL),
	).Result()
	if err != nil {
		return backend.Fence{}, err
	}
	return parseFence(reply)
}
