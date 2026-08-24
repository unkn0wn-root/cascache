package backend

import (
	"context"

	"github.com/unkn0wn-root/cascache/v4/provider"
)

// Local combines a caller-owned value store with process-local invalidation
// state. It is safe for concurrent use within one process. Replicas that share
// values must use shared invalidation state instead, such as Redis.
type Local struct {
	backend *Composite
	fences  *memoryFenceStore
}

var _ Backend = (*Local)(nil)

// NewLocal returns a backend with process-local invalidation state. It does not
// take ownership of values. Call [Local.Close] when the backend is no longer
// needed.
func NewLocal(values provider.Store, opts LocalOptions) (*Local, error) {
	fences, err := newMemoryFenceStore(opts)
	if err != nil {
		return nil, err
	}
	b, err := NewComposite(values, fences)
	if err != nil {
		_ = fences.close()
		return nil, err
	}
	return &Local{backend: b, fences: fences}, nil
}

func (b *Local) Read(ctx context.Context, key Key) (ReadResult, error) {
	return b.backend.Read(ctx, key)
}

func (b *Local) Ensure(ctx context.Context, key Key, candidate Fence) (Fence, error) {
	return b.backend.Ensure(ctx, key, candidate)
}

func (b *Local) CompareAndStore(ctx context.Context, req StoreRequest) (StoreResult, error) {
	return b.backend.CompareAndStore(ctx, req)
}

func (b *Local) Invalidate(ctx context.Context, key Key, next Fence) (InvalidateResult, error) {
	return b.backend.Invalidate(ctx, key, next)
}

func (b *Local) Discard(ctx context.Context, key Key, rejected []byte) (bool, error) {
	return b.backend.Discard(ctx, key, rejected)
}

// Close stops background invalidation-state cleanup. It is safe to call more
// than once and does not close the caller-owned value store.
func (b *Local) Close() error {
	if b == nil || b.fences == nil {
		return nil
	}
	return b.fences.close()
}
