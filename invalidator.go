package cascache

import (
	"context"

	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/internal/keyspace"
)

// InvalidatorOptions configure an [Invalidator]. Namespace and Backend are
// required, and the namespace must match the cache being invalidated.
type InvalidatorOptions struct {
	Namespace string
	Backend   backend.Backend

	// Disabled makes every call a no-op.
	Disabled bool
	Observer Observer
}

// Validate checks the required fields.
func (o InvalidatorOptions) Validate() error {
	switch {
	case o.Namespace == "":
		return ErrNoNamespace
	case isNil(o.Backend):
		return ErrNoBackend
	}
	return nil
}

// Invalidator retires cached entries without knowing their value type. Get one
// from [Cache.Invalidator] or create a standalone one with [NewInvalidator].
type Invalidator struct {
	space    keyspace.Space
	backend  backend.Backend
	disabled bool
	observer Observer
}

// NewInvalidator creates a standalone invalidator.
func NewInvalidator(opts InvalidatorOptions) (*Invalidator, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &Invalidator{
		space:    keyspace.New(opts.Namespace),
		backend:  opts.Backend,
		disabled: opts.Disabled,
		observer: nilIfNil(opts.Observer),
	}, nil
}

// Enabled reports whether the invalidator does anything.
func (i *Invalidator) Enabled() bool { return i != nil && !i.disabled }

// Invalidate makes the cached value for key unusable.
// It replaces the key's invalidation state before removing the value, so a load
// already in flight cannot write its result back and have it served.
// On error, callers should retry because the invalidation may not have taken
// effect. Delete failures are reported as [EventCleanupFailed]; the old value is
// already unreadable.
func (i *Invalidator) Invalidate(ctx context.Context, key string) error {
	if i.disabled {
		return nil
	}

	res, err := i.backend.Invalidate(ctx, i.space.Key(key), backend.NewFence())
	if err != nil {
		return i.opErr(OpInvalidate, key, err)
	}
	if res.CleanupErr != nil {
		i.observe(Event{
			Type: EventCleanupFailed,
			Op:   OpInvalidate,
			Key:  key,
			Err:  res.CleanupErr,
		})
	}
	return nil
}

func (i *Invalidator) observe(e Event) {
	if i.observer != nil {
		i.observer.Observe(e)
	}
}

func (i *Invalidator) opErr(op Op, key string, err error) error {
	wrapped := &OpError{Op: op, Key: key, Err: err}
	i.observe(Event{Type: EventOperationFailed, Op: op, Key: key, Err: wrapped})
	return wrapped
}
