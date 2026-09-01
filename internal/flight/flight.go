package flight

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

var (
	ErrGoexit = errors.New("flight: call exited without returning")
	ErrPanic  = errors.New("flight: call panicked")
)

type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("%s: %v\n%s", ErrPanic.Error(), e.Value, e.Stack)
}

func (e *PanicError) Unwrap() error { return ErrPanic }

func (r Role) String() string {
	switch r {
	case Owned:
		return "owned"
	case Shared:
		return "shared"
	default:
		return "abandoned"
	}
}

// Role says how a caller left Do.
type Role uint8

const (
	// Abandoned means the caller stopped waiting before receiving a result.
	Abandoned Role = iota
	// Owned marks the one caller that accounts for a completed call. It may not
	// be the caller that started it.
	Owned
	// Shared means the caller took a result another caller's call produced.
	Shared
)

type Group[V any] struct {
	// Timeout bounds one call's context. Zero means no timeout. It does not stop
	// a call that ignores its context, but such a call no longer takes joiners.
	Timeout time.Duration

	OnJoin   func(key string)
	OnPanic  func(key string, value any, stack []byte)
	OnGoexit func(key string)

	mu    sync.Mutex
	calls map[string]*call[V]
}

type call[V any] struct {
	ctx    context.Context
	done   chan struct{}
	cancel context.CancelFunc
	// Guarded by Group.mu. The call is canceled when waiters reaches zero.
	waiters int
	// The first caller to collect the result accounts for the call.
	accounted bool
	val       V
	err       error
}

// Do runs fn once for concurrent calls with the same key. One caller may stop
// waiting without canceling fn for the others. Panics and Goexit become errors.
// Exactly one caller of a completed call receives [Owned].
// A finished call leaves the group before waiters wake. A caller that calls Do
// again therefore joins a newer call. A call whose context has ended is never
// joined, so a run that outlives its [Group.Timeout] does not hold its key.
func (g *Group[V]) Do(
	ctx context.Context,
	key string,
	fn func(context.Context) (V, error),
) (v V, role Role, err error) {
	if err := ctx.Err(); err != nil {
		return v, Abandoned, err
	}

	g.mu.Lock()
	if c, ok := g.calls[key]; ok && c.ctx.Err() == nil {
		c.waiters++
		g.mu.Unlock()

		if g.OnJoin != nil {
			g.OnJoin(key)
		}
		return g.wait(ctx, key, c)
	}

	callCtx, cancel := g.callContext(ctx)
	c := &call[V]{ctx: callCtx, done: make(chan struct{}), cancel: cancel, waiters: 1}
	if g.calls == nil {
		g.calls = make(map[string]*call[V])
	}
	g.calls[key] = c
	g.mu.Unlock()

	go g.run(callCtx, key, c, fn)

	return g.wait(ctx, key, c)
}

func (g *Group[V]) Len() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

func (g *Group[V]) callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if g.Timeout > 0 {
		return context.WithTimeout(detached, g.Timeout)
	}
	return context.WithCancel(detached)
}

func (g *Group[V]) wait(ctx context.Context, key string, c *call[V]) (V, Role, error) {
	select {
	case <-c.done:
		return c.val, g.settle(c), c.err
	case <-ctx.Done():
	}

	// Prefer a result that became ready with the cancellation.
	select {
	case <-c.done:
		return c.val, g.settle(c), c.err
	default:
	}

	g.abandon(key, c)
	var zero V
	return zero, Abandoned, ctx.Err()
}

func (g *Group[V]) settle(c *call[V]) Role {
	g.mu.Lock()
	defer g.mu.Unlock()

	if c.accounted {
		return Shared
	}
	c.accounted = true
	return Owned
}

func (g *Group[V]) abandon(key string, c *call[V]) {
	g.mu.Lock()
	c.waiters--
	last := c.waiters == 0
	if last && g.calls[key] == c {
		// Let new callers start before canceling the old call.
		delete(g.calls, key)
	}
	g.mu.Unlock()

	if last {
		c.cancel()
	}
}

func (g *Group[V]) run(
	ctx context.Context,
	key string,
	c *call[V],
	fn func(context.Context) (V, error),
) {
	returned := false
	defer func() {
		switch r := recover(); {
		case r != nil:
			stack := debug.Stack()
			c.err = &PanicError{Value: r, Stack: stack}
			if g.OnPanic != nil {
				g.OnPanic(key, r, stack)
			}
		case !returned:
			// Goexit has no panic value.
			c.err = ErrGoexit
			if g.OnGoexit != nil {
				g.OnGoexit(key)
			}
		}

		g.mu.Lock()
		if g.calls[key] == c {
			delete(g.calls, key)
		}
		g.mu.Unlock()

		// Remove before waking waiters so new callers cannot join this call.
		close(c.done)
		c.cancel()
	}()

	c.val, c.err = fn(ctx)
	returned = true
}
