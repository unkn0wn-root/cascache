package flight

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Keys hash to a clock slot, and each lock shard owns a run of slots.
// Fewer slots save memory but make EndedSince report more false positives.
const (
	clockSlots = 256
	shardCount = 8
	shardSlots = clockSlots / shardCount
)

var clockSeed = maphash.MakeSeed()

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

// Group runs one call per key and shares its result with the other callers.
// Timeout and the hooks are read without locking, so set them before the
// first Do.
type Group[V any] struct {
	// Timeout bounds one call's context. Zero means no timeout. It does not stop
	// a call that ignores its context, but such a call no longer takes joiners.
	Timeout time.Duration

	OnJoin   func(key string)
	OnPanic  func(key string, value any, stack []byte)
	OnGoexit func(key string)

	shards [shardCount]shard[V]
}

type shard[V any] struct {
	mu    sync.Mutex
	calls map[string]*call[V]

	clocks [shardSlots]slotClock
}

// shard.mu serializes updates to returned and detached.
type slotClock struct {
	now      atomic.Uint64
	returned atomic.Uint64
	detached atomic.Int32 // Calls removed from the group but still running.
}

type call[V any] struct {
	ctx    context.Context
	done   chan struct{}
	cancel context.CancelFunc
	// Guarded by shard.mu. The call is canceled when waiters reaches zero.
	waiters int
	// The first caller to collect the result accounts for the call.
	accounted atomic.Bool
	val       V
	err       error
}

// Do runs fn once per key and shares the value fn stores through its pointer,
// and the error it returns, among concurrent callers. Canceling one caller
// leaves fn running for others. Panics and Goexit become errors, and a value
// stored before them is still returned. Exactly one caller receives [Owned] to
// report a completed call.
//
// Finished calls leave the group before waiters wake, so retries join newer calls.
// New callers never join a call whose context has ended, including on timeout.
func (g *Group[V]) Do(
	ctx context.Context,
	key string,
	fn func(context.Context, *V) error,
) (v V, role Role, err error) {
	if err := ctx.Err(); err != nil {
		return v, Abandoned, err
	}

	sh, clk := g.slotOf(key)
	sh.mu.Lock()
	if c, ok := sh.calls[key]; ok {
		if c.ctx.Err() == nil {
			c.waiters++
			sh.mu.Unlock()

			if g.OnJoin != nil {
				g.OnJoin(key)
			}
			return g.wait(ctx, key, c)
		}
		clk.detached.Add(1)
	}

	callCtx, cancel := g.callContext(ctx)
	c := &call[V]{ctx: callCtx, done: make(chan struct{}), cancel: cancel, waiters: 1}
	if sh.calls == nil {
		sh.calls = make(map[string]*call[V])
	}
	sh.calls[key] = c
	sh.mu.Unlock()

	go g.run(key, c, fn)

	return g.wait(ctx, key, c)
}

// Stamp orders events for one key within a Group.
type Stamp uint64

// Now returns the key's current stamp without advancing it.
func (g *Group[V]) Now(key string) Stamp {
	_, c := g.slotOf(key)
	return Stamp(c.now.Load())
}

// Tick advances the key's stamp and returns it.
func (g *Group[V]) Tick(key string) Stamp {
	_, c := g.slotOf(key)
	return Stamp(c.now.Add(1))
}

// EndedSince reports whether a call for key returned after s or is still
// running outside the group. Shared clocks can cause false positives.
func (g *Group[V]) EndedSince(key string, s Stamp) bool {
	_, c := g.slotOf(key)
	// Read detached first; run updates returned before decrementing it.
	return c.detached.Load() > 0 || Stamp(c.returned.Load()) > s
}

func (g *Group[V]) slotOf(key string) (*shard[V], *slotClock) {
	i := maphash.String(clockSeed, key) % clockSlots
	sh := &g.shards[i/shardSlots]
	return sh, &sh.clocks[i%shardSlots]
}

func (g *Group[V]) Len() int {
	n := 0
	for i := range g.shards {
		sh := &g.shards[i]
		sh.mu.Lock()
		n += len(sh.calls)
		sh.mu.Unlock()
	}
	return n
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
	if c.accounted.CompareAndSwap(false, true) {
		return Owned
	}
	return Shared
}

func (g *Group[V]) abandon(key string, c *call[V]) {
	sh, clk := g.slotOf(key)
	sh.mu.Lock()
	c.waiters--
	last := c.waiters == 0
	if last && sh.calls[key] == c {
		// Let new callers start before canceling the old call.
		delete(sh.calls, key)
		clk.detached.Add(1)
	}
	sh.mu.Unlock()

	if last {
		c.cancel()
	}
}

func (g *Group[V]) run(key string, c *call[V], fn func(context.Context, *V) error) {
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

		sh, clk := g.slotOf(key)
		sh.mu.Lock()
		// sh.mu orders the stores, so returned only grows.
		clk.returned.Store(clk.now.Add(1))
		if sh.calls[key] == c {
			delete(sh.calls, key)
		} else {
			clk.detached.Add(-1)
		}
		sh.mu.Unlock()

		// Remove before waking waiters so new callers cannot join this call.
		close(c.done)
		c.cancel()
	}()

	c.err = fn(c.ctx, &c.val)
	returned = true
}
