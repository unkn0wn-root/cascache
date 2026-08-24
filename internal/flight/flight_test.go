package flight

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func joinRecorder[V any](buf int) (*Group[V], <-chan string) {
	joins := make(chan string, buf)
	g := &Group[V]{OnJoin: func(key string) { joins <- key }}
	return g, joins
}

func TestZeroGroupIsUsable(t *testing.T) {
	var g Group[int]
	v, role, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 7, nil
	})
	if err != nil || role != Owned || v != 7 {
		t.Fatalf("Do = (%d, %v, %v), want (7, Owned, nil)", v, role, err)
	}
	if n := g.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

func TestDoCoalescesConcurrentCalls(t *testing.T) {
	const callers = 16

	g, joins := joinRecorder[int](callers)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls, sharedCount atomic.Int64

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, role, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return 42, nil
			})
			switch {
			case err != nil:
				t.Errorf("Do: %v", err)
			case v != 42:
				t.Errorf("value = %d, want 42", v)
			case role == Shared:
				sharedCount.Add(1)
			}
		}()
	}

	<-started
	for range callers - 1 {
		<-joins
	}
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("executions = %d, want 1", n)
	}
	if n := sharedCount.Load(); n != callers-1 {
		t.Fatalf("shared results = %d, want %d", n, callers-1)
	}
	if n := g.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

func TestDoAfterCompletionRunsAgain(t *testing.T) {
	var g Group[int]
	var calls int
	for i := range 3 {
		v, role, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
			calls++
			return calls, nil
		})
		if err != nil || role != Owned || v != i+1 {
			t.Fatalf("call %d = (%d, %v, %v)", i, v, role, err)
		}
	}
	if calls != 3 {
		t.Fatalf("executions = %d, want 3", calls)
	}
}

func TestDoPropagatesError(t *testing.T) {
	var g Group[int]
	sentinel := errors.New("boom")
	_, _, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 0, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
}

func TestWaiterHonorsItsOwnContext(t *testing.T) {
	var g Group[int]
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	go func() {
		_, _, _ = g.Do(context.Background(), "k", func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, role, err := g.Do(ctx, "k", func(context.Context) (int, error) {
		t.Error("waiter ran its own execution")
		return 0, nil
	})
	if role != Abandoned {
		t.Fatalf("role = %v, want Abandoned", role)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestAbandonedCallDoesNotEndItForRemainingCallers(t *testing.T) {
	g, joins := joinRecorder[int](1)
	started := make(chan struct{})
	release := make(chan struct{})

	initiatorCtx, cancelInitiator := context.WithCancel(context.Background())
	initiatorDone := make(chan error, 1)
	go func() {
		_, _, err := g.Do(initiatorCtx, "k", func(context.Context) (int, error) {
			close(started)
			<-release
			return 42, nil
		})
		initiatorDone <- err
	}()
	<-started

	waiterDone := make(chan int, 1)
	go func() {
		v, role, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
			t.Error("waiter ran its own execution")
			return 0, nil
		})
		if err != nil || role != Owned {
			t.Errorf("waiter = (%d, %v, %v), want (42, Owned, nil)", v, role, err)
		}
		waiterDone <- v
	}()
	<-joins

	cancelInitiator()
	if err := <-initiatorDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("initiator err = %v, want context.Canceled", err)
	}

	close(release)
	if v := <-waiterDone; v != 42 {
		t.Fatalf("waiter value = %d, want 42", v)
	}
}

func TestCallIsCanceledWhenEveryCallerLeaves(t *testing.T) {
	var g Group[int]
	started := make(chan struct{})
	callCanceled := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = g.Do(ctx, "k", func(callCtx context.Context) (int, error) {
			close(started)
			<-callCtx.Done()
			close(callCanceled)
			return 0, callCtx.Err()
		})
	}()
	<-started

	cancel()
	<-done

	select {
	case <-callCanceled:
	case <-time.After(time.Second):
		t.Fatal("the shared call kept running after its last caller left")
	}
}

func TestPanicReachesEveryCallerWithoutStranding(t *testing.T) {
	g, joins := joinRecorder[int](1)
	started := make(chan struct{})
	release := make(chan struct{})

	initiatorErr := make(chan error, 1)
	go func() {
		_, _, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
			close(started)
			<-release
			panic("boom")
		})
		initiatorErr <- err
	}()
	<-started

	waiterErr := make(chan error, 1)
	go func() {
		_, _, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
			return 0, errors.New("waiter must not execute")
		})
		waiterErr <- err
	}()
	<-joins
	close(release)

	for name, ch := range map[string]chan error{"initiator": initiatorErr, "waiter": waiterErr} {
		select {
		case err := <-ch:
			if !errors.Is(err, ErrPanic) {
				t.Fatalf("%s err = %v, want ErrPanic", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s was stranded by a panicking call", name)
		}
	}

	if _, _, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
		return 7, nil
	}); err != nil {
		t.Fatalf("Do after panic: %v", err)
	}
	if n := g.Len(); n != 0 {
		t.Fatalf("Len = %d, want 0", n)
	}
}

func TestGoexitDoesNotStrandCallers(t *testing.T) {
	var g Group[int]

	entered := make(chan struct{})
	initiatorErr := make(chan error, 1)
	go func() {
		var err error
		defer func() { initiatorErr <- err }()
		_, _, err = g.Do(context.Background(), "k", func(context.Context) (int, error) {
			close(entered)
			runtime.Goexit()
			return 0, nil
		})
	}()
	<-entered

	select {
	case err := <-initiatorErr:
		if !errors.Is(err, ErrGoexit) {
			t.Fatalf("initiator err = %v, want ErrGoexit", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the caller was stranded by a call that exited its goroutine")
	}

	next := make(chan error, 1)
	go func() {
		_, _, err := g.Do(context.Background(), "k", func(context.Context) (int, error) {
			return 7, nil
		})
		next <- err
	}()
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("Do after Goexit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("the key stayed poisoned; Len = %d", g.Len())
	}
}

func TestExactlyOneCallerIsAccountable(t *testing.T) {
	const callers = 8

	g, joins := joinRecorder[int](callers)
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var owned atomic.Int64

	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, role, _ := g.Do(context.Background(), "k", func(context.Context) (int, error) {
				once.Do(func() { close(started) })
				<-release
				return 1, nil
			})
			if role == Owned {
				owned.Add(1)
			}
		}()
	}
	<-started
	for range callers - 1 {
		<-joins
	}
	close(release)
	wg.Wait()

	if n := owned.Load(); n != 1 {
		t.Fatalf("accountable callers = %d, want exactly 1", n)
	}
}

func TestNobodyIsAccountableWhenEveryCallerLeaves(t *testing.T) {
	var g Group[int]
	started := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Role, 1)
	go func() {
		_, role, _ := g.Do(ctx, "k", func(callCtx context.Context) (int, error) {
			close(started)
			<-callCtx.Done()
			return 0, callCtx.Err()
		})
		done <- role
	}()
	<-started

	cancel()
	if role := <-done; role != Abandoned {
		t.Fatalf("role = %v, want Abandoned", role)
	}
}

func TestTimeoutBoundsTheCall(t *testing.T) {
	g := &Group[int]{Timeout: 20 * time.Millisecond}

	_, _, err := g.Do(context.Background(), "k", func(callCtx context.Context) (int, error) {
		<-callCtx.Done()
		return 0, callCtx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestDifferentKeysRunConcurrently(t *testing.T) {
	var g Group[int]
	both := make(chan struct{}, 2)
	release := make(chan struct{})

	var wg sync.WaitGroup
	for _, k := range []string{"a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = g.Do(context.Background(), k, func(context.Context) (int, error) {
				both <- struct{}{}
				<-release
				return 0, nil
			})
		}()
	}

	for range 2 {
		select {
		case <-both:
		case <-time.After(time.Second):
			t.Fatal("calls for different keys did not run concurrently")
		}
	}
	close(release)
	wg.Wait()
}

// A ready result wins even when cancellation is also ready.
func TestWaitPrefersACompletedResultOverCancellation(t *testing.T) {
	var g Group[int]

	c := &call[int]{done: make(chan struct{}), cancel: func() {}, waiters: 1, val: 42}
	close(c.done)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	v, role, err := g.wait(ctx, "k", c)
	if err != nil || v != 42 {
		t.Fatalf("wait = (%d, %v, %v), want the completed result", v, role, err)
	}
	if role != Owned {
		t.Fatalf("role = %v, want Owned: the completed call went unaccounted for", role)
	}
}
