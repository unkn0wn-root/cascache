package cascache_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
)

type loadHarness struct {
	*harness

	mu    sync.Mutex
	infos []cascache.LoadInfo
}

func newLoadHarness(t testing.TB, tweak func(*cascache.Options[user])) *loadHarness {
	lh := &loadHarness{}
	lh.harness = newHarness(t, func(o *cascache.Options[user]) {
		o.OnLoad = func(_ context.Context, info cascache.LoadInfo) {
			lh.mu.Lock()
			lh.infos = append(lh.infos, info)
			lh.mu.Unlock()
		}
		if tweak != nil {
			tweak(o)
		}
	})
	return lh
}

func (lh *loadHarness) reports() []cascache.LoadInfo {
	lh.mu.Lock()
	defer lh.mu.Unlock()
	return append([]cascache.LoadInfo(nil), lh.infos...)
}

func (lh *loadHarness) outcomes() []cascache.LoadOutcome {
	var out []cascache.LoadOutcome
	for _, info := range lh.reports() {
		out = append(out, info.Outcome)
	}
	return out
}

type account struct {
	Outcome cascache.LoadOutcome
	Runs    int
}

func (lh *loadHarness) accounts() []account {
	var out []account
	for _, info := range lh.reports() {
		out = append(out, account{info.Outcome, info.Runs})
	}
	return out
}

func onlyFill(t testing.TB, info cascache.LoadInfo) cascache.LoadFill {
	t.Helper()
	if len(info.Fills) != 1 {
		t.Fatalf("fills = %+v, want exactly one", info.Fills)
	}
	return info.Fills[0]
}

func loaderFor(v user, calls *atomic.Int64) cascache.Loader[user] {
	return func(context.Context) (user, error) {
		if calls != nil {
			calls.Add(1)
		}
		return v, nil
	}
}

func TestLoadFillsThenHits(t *testing.T) {
	lh := newLoadHarness(t, nil)
	ctx := context.Background()

	var calls atomic.Int64
	load := loaderFor(ada, &calls)

	for range 3 {
		got, err := lh.cache.Load(ctx, "42", load)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got != ada {
			t.Fatalf("Load = %+v, want %+v", got, ada)
		}
	}

	if calls.Load() != 1 {
		t.Fatalf("the loader ran %d times, want 1", calls.Load())
	}
	want := []cascache.LoadOutcome{
		cascache.LoadOutcomeLoaded,
		cascache.LoadOutcomeHit,
		cascache.LoadOutcomeHit,
	}
	got := lh.outcomes()
	if len(got) != len(want) {
		t.Fatalf("outcomes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("outcomes = %v, want %v", got, want)
		}
	}

	first := lh.reports()[0]
	if !first.Missed || onlyFill(t, first).Result.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("first report = %+v, want a miss that filled", first)
	}
}

func TestLoadSkipsTheFillWhenTheFenceMoves(t *testing.T) {
	lh := newLoadHarness(t, nil)
	ctx := context.Background()

	got, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
		if err := lh.cache.Invalidate(ctx, "42"); err != nil {
			return user{}, err
		}
		return ada, nil
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != ada {
		t.Fatalf("Load = %+v, want the loaded value %+v", got, ada)
	}

	report := lh.reports()[0]
	if fill := onlyFill(t, report); fill.Result.Outcome != cascache.SetOutcomeConflict {
		t.Fatalf("fill outcome = %v, want conflict", fill.Result.Outcome)
	}
	if _, ok := lh.storedBytes(t, "42"); ok {
		t.Fatal("a value retired mid-load was cached")
	}
}

func TestLoadServesFromTheSourceWhenTheCacheIsUnreachable(t *testing.T) {
	lh := newLoadHarness(t, nil)

	lh.fill(t, "42", ada)

	failure := errors.New("fence store is down")
	lh.fences.setFailure(failure)

	var calls atomic.Int64
	got, err := lh.cache.Load(context.Background(), "42", loaderFor(user{ID: "42", Name: "Grace"}, &calls))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "Grace" {
		t.Fatalf("Load = %+v, want the freshly loaded value", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("the loader ran %d times, want 1", n)
	}

	report := lh.reports()[0]
	if !errors.Is(report.LookupErr, failure) {
		t.Fatalf("LookupErr = %v, want %v", report.LookupErr, failure)
	}
	if onlyFill(t, report).Err == nil {
		t.Fatal("the skipped fill was not reported")
	}
	if report.Missed {
		t.Fatal("a failed lookup was counted as a miss")
	}
}

func TestLoadMissDoesNotNeedTheFenceStore(t *testing.T) {
	lh := newLoadHarness(t, nil)
	lh.fences.setFailure(errors.New("fence store is down"))

	got, err := lh.cache.Load(context.Background(), "42", loaderFor(ada, nil))
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v", got, err)
	}

	report := lh.reports()[0]
	if report.LookupErr != nil {
		t.Fatalf("LookupErr = %v, want none for an empty key", report.LookupErr)
	}
	if !report.Missed {
		t.Fatal("an empty key was not reported as a miss")
	}
}

func TestLoadCoalescesConcurrentMisses(t *testing.T) {
	lh := newLoadHarness(t, nil)
	ctx := context.Background()

	const callers = 24
	var (
		calls   atomic.Int64
		release = make(chan struct{})
		start   = make(chan struct{})
		wg      sync.WaitGroup
	)

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
				calls.Add(1)
				<-release
				return ada, nil
			})
			if err != nil || got != ada {
				t.Errorf("Load = %+v, %v", got, err)
			}
		}()
	}
	close(start)

	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("the loader ran %d times, want 1", n)
	}

	var loaded, fills int
	for _, info := range lh.reports() {
		if info.Outcome == cascache.LoadOutcomeLoaded {
			loaded++
		}
		for _, fill := range info.Fills {
			if fill.Result.Outcome == cascache.SetOutcomeStored {
				fills++
			}
		}
		if info.Reloaded {
			t.Fatal("a caller loaded again: a run that proved its value current is enough to share")
		}
	}
	if loaded != 1 {
		t.Fatalf("%d callers claimed the loader run, want 1", loaded)
	}
	if fills != 1 {
		t.Fatalf("the fill was reported %d times, want once", fills)
	}
}

func TestLoadCancelingOneCallerDoesNotFailTheOthers(t *testing.T) {
	lh := newLoadHarness(t, nil)

	var (
		started = make(chan struct{})
		release = make(chan struct{})
		wg      sync.WaitGroup
	)

	load := func(context.Context) (user, error) {
		close(started)
		<-release
		return ada, nil
	}

	quitting, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := lh.cache.Load(quitting, "42", load); !errors.Is(err, context.Canceled) {
			t.Errorf("abandoning caller error = %v, want context.Canceled", err)
		}
	}()

	<-started

	var (
		got     user
		gotErr  error
		joined  sync.WaitGroup
		waiting = context.Background()
	)
	joined.Add(1)
	go func() {
		defer joined.Done()
		got, gotErr = lh.cache.Load(waiting, "42", load)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	close(release)
	joined.Wait()

	if gotErr != nil || got != ada {
		t.Fatalf("the remaining caller got %+v, %v", got, gotErr)
	}
}

func TestLoadReportsAPanicRatherThanRaisingIt(t *testing.T) {
	lh := newLoadHarness(t, nil)

	_, err := lh.cache.Load(context.Background(), "42", func(context.Context) (user, error) {
		panic("boom")
	})
	if !errors.Is(err, cascache.ErrLoaderPanic) {
		t.Fatalf("Load error = %v, want ErrLoaderPanic", err)
	}

	var panicErr *cascache.PanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error is not a *PanicError: %#v", err)
	}
	if panicErr.Value != "boom" || len(panicErr.Stack) == 0 {
		t.Fatalf("PanicError = %+v, want the value and a stack", panicErr)
	}

	e, found := lh.events.find(cascache.EventLoaderPanic)
	if !found || e.Key != "42" {
		t.Fatalf("events = %+v, want a loader_panic for key 42", lh.events.all())
	}
}

func TestLoadReportsALoaderThatExitsItsGoroutine(t *testing.T) {
	lh := newLoadHarness(t, nil)

	_, err := lh.cache.Load(context.Background(), "42", func(context.Context) (user, error) {
		runtime.Goexit()
		return ada, nil
	})
	if !errors.Is(err, cascache.ErrLoaderGoexit) {
		t.Fatalf("Load error = %v, want ErrLoaderGoexit", err)
	}

	e, found := lh.events.find(cascache.EventLoaderPanic)
	if !found || e.Key != "42" || !errors.Is(e.Err, cascache.ErrLoaderGoexit) {
		t.Fatalf("events = %+v, want a loader_panic with ErrLoaderGoexit for key 42", lh.events.all())
	}
}

func TestLoadCountsTheLoaderRun(t *testing.T) {
	tests := []struct {
		name     string
		disabled bool
		cached   bool
		load     cascache.Loader[user]
		want     account
	}{
		{name: "hit", cached: true, load: loaderFor(ada, nil), want: account{cascache.LoadOutcomeHit, 0}},
		{name: "value", load: loaderFor(ada, nil), want: account{cascache.LoadOutcomeLoaded, 1}},
		{
			name: "error",
			load: func(context.Context) (user, error) { return user{}, errors.New("source is down") },
			want: account{cascache.LoadOutcomeLoaded, 1},
		},
		{
			name: "panic",
			load: func(context.Context) (user, error) { panic("boom") },
			want: account{cascache.LoadOutcomeLoaded, 1},
		},
		{
			name: "goexit",
			load: func(context.Context) (user, error) {
				runtime.Goexit()
				return ada, nil
			},
			want: account{cascache.LoadOutcomeLoaded, 1},
		},
		{
			name:     "disabled cache",
			disabled: true,
			load:     loaderFor(ada, nil),
			want:     account{cascache.LoadOutcomeLoaded, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lh := newLoadHarness(t, func(o *cascache.Options[user]) { o.Disabled = tt.disabled })
			if tt.cached {
				lh.fill(t, "42", ada)
			}

			_, _ = lh.cache.Load(context.Background(), "42", tt.load)
			if got := lh.accounts(); len(got) != 1 || got[0] != tt.want {
				t.Fatalf("reports = %+v, want [%+v]", got, tt.want)
			}
		})
	}
}

func TestLoadCountsASharedRunOnce(t *testing.T) {
	tests := []struct {
		name    string
		result  func() (user, error)
		wantErr error
	}{
		{name: "value", result: func() (user, error) { return ada, nil }},
		{name: "panic", result: func() (user, error) { panic("boom") }, wantErr: cascache.ErrLoaderPanic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lh := newLoadHarness(t, nil)
			ctx := context.Background()

			loading := make(chan struct{})
			release := make(chan struct{})
			starterDone := make(chan struct{})
			// The joiner releases the run and waits for the starter to collect it.
			lh.cache.SetOnJoin(func(string) {
				close(release)
				<-starterDone
			})

			go func() {
				defer close(starterDone)
				_, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
					close(loading)
					<-release
					return tt.result()
				})
				if !errors.Is(err, tt.wantErr) {
					t.Errorf("starter Load = %v, want %v", err, tt.wantErr)
				}
			}()
			<-loading

			if _, err := lh.cache.Load(ctx, "42", loaderFor(ada, nil)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("joiner Load = %v, want %v", err, tt.wantErr)
			}

			want := []account{{cascache.LoadOutcomeLoaded, 1}, {cascache.LoadOutcomeShared, 0}}
			if got := lh.accounts(); !slices.Equal(got, want) {
				t.Fatalf("reports = %+v, want %+v", got, want)
			}
		})
	}
}

// stallingReads holds reads until their context ends while stall is set.
type stallingReads struct {
	backend.Backend
	stall atomic.Bool
}

func (b *stallingReads) Read(ctx context.Context, key backend.Key) (backend.ReadResult, error) {
	if b.stall.Load() {
		<-ctx.Done()
		return backend.ReadResult{}, ctx.Err()
	}
	return b.Backend.Read(ctx, key)
}

// A joiner can take over a run its starter left and then reload.
func TestLoadCountsBothRunsOfAReload(t *testing.T) {
	tests := []struct {
		name    string
		shared  bool
		leave   bool
		timeout bool
		wantErr error
		calls   int64
		// In report order: starter, other caller if any, joiner.
		want []account
	}{
		{
			name:  "the joiner runs the loader again",
			calls: 2,
			want:  []account{{cascache.LoadOutcomeUnknown, 0}, {cascache.LoadOutcomeLoaded, 2}},
		},
		{
			name:   "the joiner shares another caller's run",
			shared: true,
			calls:  2,
			want: []account{
				{cascache.LoadOutcomeUnknown, 0},
				{cascache.LoadOutcomeLoaded, 1},
				{cascache.LoadOutcomeShared, 1},
			},
		},
		{
			name:    "the joiner leaves the second run",
			leave:   true,
			wantErr: context.Canceled,
			calls:   2,
			want:    []account{{cascache.LoadOutcomeUnknown, 0}, {cascache.LoadOutcomeUnknown, 1}},
		},
		{
			name:    "the second run times out before loading",
			timeout: true,
			wantErr: context.DeadlineExceeded,
			calls:   1,
			want:    []account{{cascache.LoadOutcomeUnknown, 0}, {cascache.LoadOutcomeUnknown, 1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := &stallingReads{}
			lh := newLoadHarness(t, func(o *cascache.Options[user]) {
				reads.Backend = o.Backend
				o.Backend = reads
				if tt.timeout {
					o.LoadTimeout = 100 * time.Millisecond
				}
			})
			ctx := context.Background()

			// Pause each joiner until the test resumes it.
			joined := make(chan struct{})
			resume := make(chan struct{})
			lh.cache.SetOnJoin(func(string) {
				joined <- struct{}{}
				<-resume
			})

			var calls atomic.Int64
			loading := make(chan struct{})
			release := make(chan struct{})
			finished := make(chan struct{})
			starter, cancel := context.WithCancel(ctx)
			starterErr := make(chan error, 1)
			go func() {
				_, err := lh.cache.Load(starter, "42", func(ctx context.Context) (user, error) {
					calls.Add(1)
					context.AfterFunc(ctx, func() { close(finished) })
					close(loading)
					<-release
					return ada, nil
				})
				starterErr <- err
			}()
			<-loading

			joiner, leave := context.WithCancel(ctx)
			defer leave()
			load := loaderFor(ada, &calls)
			if tt.leave {
				load = func(ctx context.Context) (user, error) {
					calls.Add(1)
					leave()
					<-ctx.Done()
					return user{}, ctx.Err()
				}
			}
			joinerErr := make(chan error, 1)
			go func() {
				_, err := lh.cache.Load(joiner, "42", load)
				joinerErr <- err
			}()
			<-joined

			// Make run 1 stale and leave the joiner as its only caller.
			if err := lh.cache.Invalidate(ctx, "42"); err != nil {
				t.Fatal(err)
			}
			cancel()
			if err := <-starterErr; !errors.Is(err, context.Canceled) {
				t.Fatalf("starter error = %v, want context.Canceled", err)
			}
			close(release)
			<-finished

			if tt.shared {
				// Let the joiner collect run 1, and finish run 2 only after it joins.
				_, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
					calls.Add(1)
					resume <- struct{}{}
					<-joined
					return ada, nil
				})
				if err != nil {
					t.Fatalf("other caller: %v", err)
				}
			}
			reads.stall.Store(tt.timeout)
			resume <- struct{}{}
			if err := <-joinerErr; !errors.Is(err, tt.wantErr) {
				t.Fatalf("joiner error = %v, want %v", err, tt.wantErr)
			}

			if n := calls.Load(); n != tt.calls {
				t.Fatalf("the loader ran %d times, want %d", n, tt.calls)
			}
			if got := lh.accounts(); !slices.Equal(got, tt.want) {
				t.Fatalf("reports = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLoadTimeoutBoundsTheLoader(t *testing.T) {
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
	})

	_, err := lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
		<-ctx.Done()
		return user{}, ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Load error = %v, want context.DeadlineExceeded", err)
	}
}

func TestLoadTimeoutRefusesTheFillFromALateLoader(t *testing.T) {
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
	})

	got, err := lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
		<-ctx.Done()
		return ada, nil
	})
	// The caller waited for the loader, so it keeps the value it produced.
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v and no error", got, err, ada)
	}
	if _, ok := lh.mustGet(t, "42"); ok {
		t.Fatal("a loader that returned after its timeout filled the cache")
	}

	fillErr := onlyFill(t, lh.reports()[0]).Err
	if !errors.Is(fillErr, context.DeadlineExceeded) {
		t.Fatalf("fill error = %v, want context.DeadlineExceeded", fillErr)
	}
}

func TestLoadTimeoutRefusesAFillAfterASlowComputeTTL(t *testing.T) {
	var runCtx context.Context
	ready := make(chan struct{})

	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
		o.ComputeTTL = func() (time.Duration, error) {
			<-ready
			<-runCtx.Done()
			return time.Hour, nil
		}
	})

	got, err := lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
		runCtx = ctx
		close(ready)
		return ada, nil
	})
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v and no error", got, err, ada)
	}
	if _, ok := lh.mustGet(t, "42"); ok {
		t.Fatal("a fill started after the run's deadline had passed")
	}

	fillErr := onlyFill(t, lh.reports()[0]).Err
	if !errors.Is(fillErr, context.DeadlineExceeded) {
		t.Fatalf("fill error = %v, want context.DeadlineExceeded", fillErr)
	}
}

func TestLoadTimeoutRefusesAFillAfterASlowSetCost(t *testing.T) {
	var runCtx context.Context
	ready := make(chan struct{})

	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
		o.ComputeSetCost = func(string, []byte) int64 {
			<-ready
			<-runCtx.Done()
			return 1
		}
	})

	got, err := lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
		runCtx = ctx
		close(ready)
		return ada, nil
	})
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v and no error", got, err, ada)
	}
	if _, ok := lh.mustGet(t, "42"); ok {
		t.Fatal("a write was admitted after the run's deadline had passed")
	}

	fillErr := onlyFill(t, lh.reports()[0]).Err
	if !errors.Is(fillErr, context.DeadlineExceeded) {
		t.Fatalf("fill error = %v, want context.DeadlineExceeded", fillErr)
	}
}

func TestLoadTimeoutFreesTheKeyForLaterCallers(t *testing.T) {
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
	})

	release := make(chan struct{})
	expired := make(chan struct{})
	stuck := make(chan struct{})
	defer func() {
		close(release)
		<-stuck
	}()

	go func() {
		defer close(stuck)
		_, _ = lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
			<-ctx.Done()
			close(expired)
			<-release
			return user{}, ctx.Err()
		})
	}()
	<-expired

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var ran atomic.Bool
	got, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
		ran.Store(true)
		return ada, nil
	})
	if err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v and no error", got, err, ada)
	}
	if !ran.Load() {
		t.Fatal("a later caller joined the run that outlived its timeout")
	}
}

func TestLoadRejectsANilLoader(t *testing.T) {
	lh := newLoadHarness(t, nil)

	if _, err := lh.cache.Load(context.Background(), "42", nil); !errors.Is(err, cascache.ErrNoLoader) {
		t.Fatalf("Load(nil) = %v, want ErrNoLoader", err)
	}
	if got := lh.reports(); len(got) != 0 {
		t.Fatalf("reports = %+v, want none", got)
	}
}

func TestLoadOnADisabledCacheRunsTheLoader(t *testing.T) {
	lh := newLoadHarness(t, func(o *cascache.Options[user]) { o.Disabled = true })

	var calls atomic.Int64
	for range 3 {
		got, err := lh.cache.Load(context.Background(), "42", loaderFor(ada, &calls))
		if err != nil || got != ada {
			t.Fatalf("Load = %+v, %v", got, err)
		}
	}

	if calls.Load() != 3 {
		t.Fatalf("the loader ran %d times, want 3", calls.Load())
	}
	if lh.store.Len() != 0 {
		t.Fatal("a disabled cache wrote to the store")
	}
	for _, info := range lh.reports() {
		if info.Missed {
			t.Fatal("a disabled cache reported a miss")
		}
	}
}

func TestLoadPropagatesTheLoaderError(t *testing.T) {
	lh := newLoadHarness(t, nil)
	failure := errors.New("source is down")

	_, err := lh.cache.Load(context.Background(), "42", func(context.Context) (user, error) {
		return user{}, failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("Load error = %v, want %v", err, failure)
	}
	if _, ok := lh.storedBytes(t, "42"); ok {
		t.Fatal("a failed load cached something")
	}
}

func TestLoadUsesComputeTTL(t *testing.T) {
	const ttl = 90 * time.Second
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.ComputeTTL = func() (time.Duration, error) { return ttl, nil }
	})

	if _, err := lh.cache.Load(context.Background(), "42", loaderFor(ada, nil)); err != nil {
		t.Fatal(err)
	}
	if got := onlyFill(t, lh.reports()[0]).Result.EffectiveTTL; got != ttl {
		t.Fatalf("EffectiveTTL = %v, want %v", got, ttl)
	}
}

func TestLoadReturnsTheValueWhenComputeTTLFails(t *testing.T) {
	failure := errors.New("no ttl for you")
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		o.ComputeTTL = func() (time.Duration, error) { return 0, failure }
	})

	got, err := lh.cache.Load(context.Background(), "42", loaderFor(ada, nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got != ada {
		t.Fatalf("Load = %+v, want %+v", got, ada)
	}

	fillErr := onlyFill(t, lh.reports()[0]).Err
	if !errors.Is(fillErr, cascache.ErrComputeTTL) || !errors.Is(fillErr, failure) {
		t.Fatalf("fill error = %v, want ErrComputeTTL wrapping %v", fillErr, failure)
	}

	var opErr *cascache.OpError
	if !errors.As(fillErr, &opErr) || opErr.Op != cascache.OpComputeTTL {
		t.Fatalf("fill error operation = %v, want compute_ttl", opErr)
	}
	if _, ok := lh.storedBytes(t, "42"); ok {
		t.Fatal("a value with no TTL decision was cached")
	}

	e, found := lh.events.find(cascache.EventOperationFailed)
	if !found || e.Op != cascache.OpComputeTTL {
		t.Fatalf("observed %+v, want an operation_failed for compute_ttl", lh.events.all())
	}
}

func TestLoadOutcomeString(t *testing.T) {
	cases := map[cascache.LoadOutcome]string{
		cascache.LoadOutcomeUnknown: "unknown",
		cascache.LoadOutcomeHit:     "hit",
		cascache.LoadOutcomeLoaded:  "loaded",
		cascache.LoadOutcomeShared:  "shared",
	}
	for outcome, want := range cases {
		if got := outcome.String(); got != want {
			t.Fatalf("LoadOutcome(%d).String() = %q, want %q", outcome, got, want)
		}
	}
}

// Repeat until the scheduler makes the second caller share the older run.
func TestLoadDoesNotServeAValueRetiredBeforeTheCallerAsked(t *testing.T) {
	for range 64 {
		if sharedARunFromBeforeItAsked(t) {
			return
		}
	}
	t.Fatal("the second caller never shared the run already in flight")
}

func sharedARunFromBeforeItAsked(t *testing.T) bool {
	t.Helper()

	lh := newLoadHarness(t, nil)
	ctx := context.Background()

	reading := make(chan struct{})
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		got, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
			close(reading)
			<-release
			return user{ID: "42", Name: "before"}, nil
		})
		if err != nil || got.Name != "before" {
			t.Errorf("first Load = %+v, %v; want the value it asked for", got, err)
		}
	}()
	<-reading

	if err := lh.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}

	asking := make(chan struct{})
	var got user
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(asking)
		var err error
		got, err = lh.cache.Load(ctx, "42", loaderFor(user{ID: "42", Name: "after"}, nil))
		if err != nil {
			t.Errorf("second Load: %v", err)
		}
	}()

	<-asking
	close(release)
	wg.Wait()

	if got.Name != "after" {
		t.Fatalf("Load returned %q: a value retired before the caller asked for it", got.Name)
	}

	var stored, refused int
	shared := false
	for _, info := range lh.reports() {
		shared = shared || info.Reloaded
		for _, fill := range info.Fills {
			if fill.Result.Outcome == cascache.SetOutcomeStored {
				stored++
			}
			if fill.Result.Outcome == cascache.SetOutcomeConflict {
				refused++
			}
		}
	}
	if stored != 1 || refused != 1 {
		t.Fatalf("fills = %d stored, %d refused; want one of each, each counted once", stored, refused)
	}
	return shared
}

// After an invalidation is acknowledged, later loads must not return an older
// version.
func TestLoadNeverServesAVersionOlderThanTheCall(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	var version, acked atomic.Int64
	load := func(context.Context) (user, error) {
		read := version.Load()
		time.Sleep(200 * time.Microsecond)
		return user{ID: "42", Name: strconv.FormatInt(read, 10)}, nil
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for range 300 {
			next := version.Add(1)
			if err := h.cache.Invalidate(ctx, "42"); err != nil {
				t.Errorf("Invalidate: %v", err)
				return
			}
			acked.Store(next)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				want := acked.Load()
				got, err := h.cache.Load(ctx, "42", load)
				if err != nil {
					t.Errorf("Load: %v", err)
					return
				}
				served, err := strconv.ParseInt(got.Name, 10, 64)
				if err != nil {
					t.Errorf("Load returned %+v", got)
					return
				}
				if served < want {
					t.Errorf(
						"Load returned version %d for a call that started after version %d was acknowledged",
						served,
						want,
					)
					return
				}
			}
		}()
	}
	wg.Wait()
}

type probe struct {
	backend.Backend
	missFences backend.FenceStore

	reads, ensures, stores atomic.Int64

	// Set only when no reads are in flight.
	onRead func(n int64)
}

func (p *probe) Read(ctx context.Context, key backend.Key) (backend.ReadResult, error) {
	r, err := p.Backend.Read(ctx, key)
	if err == nil && !r.Found && p.missFences != nil {
		r.Fence, r.FenceFound, err = p.missFences.Read(ctx, key)
	}
	n := p.reads.Add(1)
	if p.onRead != nil {
		p.onRead(n)
	}
	return r, err
}

func (p *probe) Ensure(ctx context.Context, key backend.Key, candidate backend.Fence) (backend.Fence, error) {
	p.ensures.Add(1)
	return p.Backend.Ensure(ctx, key, candidate)
}

func (p *probe) CompareAndStore(ctx context.Context, req backend.StoreRequest) (backend.StoreResult, error) {
	p.stores.Add(1)
	return p.Backend.CompareAndStore(ctx, req)
}

func newProbedLoadHarness(
	t testing.TB,
	fenceOnMiss bool,
	tweak func(*cascache.Options[user]),
) (*loadHarness, *probe) {
	p := &probe{}
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		p.Backend = o.Backend
		o.Backend = p
		if tweak != nil {
			tweak(o)
		}
	})
	if fenceOnMiss {
		p.missFences = lh.fences
	}
	return lh, p
}

func TestLoadMissReadsTheCacheOnce(t *testing.T) {
	tests := []struct {
		name        string
		fenceOnMiss bool
		fenced      bool
		ensures     int64
	}{
		{name: "backend reports no fence on a miss", fenced: true, ensures: 1},
		{name: "backend reports the fence on a miss", fenceOnMiss: true, fenced: true, ensures: 0},
		{name: "key has no fence yet", fenceOnMiss: true, ensures: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lh, p := newProbedLoadHarness(t, tt.fenceOnMiss, nil)
			ctx := context.Background()
			if tt.fenced {
				if err := lh.cache.Invalidate(ctx, "42"); err != nil {
					t.Fatal(err)
				}
			}

			var calls atomic.Int64
			if got, err := lh.cache.Load(ctx, "42", loaderFor(ada, &calls)); err != nil || got != ada {
				t.Fatalf("Load = %+v, %v; want %+v", got, err, ada)
			}

			if n := p.reads.Load(); n != 1 {
				t.Errorf("reads = %d, want 1: no other run could have filled the cache", n)
			}
			if n := p.ensures.Load(); n != tt.ensures {
				t.Errorf("ensures = %d, want %d", n, tt.ensures)
			}
			if n := p.stores.Load(); n != 1 {
				t.Errorf("stores = %d, want 1", n)
			}
			if got, ok := lh.mustGet(t, "42"); !ok || got != ada {
				t.Fatalf("Get = %+v, %v; want the filled value", got, ok)
			}
			if n := calls.Load(); n != 1 {
				t.Fatalf("the loader ran %d times, want 1", n)
			}
		})
	}
}

func TestLoadLateCallerTakesTheValueItJustMissed(t *testing.T) {
	for _, fenceOnMiss := range []bool{false, true} {
		t.Run("fenceOnMiss="+strconv.FormatBool(fenceOnMiss), func(t *testing.T) {
			lh, p := newProbedLoadHarness(t, fenceOnMiss, nil)
			ctx := context.Background()

			var calls atomic.Int64
			loading := make(chan struct{})
			release := make(chan struct{})
			firstDone := make(chan struct{})
			go func() {
				defer close(firstDone)
				got, err := lh.cache.Load(ctx, "42", func(context.Context) (user, error) {
					calls.Add(1)
					close(loading)
					<-release
					return ada, nil
				})
				if err != nil || got != ada {
					t.Errorf("first Load = %+v, %v", got, err)
				}
			}()
			<-loading

			// Pause the late caller after its miss.
			lookup := p.reads.Load() + 1
			missed := make(chan struct{})
			proceed := make(chan struct{})
			p.onRead = func(n int64) {
				if n == lookup {
					close(missed)
					<-proceed
				}
			}

			late := make(chan user, 1)
			go func() {
				got, err := lh.cache.Load(ctx, "42", loaderFor(user{ID: "42", Name: "again"}, &calls))
				if err != nil {
					t.Errorf("late Load: %v", err)
				}
				late <- got
			}()
			<-missed

			// Finish the first fill before letting the late caller join.
			close(release)
			<-firstDone
			close(proceed)

			if got := <-late; got != ada {
				t.Fatalf("late Load = %+v, want the stored %+v", got, ada)
			}
			if n := calls.Load(); n != 1 {
				t.Fatalf("the loader ran %d times, want 1", n)
			}
			if n := p.reads.Load(); n != lookup+1 {
				t.Fatalf("reads = %d, want %d: the late run must read again", n, lookup+1)
			}
			want := []account{{cascache.LoadOutcomeLoaded, 1}, {cascache.LoadOutcomeHit, 0}}
			if got := lh.accounts(); !slices.Equal(got, want) {
				t.Fatalf("reports = %+v, want %+v", got, want)
			}
		})
	}
}

func TestLoadFenceFromTheLookupRefusesALaterInvalidation(t *testing.T) {
	tests := []struct {
		name       string
		invalidate func(t *testing.T, lh *loadHarness, p *probe) cascache.Loader[user]
	}{
		{
			name: "between the lookup and the load",
			invalidate: func(t *testing.T, lh *loadHarness, p *probe) cascache.Loader[user] {
				p.onRead = func(int64) {
					if err := lh.cache.Invalidate(context.Background(), "42"); err != nil {
						t.Errorf("Invalidate: %v", err)
					}
				}
				return loaderFor(user{ID: "42", Name: "old"}, nil)
			},
		},
		{
			name: "during the load",
			invalidate: func(_ *testing.T, lh *loadHarness, _ *probe) cascache.Loader[user] {
				return func(ctx context.Context) (user, error) {
					if err := lh.cache.Invalidate(ctx, "42"); err != nil {
						return user{}, err
					}
					return user{ID: "42", Name: "old"}, nil
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lh, p := newProbedLoadHarness(t, true, nil)
			ctx := context.Background()
			if err := lh.cache.Invalidate(ctx, "42"); err != nil {
				t.Fatal(err)
			}

			load := tt.invalidate(t, lh, p)
			if _, err := lh.cache.Load(ctx, "42", load); err != nil {
				t.Fatalf("Load: %v", err)
			}
			p.onRead = nil

			if fill := onlyFill(t, lh.reports()[0]); fill.Result.Outcome != cascache.SetOutcomeConflict {
				t.Fatalf("fill outcome = %v, want conflict", fill.Result.Outcome)
			}
			if n := p.ensures.Load(); n != 0 {
				t.Fatalf("ensures = %d, want the lookup's fence to be used", n)
			}
			if _, ok := lh.storedBytes(t, "42"); ok {
				t.Fatal("a value loaded under a retired fence was cached")
			}
		})
	}
}

func TestLoadReadsAgainAfterReplacingAnExpiredRun(t *testing.T) {
	lh, p := newProbedLoadHarness(t, true, func(o *cascache.Options[user]) {
		o.LoadTimeout = 20 * time.Millisecond
	})

	release := make(chan struct{})
	expired := make(chan struct{})
	stuck := make(chan struct{})
	defer func() {
		close(release)
		<-stuck
	}()

	go func() {
		defer close(stuck)
		_, _ = lh.cache.Load(context.Background(), "42", func(ctx context.Context) (user, error) {
			<-ctx.Done()
			close(expired)
			<-release
			return user{}, ctx.Err()
		})
	}()
	<-expired

	before := p.reads.Load()
	if got, err := lh.cache.Load(context.Background(), "42", loaderFor(ada, nil)); err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, ada)
	}
	if n := p.reads.Load() - before; n != 2 {
		t.Fatalf("reads = %d, want 2: the lookup and a second read", n)
	}
}

type invalidMissFence struct{ *probe }

func (b invalidMissFence) Read(ctx context.Context, key backend.Key) (backend.ReadResult, error) {
	r, err := b.probe.Read(ctx, key)
	if err == nil && !r.Found {
		r.Fence, r.FenceFound = backend.Fence{}, true
	}
	return r, err
}

func TestLoadSnapshotsWhenAMissReportsAnInvalidFence(t *testing.T) {
	p := &probe{}
	lh := newLoadHarness(t, func(o *cascache.Options[user]) {
		p.Backend = o.Backend
		o.Backend = invalidMissFence{p}
	})
	ctx := context.Background()
	if err := lh.cache.Invalidate(ctx, "42"); err != nil {
		t.Fatal(err)
	}

	if got, err := lh.cache.Load(ctx, "42", loaderFor(ada, nil)); err != nil || got != ada {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, ada)
	}
	if n := p.ensures.Load(); n != 1 {
		t.Fatalf("ensures = %d, want 1", n)
	}
	if fill := onlyFill(t, lh.reports()[0]); fill.Result.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("fill outcome = %v, want stored", fill.Result.Outcome)
	}
}
