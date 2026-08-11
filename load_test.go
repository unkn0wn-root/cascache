package cascache_test

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
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
