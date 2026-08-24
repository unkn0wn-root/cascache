package typed

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
)

// Every callback reaches the cache through exactly one hook: the observer for
// health events, the load func for the load path, or a direct call from the
// operation that records it. A callback missing from the hook that dispatches it
// would be set by the caller and never called. Every field of Metrics must
// appear below, which the table checks against Metrics itself.
func TestEveryCallbackIsWiredToTheHookThatDispatchesIt(t *testing.T) {
	cases := []struct {
		field        string // the Metrics callback this case sets
		metrics      Metrics
		wantObserver bool
		wantLoad     bool
	}{
		{field: "", metrics: Metrics{}},

		// The load path reports these, through Cache.Load or the fills it makes.
		{field: "Hit", metrics: Metrics{Hit: func() {}}, wantLoad: true},
		{field: "Miss", metrics: Metrics{Miss: func() {}}, wantLoad: true},
		{field: "Fill", metrics: Metrics{Fill: func(time.Duration) {}}, wantLoad: true},
		{field: "SetSkipped", metrics: Metrics{SetSkipped: func() {}}, wantLoad: true},
		{field: "Load", metrics: Metrics{Load: func(cascache.LoadOutcome) {}}, wantLoad: true},
		{field: "LoadFailed", metrics: Metrics{LoadFailed: func(error) {}}, wantLoad: true},
		{field: "LoadCanceled", metrics: Metrics{LoadCanceled: func() {}}, wantLoad: true},
		{field: "LoadReloaded", metrics: Metrics{LoadReloaded: func() {}}, wantLoad: true},

		// Call sites report returned errors, and observe ignores
		// EventOperationFailed, so Error needs no observer.
		{field: "Error", metrics: Metrics{Error: func(cascache.Op, error) {}}, wantLoad: true},

		// Health events reach a callback only through the observer.
		{
			field:        "EntryRejected",
			metrics:      Metrics{EntryRejected: func(cascache.RejectReason) {}},
			wantObserver: true,
		},
		{field: "StoreRejected", metrics: Metrics{StoreRejected: func() {}}, wantObserver: true},
		{field: "CleanupFailed", metrics: Metrics{CleanupFailed: func() {}}, wantObserver: true},
		{field: "LoaderPanic", metrics: Metrics{LoaderPanic: func() {}}, wantObserver: true},

		// Invalidate calls this one itself, so it needs neither hook.
		{field: "Invalidated", metrics: Metrics{Invalidated: func() {}}},
	}

	covered := make(map[string]bool, len(cases))
	for _, tc := range cases {
		covered[tc.field] = true
		name := tc.field
		if name == "" {
			name = "nothing set"
		}
		t.Run(name, func(t *testing.T) {
			if got := tc.metrics.observer() != nil; got != tc.wantObserver {
				t.Errorf("observer() built = %v, want %v", got, tc.wantObserver)
			}
			if got := tc.metrics.loadFunc() != nil; got != tc.wantLoad {
				t.Errorf("loadFunc() built = %v, want %v", got, tc.wantLoad)
			}
		})
	}

	metrics := reflect.TypeOf(Metrics{})
	for i := range metrics.NumField() {
		if name := metrics.Field(i).Name; !covered[name] {
			t.Errorf("%s is missing from the table; add it with the hook that dispatches it", name)
		}
	}
}

func TestObserverDispatchesEveryEventItClaims(t *testing.T) {
	var rejected, storeRejected, cleanupFailed, loaderPanic int

	obs := Metrics{
		EntryRejected: func(cascache.RejectReason) { rejected++ },
		StoreRejected: func() { storeRejected++ },
		CleanupFailed: func() { cleanupFailed++ },
		LoaderPanic:   func() { loaderPanic++ },
	}.observer()

	for _, t := range []cascache.EventType{
		cascache.EventEntryRejected,
		cascache.EventStoreRejected,
		cascache.EventCleanupFailed,
		cascache.EventLoaderPanic,
		cascache.EventOperationFailed,
		cascache.EventType(200), // one this build does not know about
	} {
		obs.Observe(cascache.Event{Type: t})
	}

	if rejected != 1 || storeRejected != 1 || cleanupFailed != 1 || loaderPanic != 1 {
		t.Fatalf("dispatch counts = %d, %d, %d, %d; want one each",
			rejected, storeRejected, cleanupFailed, loaderPanic)
	}
}

func TestObserveLoadReportsEveryWriteItAccountsFor(t *testing.T) {
	var fills, skipped, reloaded int
	m := Metrics{
		Fill:         func(time.Duration) { fills++ },
		SetSkipped:   func() { skipped++ },
		LoadReloaded: func() { reloaded++ },
	}

	m.observeLoad(cascache.LoadInfo{
		Outcome:  cascache.LoadOutcomeLoaded,
		Missed:   true,
		Reloaded: true,
		Fills: []cascache.LoadFill{
			{Result: cascache.SetResult{Outcome: cascache.SetOutcomeConflict}},
			{Result: cascache.SetResult{Outcome: cascache.SetOutcomeStored, EffectiveTTL: time.Minute}},
		},
	})

	if fills != 1 || skipped != 1 || reloaded != 1 {
		t.Fatalf("fills = %d, skipped = %d, reloaded = %d; want 1 of each", fills, skipped, reloaded)
	}
}

// A failed lookup records an error, not a hit or miss.
func TestObserveLoadReportsLookupErrorsAndCancellation(t *testing.T) {
	var ops []cascache.Op
	var hits, misses, canceled int
	m := Metrics{
		Hit:          func() { hits++ },
		Miss:         func() { misses++ },
		Error:        func(op cascache.Op, _ error) { ops = append(ops, op) },
		LoadCanceled: func() { canceled++ },
	}

	m.observeLoad(cascache.LoadInfo{
		Outcome:   cascache.LoadOutcomeShared,
		LookupErr: errors.New("the store is down"),
		Canceled:  true,
	})

	if len(ops) != 1 || ops[0] != cascache.OpGet {
		t.Errorf("errors = %v, want one %v", ops, cascache.OpGet)
	}
	if canceled != 1 {
		t.Errorf("canceled = %d, want 1", canceled)
	}
	if hits != 0 || misses != 0 {
		t.Errorf("hits = %d, misses = %d; want neither for a failed lookup", hits, misses)
	}
}
