package typed

import (
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
)

func TestObserverIsBuiltOnlyForTheCallbacksItDispatchesTo(t *testing.T) {
	cases := []struct {
		name    string
		metrics Metrics
		want    bool
	}{
		{"nothing set", Metrics{}, false},
		{"entry rejected", Metrics{EntryRejected: func(cascache.RejectReason) {}}, true},
		{"store rejected", Metrics{StoreRejected: func() {}}, true},
		{"cleanup failed", Metrics{CleanupFailed: func() {}}, true},
		{"loader panic", Metrics{LoaderPanic: func() {}}, true},
		{
			name:    "only error",
			metrics: Metrics{Error: func(cascache.Op, error) {}},
			want:    false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.metrics.observer() != nil; got != tc.want {
				t.Fatalf("observer() built = %v, want %v", got, tc.want)
			}
		})
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
