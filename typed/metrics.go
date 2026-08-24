package typed

import (
	"context"
	"errors"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
)

// Metrics contains optional callbacks. They must be safe for concurrent use and
// must not block. Use keyed fields because later releases may add callbacks.
// A failed lookup records an error, not a hit or miss.
type Metrics struct {
	// Hit records a successful cache lookup.
	Hit func()

	// Miss records a cache lookup that did not produce a value.
	Miss func()

	// Fill records a value the backend accepted, with the TTL applied.
	Fill func(ttl time.Duration)

	// SetSkipped records a write refused because its snapshot is no longer
	// current.
	SetSkipped func()

	// Invalidated records a successful invalidation.
	Invalidated func()

	// Error records an operation error.
	Error func(op cascache.Op, err error)

	// Load records whether a call ran the loader or shared another call's run.
	Load func(outcome cascache.LoadOutcome)

	// LoadFailed is called once per failed loader run, however many callers
	// shared it.
	LoadFailed func(err error)

	// LoadCanceled records a caller that stopped waiting for a load.
	LoadCanceled func()

	// LoadReloaded records a load that rejected an older shared result and joined
	// a new run.
	LoadReloaded func()

	// EntryRejected records an entry that failed validation. Repeated
	// [cascache.RejectStateMissing] events usually mean invalidation state expires
	// before the values it judges.
	EntryRejected func(reason cascache.RejectReason)

	// StoreRejected records a write the backend declined under its own
	// admission or memory policy.
	StoreRejected func()

	// CleanupFailed records a failed best-effort delete.
	CleanupFailed func()

	// LoaderPanic records a loader panic.
	LoaderPanic func()
}

// Build an observer only when a health callback is set.
func (m Metrics) observer() cascache.Observer {
	if m.EntryRejected == nil && m.StoreRejected == nil &&
		m.CleanupFailed == nil && m.LoaderPanic == nil {
		return nil
	}
	return cascache.ObserverFunc(m.observe)
}

// Build a load callback only when a load metric is set.
// A nil one lets the core skip its own reporting too, not just this dispatch.
func (m Metrics) loadFunc() cascache.LoadFunc {
	if m.Hit == nil && m.Miss == nil &&
		m.Fill == nil && m.SetSkipped == nil &&
		m.Error == nil && m.Load == nil &&
		m.LoadFailed == nil && m.LoadCanceled == nil &&
		m.LoadReloaded == nil {
		return nil
	}
	return func(_ context.Context, info cascache.LoadInfo) {
		m.observeLoad(info)
	}
}

func (m Metrics) observe(e cascache.Event) {
	switch e.Type {
	case cascache.EventEntryRejected:
		if m.EntryRejected != nil {
			m.EntryRejected(e.Reason)
		}
	case cascache.EventStoreRejected:
		if m.StoreRejected != nil {
			m.StoreRejected()
		}
	case cascache.EventCleanupFailed:
		if m.CleanupFailed != nil {
			m.CleanupFailed()
		}
	case cascache.EventOperationFailed:
		// Call sites report returned errors; do not count them twice.
	case cascache.EventLoaderPanic:
		if m.LoaderPanic != nil {
			m.LoaderPanic()
		}
	default:
		// Ignore event types added by newer versions.
	}
}

func (m Metrics) observeLoad(info cascache.LoadInfo) {
	switch {
	case info.Outcome == cascache.LoadOutcomeHit:
		if m.Hit != nil {
			m.Hit()
		}
	case info.Missed:
		if m.Miss != nil {
			m.Miss()
		}
	}

	if info.LookupErr != nil && m.Error != nil {
		m.Error(cascache.OpGet, info.LookupErr)
	}

	switch info.Outcome {
	case cascache.LoadOutcomeLoaded, cascache.LoadOutcomeShared:
		if m.Load != nil {
			m.Load(info.Outcome)
		}
	case cascache.LoadOutcomeHit, cascache.LoadOutcomeUnknown:
	}

	if info.Outcome == cascache.LoadOutcomeLoaded && info.Err != nil && m.LoadFailed != nil {
		m.LoadFailed(info.Err)
	}
	if info.Canceled && m.LoadCanceled != nil {
		m.LoadCanceled()
	}
	if info.Reloaded && m.LoadReloaded != nil {
		m.LoadReloaded()
	}

	for _, fill := range info.Fills {
		m.observeSet(fill.Result, fill.Err)
	}
}

func (m Metrics) observeSet(res cascache.SetResult, err error) {
	if err != nil {
		if m.Error != nil {
			// Fill errors may come from snapshot, TTL, or set.
			op := cascache.OpSet
			var opErr *cascache.OpError
			if errors.As(err, &opErr) {
				op = opErr.Op
			}
			m.Error(op, err)
		}
		return
	}

	switch res.Outcome {
	case cascache.SetOutcomeStored:
		if m.Fill != nil {
			m.Fill(res.EffectiveTTL)
		}
	case cascache.SetOutcomeConflict:
		if m.SetSkipped != nil {
			m.SetSkipped()
		}
	case cascache.SetOutcomeBackendRejected:
		// Reported through EventStoreRejected.
	case cascache.SetOutcomeUnknown, cascache.SetOutcomeDisabled:
	}
}
