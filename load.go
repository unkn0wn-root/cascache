package cascache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/unkn0wn-root/cascache/v4/internal/flight"
)

// LoadOutcome describes how [Cache.Load] got its result.
type LoadOutcome uint8

const (
	// LoadOutcomeUnknown means no value was produced.
	LoadOutcomeUnknown LoadOutcome = iota
	// LoadOutcomeHit means the value came from the cache.
	LoadOutcomeHit
	// LoadOutcomeLoaded means this call accounts for the shared loader run.
	// Exactly one caller accounts for each run, even if its starter left early.
	LoadOutcomeLoaded
	// LoadOutcomeShared means this call took the result of another call's run.
	LoadOutcomeShared
)

func (o LoadOutcome) String() string {
	switch o {
	case LoadOutcomeHit:
		return "hit"
	case LoadOutcomeLoaded:
		return "loaded"
	case LoadOutcomeShared:
		return "shared"
	default:
		return "unknown"
	}
}

// LoadFill reports one fill attempt. Result is set when the backend judged the
// write; otherwise Err reports why the attempt stopped.
type LoadFill struct {
	Result SetResult
	Err    error
}

// LoadInfo describes a completed [Cache.Load]. Use keyed fields when building
// one because later releases may add data.
type LoadInfo struct {
	Key     string
	Outcome LoadOutcome

	// Missed is false when the cache is disabled or the lookup failed.
	Missed bool

	// LookupErr records a cache read error even when Load returns a value.
	LookupErr error

	// Fills lists fill attempts assigned to this call for reporting. A shared run
	// assigns its fill to one caller, so this list may be empty.
	Fills []LoadFill

	// Reloaded reports that an older shared result could not be proved current,
	// so Load joined a second run instead of returning it.
	Reloaded bool

	// Err is the error returned by the load.
	Err error

	// Canceled reports that this caller's context ended the call.
	Canceled bool
	Duration time.Duration
}

// LoadFunc observes completed loads. The context may already be done.
// Implementations must be safe for concurrent use and must not block.
type LoadFunc func(ctx context.Context, info LoadInfo)

// Load returns a cached value or calls load after a miss. If several callers
// request the same missing key at the same time, Load calls the loader once and
// returns the result to all of them.
// Load takes a snapshot before calling the loader and checks it before caching
// the result. An invalidation during the load therefore prevents the fill.
// A caller never accepts a shared value retired before the call began. If an
// older run cannot prove its value is current, the caller joins a new run.
// Changes made without an invalidation are governed by the TTL.
// Cache lookup and fill errors do not hide a value returned by the loader. They
// are reported through [LoadInfo].
// Canceling one caller does not cancel a run still used by others. The run ends
// when no callers remain or [Options.LoadTimeout] expires. Cancellation is
// cooperative: load must honor its context. Once a run expires, a later caller
// may start another run for the same key while the expired loader is still
// running, so loader executions can overlap. A run that ends before backend
// admission keeps its value but attempts no fill. Loader panics return a
// [*PanicError]. A disabled cache calls load with the caller's context.
func (c *Cache[V]) Load(ctx context.Context, key string, load Loader[V]) (V, error) {
	var zero V
	if load == nil {
		return zero, ErrNoLoader
	}

	l := loadCall[V]{
		cache: c,
		key:   key,
		load:  load,
		start: time.Now(),
		info:  LoadInfo{Key: key},
	}
	v, err := l.run(ctx)
	if err != nil {
		v = zero
	}
	l.report(ctx, err)
	return v, err
}

type loadCall[V any] struct {
	cache *Cache[V]
	key   string
	load  Loader[V]

	// A shared result must have been known current after this time.
	start time.Time

	info LoadInfo
}

func (l *loadCall[V]) run(ctx context.Context) (V, error) {
	var zero V

	if l.cache.inv.disabled {
		l.info.Outcome = LoadOutcomeLoaded
		return l.load(ctx)
	}

	if v, ok := l.lookup(ctx); ok {
		l.info.Outcome = LoadOutcomeHit
		return v, nil
	}

	res, err := l.attempt(ctx)
	if err != nil {
		return zero, err
	}
	if !res.currentAt.Before(l.start) {
		return res.value, nil
	}

	// This result predates the call and was not proved current. Join a new run
	// rather than return a value an invalidation may have retired. Finished runs
	// leave the group before waking waiters, so the next run starts after this
	// call and its result cannot predate it.
	l.info.Reloaded = true
	res, err = l.attempt(ctx)
	if err != nil {
		return zero, err
	}
	return res.value, nil
}

// Join or start a loader run and record this caller's role in it.
func (l *loadCall[V]) attempt(ctx context.Context) (fillResult[V], error) {
	res, role, err := l.cache.fills.Do(ctx, l.key, l.cache.fill(l.key, l.load))

	// Classify errors too, so only one caller accounts for a failed run.
	switch role {
	case flight.Abandoned:
	case flight.Shared:
		l.info.Outcome = LoadOutcomeShared
	case flight.Owned:
		l.adopt(ctx, res)
		switch {
		case res.ranLoader:
			l.info.Outcome = LoadOutcomeLoaded
		case err == nil:
			l.info.Outcome = LoadOutcomeHit
		}
	}

	if err != nil {
		return res, l.cache.loaderError(err)
	}
	return res, nil
}

func (l *loadCall[V]) lookup(ctx context.Context) (V, bool) {
	switch v, ok, err := l.cache.Get(ctx, l.key); {
	case err != nil:
		l.noteLookupErr(ctx, err)
	case ok:
		return v, true
	default:
		l.info.Missed = true
	}
	var zero V
	return zero, false
}

// Copy the work assigned to this caller from the shared run.
func (l *loadCall[V]) adopt(ctx context.Context, res fillResult[V]) {
	if res.missed {
		l.info.Missed = true
	}
	if res.lookupErr != nil {
		l.noteLookupErr(ctx, res.lookupErr)
	}
	// Hits and loader errors do not attempt a fill.
	if res.fill.Outcome != SetOutcomeUnknown || res.fillErr != nil {
		l.info.Fills = append(l.info.Fills, LoadFill{Result: res.fill, Err: res.fillErr})
	}
}

func (l *loadCall[V]) noteLookupErr(ctx context.Context, err error) {
	if l.info.LookupErr == nil && !endedByContext(ctx, err) {
		l.info.LookupErr = err
	}
}

func (l *loadCall[V]) report(ctx context.Context, err error) {
	if l.cache.onLoad == nil {
		return
	}
	l.info.Err = err
	l.info.Canceled = err != nil && endedByContext(ctx, err)
	l.info.Duration = time.Since(l.start)
	l.cache.onLoad(ctx, l.info)
}

type fillResult[V any] struct {
	value V

	// Lower bound for when the value was known current. It is recorded before
	// the read or fence check, so it can only make freshness look older.
	currentAt time.Time

	ranLoader bool

	missed    bool
	lookupErr error
	fill      SetResult
	fillErr   error
}

func (c *Cache[V]) fill(key string, load Loader[V]) func(context.Context) (fillResult[V], error) {
	return func(ctx context.Context) (fillResult[V], error) {
		var res fillResult[V]
		if err := ctx.Err(); err != nil {
			return res, err
		}

		// Another run may have filled the cache while this one waited.
		res.currentAt = time.Now()
		switch v, ok, err := c.Get(ctx, key); {
		case err != nil:
			res.lookupErr = err
		case ok:
			res.value = v
			return res, nil
		default:
			res.missed = true
		}

		// Snapshot before loading so an invalidation can refuse the later write.
		snapshot, snapshotErr := c.Snapshot(ctx, key)
		if err := ctx.Err(); err != nil {
			return res, err
		}

		res.ranLoader = true
		res.currentAt = time.Now()
		v, err := load(ctx)
		if err != nil {
			return res, err
		}
		res.value = v

		// The run is over, so refuse a write that would give an arbitrarily old
		// value a fresh TTL. The loader still succeeded, so keep its value.
		// SetWithTTL refuses again at admission; this skips the encoding first.
		if err := ctx.Err(); err != nil {
			res.fillErr = &OpError{Op: OpSet, Key: key, Err: err}
			return res, nil //nolint:nilerr // the loader succeeded; only the fill did not
		}

		if snapshotErr != nil {
			// Return the value and report only the fill error.
			res.fillErr = snapshotErr
			return res, nil //nolint:nilerr // the loader succeeded; only the fill did not
		}

		ttl, err := c.computeTTL()
		if err != nil {
			// TTL errors stop the fill, not the successful load.
			res.fillErr = &OpError{Op: OpComputeTTL, Key: key, Err: wrapComputeTTL(err)}
			return res, nil
		}

		attempted := time.Now()
		res.fill, res.fillErr = c.SetWithTTL(ctx, key, v, snapshot, ttl)
		if provenCurrent(res.fill.Outcome) {
			res.currentAt = attempted
		}
		return res, nil
	}
}

// A stored or policy-rejected write proves the fence was still current. Other
// outcomes cannot prove an older shared value is safe to return.
func provenCurrent(o SetOutcome) bool {
	return o == SetOutcomeStored || o == SetOutcomeBackendRejected
}

func wrapComputeTTL(err error) error {
	return fmt.Errorf("%w: %w", ErrComputeTTL, err)
}

// Preserve panic details while translating flight errors to the public API.
func (c *Cache[V]) loaderError(err error) error {
	var panicErr *flight.PanicError
	switch {
	case errors.As(err, &panicErr):
		return &PanicError{Value: panicErr.Value, Stack: panicErr.Stack}
	case errors.Is(err, flight.ErrGoexit):
		return ErrLoaderGoexit
	default:
		return err
	}
}

func endedByContext(ctx context.Context, err error) bool {
	cause := ctx.Err()
	return cause != nil && errors.Is(err, cause)
}
