package cascache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
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

// LoadFill reports one fill attempt. Result is set when the backend checked the
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

// LoadFunc observes completed loads and must not block.
// The context may already be done.
type LoadFunc func(ctx context.Context, info LoadInfo)

// Load returns a cached value or calls load after a miss. Concurrent calls for
// the same missing key share one loader run.
//
// Invalidation during a load blocks the cache write. An older shared result is
// retried unless confirmed current after this call began. Cache errors go to
// [LoadInfo]; callers still receive the loaded value.
//
// A run is canceled when no callers remain or [Options.LoadTimeout] expires.
// Loaders must honor cancellation or may overlap a new run. Loader panics return
// a [*PanicError].
func (c *Cache[V]) Load(ctx context.Context, key string, load Loader[V]) (V, error) {
	var zero V
	if load == nil {
		return zero, ErrNoLoader
	}

	l := loadCall[V]{
		cache: c,
		key:   key,
		load:  load,
		info:  LoadInfo{Key: key},
	}
	if c.onLoad != nil {
		l.start = time.Now()
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
	bkey  backend.Key
	load  Loader[V]

	start time.Time

	// Shared results must be newer than since.
	since flight.Stamp

	info LoadInfo
}

type miss struct {
	ok bool

	// since was taken before the lookup.
	since flight.Stamp

	fence backend.Fence
}

func (l *loadCall[V]) run(ctx context.Context) (V, error) {
	var zero V

	if l.cache.inv.disabled {
		l.info.Outcome = LoadOutcomeLoaded
		return l.load(ctx)
	}

	l.bkey = l.cache.inv.space.Key(l.key)
	l.since = l.cache.fills.Now(l.key)
	v, seen, ok := l.lookup(ctx)
	if ok {
		l.info.Outcome = LoadOutcomeHit
		return v, nil
	}

	res, err := l.attempt(ctx, seen)
	if err != nil {
		return zero, err
	}
	if res.current > l.since {
		return res.value, nil
	}

	// This result may predate an invalidation. Retry without reusing the miss;
	// the old run has already left the group.
	l.info.Reloaded = true
	res, err = l.attempt(ctx, miss{})
	if err != nil {
		return zero, err
	}
	return res.value, nil
}

func (l *loadCall[V]) attempt(ctx context.Context, seen miss) (fillResult[V], error) {
	res, role, err := l.cache.fills.Do(ctx, l.key, l.cache.fill(l.key, l.bkey, l.load, seen))

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

func (l *loadCall[V]) lookup(ctx context.Context) (V, miss, bool) {
	var seen miss
	switch e, fence, err := l.cache.get(ctx, l.key, l.bkey); {
	case err != nil:
		l.noteLookupErr(ctx, err)
	case e.ok:
		return e.val, seen, true
	default:
		l.info.Missed = true
		seen = miss{ok: true, since: l.since, fence: fence}
	}
	var zero V
	return zero, seen, false
}

func (l *loadCall[V]) adopt(ctx context.Context, res fillResult[V]) {
	if res.missed {
		l.info.Missed = true
	}
	if res.lookupErr != nil {
		l.noteLookupErr(ctx, res.lookupErr)
	}
	// Fills is only reported to OnLoad.
	if l.cache.onLoad != nil && (res.fill.Outcome != SetOutcomeUnknown || res.fillErr != nil) {
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

	// Stamp before the read, load, or fence check. Zero forces a retry.
	current flight.Stamp

	ranLoader bool

	missed    bool
	lookupErr error
	fill      SetResult
	fillErr   error
}

// fill must not capture a loadCall: another caller may report this shared run,
// causing concurrent writes to LoadInfo. Return a fillResult for adopt to copy.
func (c *Cache[V]) fill(
	key string,
	bkey backend.Key,
	load Loader[V],
	seen miss,
) func(context.Context) (fillResult[V], error) {
	return func(ctx context.Context) (fillResult[V], error) {
		var res fillResult[V]
		if err := ctx.Err(); err != nil {
			return res, err
		}

		fence := seen.fence
		// Reuse the miss unless an earlier fill in this Cache may have written.
		if !seen.ok || c.fills.EndedSince(key, seen.since) {
			res.current = c.fills.Tick(key)
			e, f, err := c.get(ctx, key, bkey)
			if e.ok {
				res.value = e.val
				return res, nil
			}
			fence, res.lookupErr = f, err
		}
		res.missed = res.lookupErr == nil

		// Reusing the read's fence skips the TTL refresh. Expiry during the load
		// will reject the fill.
		snapshot := Snapshot{fence: fence}
		var snapshotErr error
		if !fence.Valid() {
			snapshot, snapshotErr = c.snapshot(ctx, key, bkey)
		}
		if err := ctx.Err(); err != nil {
			return res, err
		}

		res.ranLoader = true
		res.current = c.fills.Tick(key)
		v, err := load(ctx)
		if err != nil {
			return res, err
		}
		res.value = v

		// Skip late fills and report cancellation through LoadInfo.
		if err := ctx.Err(); err != nil {
			res.fillErr = &OpError{Op: OpSet, Key: key, Err: err}
			return res, nil //nolint:nilerr // the loader succeeded; only the fill did not
		}

		if snapshotErr != nil {
			res.fillErr = snapshotErr
			return res, nil //nolint:nilerr // the loader succeeded; only the fill did not
		}

		ttl, err := c.computeTTL()
		if err != nil {
			res.fillErr = c.opErr(OpComputeTTL, key, wrapComputeTTL(err))
			return res, nil
		}

		attempted := c.fills.Tick(key)
		res.fill, res.fillErr = c.set(ctx, key, bkey, v, snapshot, ttl)
		if provenCurrent(res.fill.Outcome) {
			res.current = attempted
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
