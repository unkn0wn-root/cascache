package cascache

// Op names the cache operation an event came from.
type Op uint8

const (
	// OpUnknown is the zero value and is never reported.
	OpUnknown Op = iota
	// OpGet is a cache lookup, including the lookups Load makes.
	OpGet
	// OpSnapshot is capturing the invalidation state that guards a fill.
	OpSnapshot
	// OpComputeTTL is the TTL calculation that precedes a write.
	OpComputeTTL
	// OpSet is a cache write, including the fills Load makes.
	OpSet
	// OpInvalidate is retiring a key.
	OpInvalidate
	// OpLoad is the loader run itself.
	OpLoad
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpSnapshot:
		return "snapshot"
	case OpComputeTTL:
		return "compute_ttl"
	case OpSet:
		return "set"
	case OpInvalidate:
		return "invalidate"
	case OpLoad:
		return "load"
	default:
		return "unknown"
	}
}

// EventType names what happened.
type EventType uint8

const (
	// EventUnknown is the zero value and is never reported.
	EventUnknown EventType = iota
	// EventEntryRejected means an entry failed validation and became a miss.
	EventEntryRejected
	// EventStoreRejected means the backend declined an otherwise valid write,
	// usually under an admission or memory policy.
	EventStoreRejected
	// EventCleanupFailed means a best-effort delete failed.
	EventCleanupFailed
	// EventOperationFailed means an operation returned an error to its caller.
	EventOperationFailed
	// EventLoaderPanic means the loader panicked or exited without returning.
	// Event.Err is a [*PanicError] for a panic.
	EventLoaderPanic
)

func (t EventType) String() string {
	switch t {
	case EventEntryRejected:
		return "entry_rejected"
	case EventStoreRejected:
		return "store_rejected"
	case EventCleanupFailed:
		return "cleanup_failed"
	case EventOperationFailed:
		return "operation_failed"
	case EventLoaderPanic:
		return "loader_panic"
	default:
		return "unknown"
	}
}

// RejectReason explains why a stored entry failed validation.
type RejectReason uint8

const (
	// RejectUnknown is the zero value and is never reported.
	RejectUnknown RejectReason = iota
	// RejectFrameCorrupt means the bytes are damaged or were not written by
	// cascache. The entry is removed.
	RejectFrameCorrupt
	// RejectUnsupportedFormat means another cascache version or kind wrote the
	// frame. The entry is left in place.
	RejectUnsupportedFormat
	// RejectStateMissing means the key's invalidation state is gone, so the
	// entry cannot be proved current. The entry is removed.
	RejectStateMissing
	// RejectRetired means an invalidation retired the entry. It is removed.
	RejectRetired
	// RejectValueDecode means the codec rejected the payload. The entry is
	// removed.
	RejectValueDecode
)

func (r RejectReason) String() string {
	switch r {
	case RejectFrameCorrupt:
		return "frame_corrupt"
	case RejectUnsupportedFormat:
		return "unsupported_format"
	case RejectStateMissing:
		return "state_missing"
	case RejectRetired:
		return "retired"
	case RejectValueDecode:
		return "value_decode"
	default:
		return "unknown"
	}
}

// Event describes something worth observing. Use keyed fields when building one
// because later releases may add data.
type Event struct {
	Type EventType
	Op   Op

	// Key is the caller's key, not a storage key.
	Key string

	// Reason is set for [EventEntryRejected].
	Reason RejectReason

	// Err is set when the event reports a failure.
	Err error
}

// Observer receives cache events on the calling goroutine. Implementations must
// be safe for concurrent use and must not block. The hooks/async package can
// move observation off the operation path.
//
// New event types and reasons are added over time, so an Observer should ignore
// values it does not recognize rather than treat them as errors.
type Observer interface {
	Observe(Event)
}

// ObserverFunc adapts a function to [Observer].
type ObserverFunc func(Event)

func (f ObserverFunc) Observe(e Event) { f(e) }

// MultiObserver calls each observer in order, skipping nil ones.
func MultiObserver(observers ...Observer) Observer {
	kept := make([]Observer, 0, len(observers))
	for _, o := range observers {
		if !isNil(o) {
			kept = append(kept, o)
		}
	}
	switch len(kept) {
	case 0:
		return nil
	case 1:
		return kept[0]
	}
	return multiObserver(kept)
}

type multiObserver []Observer

func (m multiObserver) Observe(e Event) {
	for _, o := range m {
		o.Observe(e)
	}
}
