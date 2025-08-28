package cascache

// Hooks are lightweight callbacks for high-signal events.
// Implementations MUST be cheap and non-blocking; do not perform I/O.
// If work may block, buffer it and drop on backpressure (best effort).
type Hooks interface {
	SelfHealSingle(storageKey, reason string)
	BulkRejected(namespace string, requested int, reason string)
	ProviderSetRejected(storageKey string, isBulk bool)
	GenSnapshotError(count int, err error)
	GenBumpError(storageKey string, err error)
	InvalidateOutage(key string, bumpErr, delErr error)
	LocalGenWithBulk()
}

// NopHooks is a default no-op.
type NopHooks struct{}

func (NopHooks) SelfHealSingle(string, string)         {}
func (NopHooks) BulkRejected(string, int, string)      {}
func (NopHooks) ProviderSetRejected(string, bool)      {}
func (NopHooks) GenSnapshotError(int, error)           {}
func (NopHooks) GenBumpError(string, error)            {}
func (NopHooks) InvalidateOutage(string, error, error) {}
func (NopHooks) LocalGenWithBulk()                     {}

// Multi returns a Hooks that fan-outs to all provided hooks, in order.
// Nil entries are ignored.
// Panics from a hook will propagate to the caller.
//
// example usage:
//
// logH   := sloghook.New(slog.Default(), sloghook.Options{SelfHealEvery: 10})
// metH   := promhook.New(...)            // some kind og metrics adapter
// auditH := myAuditHook{...}             // audit adapter
//
// // fan-out
// mh := cascache.MultiHooks{logH, metH, auditH}
//
// // Either: single async queue for the whole fan-out
// hooks := asynchook.New(mh, 1, 1000)
//
// // Or: give each hook its own queue (isolate backpressure)
//
//	hooks := cascache.MultiHooks{
//	    asynchook.New(logH,   1, 1000),
//	    asynchook.New(metH,   1, 1000),
//	    asynchook.New(auditH, 1, 1000),
//	}
func Multi(hs ...Hooks) Hooks {
	nn := make([]Hooks, 0, len(hs))
	for _, h := range hs {
		if h != nil {
			nn = append(nn, h)
		}
	}
	return multiHooks(nn)
}

type multiHooks []Hooks

func (m multiHooks) SelfHealSingle(k, r string) {
	for _, h := range m {
		h.SelfHealSingle(k, r)
	}
}
func (m multiHooks) BulkRejected(ns string, n int, r string) {
	for _, h := range m {
		h.BulkRejected(ns, n, r)
	}
}
func (m multiHooks) ProviderSetRejected(k string, b bool) {
	for _, h := range m {
		h.ProviderSetRejected(k, b)
	}
}
func (m multiHooks) GenSnapshotError(n int, err error) {
	for _, h := range m {
		h.GenSnapshotError(n, err)
	}
}
func (m multiHooks) GenBumpError(k string, err error) {
	for _, h := range m {
		h.GenBumpError(k, err)
	}
}
func (m multiHooks) InvalidateOutage(k string, be, de error) {
	for _, h := range m {
		h.InvalidateOutage(k, be, de)
	}
}
func (m multiHooks) LocalGenWithBulk() {
	for _, h := range m {
		h.LocalGenWithBulk()
	}
}
