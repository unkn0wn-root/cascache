package cascache

import "github.com/unkn0wn-root/cascache/genstore"

// Hooks are lightweight callbacks for high-signal events.
// Implementations MUST be cheap and non-blocking; do not perform I/O.
// If work may block, buffer it and drop on backpressure (best effort).
//
// Key-bearing callbacks intentionally expose different key kinds:
//   - SelfHealSingle / ProviderSetRejected: provider storage keys.
//   - GenBumpError: canonical genstore.CacheKey identity.
type Hooks interface {
	SelfHealSingle(storageKey string, reason SelfHealReason)
	BatchRejected(namespace string, requested int, reason BatchRejectReason)
	ProviderSetRejected(storageKey string, isBatch bool)
	GenSnapshotError(count int, err error)
	GenBumpError(cacheKey genstore.CacheKey, err error)
	InvalidateOutage(key string, bumpErr, delErr error)
	LocalGenWithBatch()
}

// NopHooks is a default no-op.
type NopHooks struct{}

func (NopHooks) SelfHealSingle(string, SelfHealReason)        {}
func (NopHooks) BatchRejected(string, int, BatchRejectReason) {}
func (NopHooks) ProviderSetRejected(string, bool)             {}
func (NopHooks) GenSnapshotError(int, error)                  {}
func (NopHooks) GenBumpError(genstore.CacheKey, error)        {}
func (NopHooks) InvalidateOutage(string, error, error)        {}
func (NopHooks) LocalGenWithBatch()                           {}

// Multi returns a Hooks implementation that fans out to all provided hooks
// in order. Nil entries are silently skipped. Panics from any hook propagate
// to the caller.
//
// Example usage:
//
// logH   := sloghook.New(slog.Default(), sloghook.Options{SelfHealEvery: 10})
// metH   := promhook.New(...)            // some kind of metrics adapter
// auditH := myAuditHook{...}             // audit adapter
//
// fan-out
// mh := cascache.MultiHooks{logH, metH, auditH}
//
// Either: single async queue for the whole fan-out
// hooks := asynchook.New(mh, 1, 1000)
//
// Or: give each hook its own queue (isolate backpressure)
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

func (m multiHooks) SelfHealSingle(k string, r SelfHealReason) {
	for _, h := range m {
		h.SelfHealSingle(k, r)
	}
}

func (m multiHooks) BatchRejected(ns string, n int, r BatchRejectReason) {
	for _, h := range m {
		h.BatchRejected(ns, n, r)
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

func (m multiHooks) GenBumpError(k genstore.CacheKey, err error) {
	for _, h := range m {
		h.GenBumpError(k, err)
	}
}

func (m multiHooks) InvalidateOutage(k string, be, de error) {
	for _, h := range m {
		h.InvalidateOutage(k, be, de)
	}
}

func (m multiHooks) LocalGenWithBatch() {
	for _, h := range m {
		h.LocalGenWithBatch()
	}
}
