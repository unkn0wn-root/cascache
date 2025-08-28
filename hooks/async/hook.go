// usage:
//
// import (
//
//	"log/slog"
//
//	"github.com/unkn0wn-root/cascache"
//	"github.com/unkn0wn-root/cascache/codec"
//	"github.com/unkn0wn-root/cascache/genstore"
//	"github.com/unkn0wn-root/cascache/hooks/async"
//	"github.com/unkn0wn-root/cascache/hooks/slog"
//
// )
//
//	raw := sloghook.New(slog.Default(), sloghook.Options{
//	    SelfHealEvery:   10, // sample logs: ~every 10th self-heal
//	    BulkRejectEvery: 1,  // log every bulk rejection
//	})
//
// hooks := asynchook.New(raw, 1, 1000) // 1 worker; queue 1000 events
// defer hooks.Close()
//
//	cache, _ := cascache.New[User](cascache.Options[User]{
//	    Namespace: "app:prod:user",
//	    Provider:  provider,
//	    Codec:     codec.JSON[User]{},
//	    GenStore:  genstore.NewRedisGenStoreWithTTL(rdb, "app:prod:user", 24*time.Hour),
//	    Hooks:     hooks, // or `raw` if you don’t want async
//	})
package asynchook

import (
	"sync"

	"github.com/unkn0wn-root/cascache"
)

type Hooks struct {
	inner cascache.Hooks
	q     chan func()
	wg    sync.WaitGroup
	once  sync.Once
}

var _ cascache.Hooks = (*Hooks)(nil)

func New(inner cascache.Hooks, workers, qlen int) *Hooks {
	if workers <= 0 {
		workers = 1
	}
	if qlen <= 0 {
		qlen = 1024
	}

	h := &Hooks{inner: inner, q: make(chan func(), qlen)}
	h.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer h.wg.Done()
			for f := range h.q {
				f()
			}
		}()
	}
	return h
}

func (h *Hooks) Close() {
	h.once.Do(func() {
		close(h.q)
		h.wg.Wait()
	})
}

func (h *Hooks) try(f func()) {
	select {
	case h.q <- f:
	default: // drop
	}
}

func (h *Hooks) SelfHealSingle(k, r string)       { h.try(func() { h.inner.SelfHealSingle(k, r) }) }
func (h *Hooks) GenBumpError(k string, err error) { h.try(func() { h.inner.GenBumpError(k, err) }) }
func (h *Hooks) LocalGenWithBulk()                { h.try(func() { h.inner.LocalGenWithBulk() }) }
func (h *Hooks) BulkRejected(ns string, n int, r string) {
	h.try(func() { h.inner.BulkRejected(ns, n, r) })
}
func (h *Hooks) ProviderSetRejected(k string, b bool) {
	h.try(func() { h.inner.ProviderSetRejected(k, b) })
}
func (h *Hooks) GenSnapshotError(n int, err error) {
	h.try(func() { h.inner.GenSnapshotError(n, err) })
}
func (h *Hooks) InvalidateOutage(k string, be, de error) {
	h.try(func() { h.inner.InvalidateOutage(k, be, de) })
}
