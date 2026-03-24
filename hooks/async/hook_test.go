package asynchook

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache"
)

type blockingHook struct {
	cascache.NopHooks
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
	calls       atomic.Int32
}

func (h *blockingHook) SelfHealSingle(string, cascache.SelfHealReason) {
	if h.started != nil {
		h.startedOnce.Do(func() { close(h.started) })
	}
	if h.release != nil {
		<-h.release
	}
	h.calls.Add(1)
}

func TestHooksCloseDropsLateEvents(t *testing.T) {
	t.Parallel()

	h := New(nil, 1, 1)
	h.Close()

	h.SelfHealSingle("k", cascache.SelfHealReasonCorrupt)
	h.BatchRejected("ns", 1, cascache.BatchRejectReasonDecodeError)
}

func TestHooksCloseWaitsForInFlightEvent(t *testing.T) {
	t.Parallel()

	inner := &blockingHook{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	h := New(inner, 1, 1)

	h.SelfHealSingle("k", cascache.SelfHealReasonCorrupt)
	<-inner.started

	closed := make(chan struct{})
	go func() {
		h.Close()
		close(closed)
	}()

	select {
	case <-closed:
		t.Fatal("Close returned before in-flight event completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(inner.release)

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after releasing in-flight event")
	}

	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("call count mismatch: got %d want 1", got)
	}
}
