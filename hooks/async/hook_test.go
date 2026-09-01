package asynchook_test

import (
	"sync"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	asynchook "github.com/unkn0wn-root/cascache/v4/hooks/async"
)

type recorder struct {
	mu     sync.Mutex
	events []cascache.Event
	block  chan struct{}
}

func (r *recorder) Observe(e cascache.Event) {
	if r.block != nil {
		<-r.block
	}
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) all() []cascache.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]cascache.Event(nil), r.events...)
}

func TestWrapDeliversInTheBackground(t *testing.T) {
	rec := &recorder{}
	q := asynchook.New(1, 16)
	obs := q.Wrap(rec)

	obs.Observe(cascache.Event{Type: cascache.EventEntryRejected, Key: "a"})
	obs.Observe(cascache.Event{Type: cascache.EventStoreRejected, Key: "b"})

	q.Close()

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2: %+v", len(got), got)
	}
	if got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("events arrived as %+v, want a then b", got)
	}
}

func TestWrapOfNilIsNil(t *testing.T) {
	q := asynchook.New(1, 1)
	defer q.Close()

	if obs := q.Wrap(nil); obs != nil {
		t.Fatalf("Wrap(nil) = %v, want nil", obs)
	}

	var typedNil *recorder
	if obs := q.Wrap(typedNil); obs != nil {
		t.Fatalf("Wrap(typed nil) = %v, want nil", obs)
	}
}

func TestSubmittingToAFullQueueDropsRatherThanBlocks(t *testing.T) {
	rec := &recorder{block: make(chan struct{})}
	q := asynchook.New(1, 1)
	obs := q.Wrap(rec)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			obs.Observe(cascache.Event{Type: cascache.EventEntryRejected})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Observe blocked on a full queue")
	}

	close(rec.block)
	q.Close()

	if got := len(rec.all()); got == 0 || got >= 100 {
		t.Fatalf("delivered %d of 100 events, want some but not all", got)
	}
}

func TestSubmitAfterCloseIsDropped(t *testing.T) {
	rec := &recorder{}
	q := asynchook.New(1, 16)
	obs := q.Wrap(rec)

	q.Close()
	obs.Observe(cascache.Event{Type: cascache.EventEntryRejected})

	if got := rec.all(); len(got) != 0 {
		t.Fatalf("delivered %+v after Close, want nothing", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	q := asynchook.New(0, 0)
	for range 3 {
		q.Close()
	}
}

func TestObserversShareTheQueue(t *testing.T) {
	first, second := &recorder{}, &recorder{}
	q := asynchook.New(2, 16)

	q.Wrap(first).Observe(cascache.Event{Type: cascache.EventEntryRejected, Key: "first"})
	q.Wrap(second).Observe(cascache.Event{Type: cascache.EventEntryRejected, Key: "second"})
	q.Close()

	if got := first.all(); len(got) != 1 || got[0].Key != "first" {
		t.Fatalf("first observer got %+v", got)
	}
	if got := second.all(); len(got) != 1 || got[0].Key != "second" {
		t.Fatalf("second observer got %+v", got)
	}
}
