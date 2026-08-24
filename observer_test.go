package cascache_test

import (
	"testing"

	"github.com/unkn0wn-root/cascache/v4"
)

func TestMultiObserver(t *testing.T) {
	var order []string
	first := cascache.ObserverFunc(func(cascache.Event) { order = append(order, "first") })
	second := cascache.ObserverFunc(func(cascache.Event) { order = append(order, "second") })

	var typedNil *eventLog

	obs := cascache.MultiObserver(first, nil, typedNil, second)
	if obs == nil {
		t.Fatal("MultiObserver dropped every observer")
	}
	obs.Observe(cascache.Event{Type: cascache.EventEntryRejected})

	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("observers ran as %v, want first then second", order)
	}
}

func TestMultiObserverWithNothingToCallIsNil(t *testing.T) {
	var typedNil *eventLog

	if obs := cascache.MultiObserver(); obs != nil {
		t.Fatalf("MultiObserver() = %v, want nil", obs)
	}
	if obs := cascache.MultiObserver(nil, typedNil); obs != nil {
		t.Fatalf("MultiObserver(nils) = %v, want nil", obs)
	}
}

func TestMultiObserverWithOneReturnsIt(t *testing.T) {
	only := &eventLog{}
	if got := cascache.MultiObserver(nil, only); got != cascache.Observer(only) {
		t.Fatalf("MultiObserver with one observer wrapped it: %T", got)
	}
}

func TestTypedNilObserverIsSafe(t *testing.T) {
	var typedNil *eventLog

	h := newHarness(t, func(o *cascache.Options[user]) { o.Observer = typedNil })
	h.fill(t, "42", ada)

	h.fences.forget(h.space("42"))
	if _, ok := h.mustGet(t, "42"); ok {
		t.Fatal("a value with no fence was served")
	}
}
