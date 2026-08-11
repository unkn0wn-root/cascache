// Package asynchook runs cascache observers outside cache operations. Its queue
// is bounded and drops events instead of blocking producers.
package asynchook

import (
	"sync"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/internal/typednil"
)

const (
	DefaultWorkers   = 1
	DefaultQueueSize = 1024
)

// Keep the observer with its queued event.
type queued struct {
	observer cascache.Observer
	event    cascache.Event
}

// Queue runs wrapped observers on background workers.
type Queue struct {
	events chan queued
	stop   chan struct{}
	wg     sync.WaitGroup
	once   sync.Once

	// Keep events open because submit may race with Close.
	mu     sync.Mutex
	closed bool
}

// New starts a Queue. Non-positive values use the defaults.
func New(workers, queueSize int) *Queue {
	if workers <= 0 {
		workers = DefaultWorkers
	}
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}

	q := &Queue{
		events: make(chan queued, queueSize),
		stop:   make(chan struct{}),
	}
	for range workers {
		q.wg.Add(1)
		go q.work()
	}
	return q
}

// Wrap returns an observer that delivers to inner on this queue's workers.
// Observers wrapped by the same Queue share its capacity and its workers.
func (q *Queue) Wrap(inner cascache.Observer) cascache.Observer {
	if typednil.Is(inner) {
		return nil
	}
	return cascache.ObserverFunc(func(e cascache.Event) {
		q.submit(queued{observer: inner, event: e})
	})
}

// Close drains queued events and stops the workers. It is safe to call more
// than once.
func (q *Queue) Close() {
	q.once.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()

		close(q.stop)
		q.wg.Wait()
	})
}

func (q *Queue) submit(item queued) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	select {
	case q.events <- item:
	default:
		// Drop rather than block the cache operation.
	}
}

func (q *Queue) work() {
	defer q.wg.Done()
	for {
		select {
		case item := <-q.events:
			item.observer.Observe(item.event)
		case <-q.stop:
			q.drain()
			return
		}
	}
}

func (q *Queue) drain() {
	for {
		select {
		case item := <-q.events:
			item.observer.Observe(item.event)
		default:
			return
		}
	}
}
