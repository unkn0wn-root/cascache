package backend

import (
	"cmp"
	"context"
	"fmt"
	"hash/maphash"
	"sync"
	"time"
)

// Defaults for [LocalOptions].
const (
	// DefaultInvalidationTTL is how long invalidation state lives by default.
	DefaultInvalidationTTL = 24 * time.Hour
	// DefaultCleanupInterval is how often expired invalidation state is swept.
	DefaultCleanupInterval = time.Minute
)

const memoryShards = 256

// LocalOptions configures a [Local] backend.
type LocalOptions struct {
	// InvalidationTTL is how long a key's invalidation state lives after its
	// last write. Zero uses [DefaultInvalidationTTL]; [NoExpiration] keeps it
	// forever. Value TTLs are limited to this duration.
	InvalidationTTL time.Duration

	// CleanupInterval is how often expired invalidation state is swept. Zero
	// uses [DefaultCleanupInterval]. A negative value runs no background
	// goroutine, but expired state is still treated as absent.
	CleanupInterval time.Duration
}

type memoryEntry struct {
	fence     Fence
	writtenAt time.Time
}

type memoryShard struct {
	mu      sync.RWMutex
	entries map[string]memoryEntry
}

// memoryFenceStore keeps fences in this process.
type memoryFenceStore struct {
	seed      maphash.Seed
	retention time.Duration
	shards    [memoryShards]memoryShard
	// now is replaced in tests so expiry can be exercised without waiting.
	now       func() time.Time
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

var _ FenceStore = (*memoryFenceStore)(nil)

func newMemoryFenceStore(opts LocalOptions) (*memoryFenceStore, error) {
	if opts.InvalidationTTL < 0 && opts.InvalidationTTL != NoExpiration {
		return nil, fmt.Errorf("cascache/backend: invalid invalidation TTL %v", opts.InvalidationTTL)
	}

	s := &memoryFenceStore{
		seed: maphash.MakeSeed(),
		now:  time.Now,
	}
	if opts.InvalidationTTL != NoExpiration {
		s.retention = cmp.Or(opts.InvalidationTTL, DefaultInvalidationTTL)
	}
	for i := range s.shards {
		s.shards[i].entries = make(map[string]memoryEntry)
	}

	if s.retention > 0 && opts.CleanupInterval >= 0 {
		interval := cmp.Or(opts.CleanupInterval, DefaultCleanupInterval)
		s.stop = make(chan struct{})
		s.wg.Add(1)
		go s.sweep(interval)
	}
	return s, nil
}

func (s *memoryFenceStore) sweep(interval time.Duration) {
	defer s.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.cleanup(s.now())
		case <-s.stop:
			return
		}
	}
}

func (s *memoryFenceStore) shard(key Key) *memoryShard {
	return &s.shards[maphash.String(s.seed, key.ID())%memoryShards]
}

// Check whether e is still current at now.
func (s *memoryFenceStore) live(e memoryEntry, now time.Time) bool {
	return s.retention <= 0 || now.Sub(e.writtenAt) < s.retention
}

func (s *memoryFenceStore) Lifetime() time.Duration { return s.retention }

func (s *memoryFenceStore) Ensure(_ context.Context, key Key, candidate Fence) (Fence, error) {
	if err := CheckKeyFence(key, candidate); err != nil {
		return Fence{}, err
	}
	id, sh := key.ID(), s.shard(key)
	now := s.now()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	// Do not revive an expired fence; install the new candidate.
	if e, ok := sh.entries[id]; ok && s.live(e, now) {
		e.writtenAt = now
		sh.entries[id] = e
		return e.fence, nil
	}
	sh.entries[id] = memoryEntry{fence: candidate, writtenAt: now}
	return candidate, nil
}

func (s *memoryFenceStore) Read(_ context.Context, key Key) (Fence, bool, error) {
	if err := CheckKey(key); err != nil {
		return Fence{}, false, err
	}
	sh := s.shard(key)
	now := s.now()

	sh.mu.RLock()
	e, ok := sh.entries[key.ID()]
	sh.mu.RUnlock()

	if !ok || !s.live(e, now) {
		return Fence{}, false, nil
	}
	return e.fence, true, nil
}

func (s *memoryFenceStore) Retain(_ context.Context, key Key, expected Fence) (bool, error) {
	if err := CheckKeyFence(key, expected); err != nil {
		return false, err
	}
	id, sh := key.ID(), s.shard(key)
	now := s.now()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.entries[id]
	if !ok || !s.live(e, now) || !e.fence.Equal(expected) {
		return false, nil
	}
	e.writtenAt = now
	sh.entries[id] = e
	return true, nil
}

func (s *memoryFenceStore) Replace(_ context.Context, key Key, next Fence) error {
	if err := CheckKeyFence(key, next); err != nil {
		return err
	}
	sh := s.shard(key)
	now := s.now()

	sh.mu.Lock()
	sh.entries[key.ID()] = memoryEntry{fence: next, writtenAt: now}
	sh.mu.Unlock()
	return nil
}

// Cleanup removes fences past their retention. It does nothing when fences do
// not expire.
func (s *memoryFenceStore) cleanupExpired() {
	if s.retention > 0 {
		s.cleanup(s.now())
	}
}

func (s *memoryFenceStore) cleanup(now time.Time) {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for id, e := range sh.entries {
			if !s.live(e, now) {
				delete(sh.entries, id)
			}
		}
		sh.mu.Unlock()
	}
}

// Len reports how many fences are held. It is intended for tests and metrics.
func (s *memoryFenceStore) len() int {
	var n int
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.entries)
		sh.mu.RUnlock()
	}
	return n
}

// Close stops the background sweep. It is safe to call more than once.
func (s *memoryFenceStore) close() error {
	if s == nil || s.stop == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stop)
		s.wg.Wait()
	})
	return nil
}
