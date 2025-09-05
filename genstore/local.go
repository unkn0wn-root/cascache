package genstore

import (
	"context"
	"sync"
	"time"
)

// localGenEntry holds the per-key generation and the time of the last bump.
//
// UpdatedAt is set only on bumps (writes). Reads do NOT modify timestamps,
// which avoids write amplification on hot read paths. Cleanup relies on
// UpdatedAt to prune long-inactive keys.
type localGenEntry struct {
	Gen       uint64
	UpdatedAt time.Time
}

// LocalGenStore keeps generations in-process (no network I/O).
// Optionally starts a background cleanup goroutine that periodically prunes
// keys whose generation hasn't been bumped for at least `retention` duration.
//
//   - Reads take a shared RLock (Snapshot, SnapshotMany).
//   - Bumps take an exclusive Lock and are O(1).
//   - Cleanup takes an exclusive Lock and scans the map.
//
// Ctx parameters are accepted to satisfy the GenStore interface, but are
// ignored because all operations are local and non-blocking.
type LocalGenStore struct {
	mu     sync.RWMutex
	gens   map[string]localGenEntry
	ticker *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup

	// retention is the minimum age since the last bump after which a key may be
	// pruned by Cleanup. A non-positive retention disables pruning.
	retention time.Duration
}

var _ GenStore = (*LocalGenStore)(nil)

// NewLocalGenStore constructs a LocalGenStore.
//
// If both cleanupInterval > 0 and retention > 0, a background goroutine is
// started that calls Cleanup(retention) every cleanupInterval. If either is
// non-positive, no background cleanup runs and you may call Cleanup manually.
func NewLocalGenStore(cleanupInterval, retention time.Duration) *LocalGenStore {
	s := &LocalGenStore{
		gens:      make(map[string]localGenEntry),
		retention: retention,
	}
	if cleanupInterval > 0 && retention > 0 {
		s.ticker = time.NewTicker(cleanupInterval)
		s.stopCh = make(chan struct{})
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				select {
				case <-s.ticker.C:
					s.Cleanup(retention)
				case <-s.stopCh:
					return
				}
			}
		}()
	}
	return s
}

// Snapshot returns the current generation for key k.
//
// Returns 0 if the key has never been bumped (i.e., missing in the map).
// This is safe for CAS semantics: a 0 snapshot simply causes stale writes to
// be skipped and stale reads to self-heal in the higher-level cache.
func (s *LocalGenStore) Snapshot(_ context.Context, k string) (uint64, error) {
	s.mu.RLock()
	e, ok := s.gens[k]
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	return e.Gen, nil
}

// SnapshotMany returns the current generations for all requested keys.
//
// acquires the read lock once for the entire batch to avoid per-key
// lock/unlock overhead. Missing keys map to 0 (zero value).
func (s *LocalGenStore) SnapshotMany(_ context.Context, ks []string) (map[string]uint64, error) {
	out := make(map[string]uint64, len(ks))
	s.mu.RLock()
	for _, k := range ks {
		out[k] = s.gens[k].Gen // zero value (0) if missing
	}
	s.mu.RUnlock()
	return out, nil
}

// Bump atomically increments the generation for key k and updates UpdatedAt.
// If the key does not exist, it is created with Gen=1.
//
// Returns the new generation value.
func (s *LocalGenStore) Bump(_ context.Context, k string) (uint64, error) {
	now := time.Now()
	s.mu.Lock()
	e := s.gens[k] // zero value if missing
	e.Gen++
	e.UpdatedAt = now
	s.gens[k] = e
	s.mu.Unlock()
	return e.Gen, nil
}

// Cleanup removes keys whose UpdatedAt is older than retention ago.
//
// This bounds memory usage of the in-process map for long-inactive keys.
// Reads remain correct if a key is cleaned up: Snapshot will report 0 until
// the next bump recreates the entry.
func (s *LocalGenStore) Cleanup(retention time.Duration) {
	if retention <= 0 {
		return
	}
	cutoff := time.Now().Add(-retention)

	s.mu.Lock()
	for k, e := range s.gens {
		if !e.UpdatedAt.IsZero() && e.UpdatedAt.Before(cutoff) {
			delete(s.gens, k)
		}
	}
	s.mu.Unlock()
}

// Close stops the optional cleanup goroutine and releases the ticker.
// Safe to call Close multiple times; subsequent calls are no-ops.
func (s *LocalGenStore) Close(_ context.Context) error {
	s.mu.Lock()
	stopCh := s.stopCh
	ticker := s.ticker
	s.stopCh, s.ticker = nil, nil
	s.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		if ticker != nil {
			ticker.Stop()
		}
		s.wg.Wait()
	}
	return nil
}
