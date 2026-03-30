package version

import (
	"context"
	"sync"
	"time"
)

// localEntry holds the per-key authoritative fence and the time of the last
// state mutation.
//
// UpdatedAt is set on init and bump operations when timestamp tracking is
// enabled. Reads do NOT modify timestamps, which avoids write amplification on
// hot read paths. Cleanup relies on UpdatedAt to prune long-inactive keys.
type localEntry struct {
	Fence     Fence
	UpdatedAt time.Time
}

// LocalStore keeps authoritative fence state in-process (no network I/O).
// Optionally starts a background cleanup goroutine that periodically prunes
// keys whose authoritative state hasn't changed for at least `retention` duration.
//
//   - Reads take a shared RLock (Snapshot, SnapshotMany).
//   - Advances take an exclusive Lock and are O(1).
//   - Cleanup takes an exclusive Lock and scans the map.
//
// Ctx parameters are accepted to satisfy the Store interface, but are
// ignored because all operations are local and non-blocking.
type LocalStore struct {
	mu             sync.RWMutex
	entries        map[string]localEntry
	stopCh         chan struct{}
	wg             sync.WaitGroup
	closeOnce      sync.Once
	trackUpdatedAt bool

	// retention is the minimum age since the last advance after which a key may be
	// pruned by Cleanup. A non-positive retention disables pruning.
	retention time.Duration
}

var _ Store = (*LocalStore)(nil)

// NewLocal constructs the in-process authoritative fence store used by the cache
// by default. It does not start background cleanup and skips timestamp tracking
// because strict mode never prunes metadata.
func NewLocal() *LocalStore {
	return &LocalStore{
		entries: make(map[string]localEntry),
	}
}

// NewLocalWithCleanup constructs a LocalStore with optional background
// cleanup.
//
// If both cleanupInterval > 0 and retention > 0, a background goroutine is
// started that calls Cleanup(retention) every cleanupInterval. If either is
// non-positive, no background cleanup runs and you may call Cleanup manually.
func NewLocalWithCleanup(cleanupInterval, retention time.Duration) *LocalStore {
	s := &LocalStore{
		entries:        make(map[string]localEntry),
		retention:      retention,
		trackUpdatedAt: true,
	}

	if cleanupInterval > 0 && retention > 0 {
		s.stopCh = make(chan struct{})
		s.wg.Add(1)
		go func(stop <-chan struct{}) {
			defer s.wg.Done()
			t := time.NewTicker(cleanupInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					s.Cleanup(retention)
				case <-stop:
					return
				}
			}
		}(s.stopCh)
	}
	return s
}

// Snapshot returns the current authoritative state for key k.
//
// Missing keys are reported as Exists=false.
func (s *LocalStore) Snapshot(_ context.Context, k CacheKey) (Snapshot, error) {
	raw := k.String()
	s.mu.RLock()
	e, ok := s.entries[raw]
	s.mu.RUnlock()
	if !ok {
		return Snapshot{}, nil
	}
	return Snapshot{
		Fence:  e.Fence,
		Exists: true,
	}, nil
}

// SnapshotMany returns the current authoritative state for all requested keys.
func (s *LocalStore) SnapshotMany(_ context.Context, ks []CacheKey) (map[CacheKey]Snapshot, error) {
	out := make(map[CacheKey]Snapshot, len(ks))
	s.mu.RLock()
	for _, k := range ks {
		e, ok := s.entries[k.String()]
		if ok {
			out[k] = Snapshot{
				Fence:  e.Fence,
				Exists: true,
			}
			continue
		}
		out[k] = Snapshot{}
	}
	s.mu.RUnlock()
	return out, nil
}

// CreateIfMissing creates authoritative state with a fresh fence when the
// key is currently missing.
func (s *LocalStore) CreateIfMissing(_ context.Context, k CacheKey) (Snapshot, bool, error) {
	raw := k.String()

	var updatedAt time.Time
	if s.trackUpdatedAt {
		updatedAt = time.Now()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[raw]; ok {
		return Snapshot{
			Fence:  e.Fence,
			Exists: true,
		}, false, nil
	}

	f, err := NewFence()
	if err != nil {
		return Snapshot{}, false, err
	}

	e := localEntry{
		Fence:     f,
		UpdatedAt: updatedAt,
	}
	s.entries[raw] = e
	return Snapshot{
		Fence:  e.Fence,
		Exists: true,
	}, true, nil
}

// Advance moves the authoritative state for key k to a fresh fence and updates UpdatedAt.
// Missing keys are created with a fresh fence.
func (s *LocalStore) Advance(_ context.Context, k CacheKey) (Snapshot, error) {
	raw := k.String()
	f, err := NewFence()
	if err != nil {
		return Snapshot{}, err
	}

	var updatedAt time.Time
	if s.trackUpdatedAt {
		updatedAt = time.Now()
	}

	s.mu.Lock()
	e := s.entries[raw]
	e.Fence = f
	e.UpdatedAt = updatedAt
	s.entries[raw] = e
	s.mu.Unlock()
	return Snapshot{
		Fence:  e.Fence,
		Exists: true,
	}, nil
}

// Cleanup removes keys whose UpdatedAt is older than retention ago.
func (s *LocalStore) Cleanup(retention time.Duration) {
	if retention <= 0 {
		return
	}
	cutoff := time.Now().Add(-retention)

	s.mu.Lock()
	for k, e := range s.entries {
		if !e.UpdatedAt.IsZero() && e.UpdatedAt.Before(cutoff) {
			delete(s.entries, k)
		}
	}
	s.mu.Unlock()
}

// Close stops the optional cleanup goroutine and releases the ticker.
func (s *LocalStore) Close(_ context.Context) error {
	if s.stopCh == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		close(s.stopCh)
		s.wg.Wait()
	})
	return nil
}
