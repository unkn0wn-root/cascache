package genstore

import (
	"context"
	"sync"
	"time"
)

type localGenEntry struct {
	Gen       uint64
	UpdatedAt time.Time
}

// LocalGenStore keeps generations in-process (the default).
// It includes an optional cleanup loop to prune long-inactive entries.
type LocalGenStore struct {
	mu     sync.RWMutex
	gens   map[string]localGenEntry
	ticker *time.Ticker
	stopCh chan struct{}
	wg     sync.WaitGroup

	retention time.Duration
}

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

func (s *LocalGenStore) Snapshot(_ context.Context, k string) (uint64, error) {
	s.mu.RLock()
	e, ok := s.gens[k]
	s.mu.RUnlock()
	if !ok {
		return 0, nil
	}
	return e.Gen, nil
}

func (s *LocalGenStore) SnapshotMany(ctx context.Context, ks []string) (map[string]uint64, error) {
	out := make(map[string]uint64, len(ks))
	for _, k := range ks {
		g, _ := s.Snapshot(ctx, k)
		out[k] = g
	}
	return out, nil
}

func (s *LocalGenStore) Bump(_ context.Context, k string) (uint64, error) {
	s.mu.Lock()
	e := s.gens[k]
	e.Gen++
	e.UpdatedAt = time.Now()
	s.gens[k] = e
	s.mu.Unlock()
	return e.Gen, nil
}

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

func (s *LocalGenStore) Close(context.Context) error {
	if s.stopCh != nil {
		close(s.stopCh)
		s.wg.Wait()
		if s.ticker != nil {
			s.ticker.Stop()
		}
	}
	return nil
}
