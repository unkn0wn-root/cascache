package memstore

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/unkn0wn-root/cascache/v4/provider"
)

// Hook intercepts operations. A nil field leaves the operation alone.
type Hook struct {
	Get func(key string) error
	Set func(key string, value []byte) (ok bool, err error)
	Del func(key string) error
}

// Options configures a [Store].
type Options struct {
	Now  func() time.Time
	Hook Hook
}

type entry struct {
	value     []byte
	expiresAt time.Time // zero means no expiry
}

// Store is a map-backed value store.
type Store struct {
	mu      sync.Mutex
	entries map[string]entry
	now     func() time.Time
	hook    Hook
}

var _ provider.Store = (*Store)(nil)

// New returns an empty store.
func New(opts Options) *Store {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Store{entries: make(map[string]entry), now: now, hook: opts.Hook}
}

func (s *Store) Get(_ context.Context, key string) ([]byte, bool, error) {
	if s.hook.Get != nil {
		if err := s.hook.Get(key); err != nil {
			return nil, false, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok {
		return nil, false, nil
	}
	if !e.expiresAt.IsZero() && !s.now().Before(e.expiresAt) {
		delete(s.entries, key)
		return nil, false, nil
	}
	return bytes.Clone(e.value), true, nil
}

func (s *Store) Set(_ context.Context, key string, value []byte, _ int64, ttl time.Duration) (bool, error) {
	if s.hook.Set != nil {
		return s.hook.Set(key, value)
	}

	e := entry{value: bytes.Clone(value)}
	if ttl > 0 {
		e.expiresAt = s.now().Add(ttl)
	}

	s.mu.Lock()
	s.entries[key] = e
	s.mu.Unlock()
	return true, nil
}

func (s *Store) Del(_ context.Context, key string) error {
	if s.hook.Del != nil {
		if err := s.hook.Del(key); err != nil {
			return err
		}
	}

	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
	return nil
}

// Len includes expired entries that have not been read.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// Keys returns the stored keys in no particular order.
func (s *Store) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.entries))
	for k := range s.entries {
		keys = append(keys, k)
	}
	return keys
}
