package cascache_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/provider"
)

type memoryStore struct {
	mu      sync.Mutex
	entries map[string][]byte
}

var _ provider.Store = (*memoryStore)(nil)

func newMemoryStore() *memoryStore {
	return &memoryStore{entries: make(map[string][]byte)}
}

func (s *memoryStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.entries[key]
	return v, ok, nil
}

func (s *memoryStore) Set(_ context.Context, key string, value []byte, _ int64, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = value
	return true, nil
}

func (s *memoryStore) Del(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

type person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newExampleCache() (*cascache.Cache[person], *backend.Local) {
	b, err := backend.NewLocal(newMemoryStore(), backend.LocalOptions{})
	if err != nil {
		panic(err)
	}
	cache, err := cascache.New(cascache.Options[person]{
		Namespace:  "person",
		Backend:    b,
		Codec:      codec.JSON[person]{},
		DefaultTTL: 5 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	return cache, b
}

func Example() {
	ctx := context.Background()
	cache, b := newExampleCache()
	defer b.Close()

	reads := 0
	load := func(context.Context) (person, error) {
		reads++
		return person{ID: "42", Name: "Ada"}, nil
	}

	// Only the first load reads from the source.
	for range 3 {
		p, err := cache.Load(ctx, "42", load)
		if err != nil {
			panic(err)
		}
		fmt.Println(p.Name)
	}
	fmt.Println("source reads:", reads)

	// Output:
	// Ada
	// Ada
	// Ada
	// source reads: 1
}

func ExampleCache_Invalidate() {
	ctx := context.Background()
	cache, b := newExampleCache()
	defer b.Close()

	names := []string{"Ada", "Grace"}
	load := func(context.Context) (person, error) {
		name := names[0]
		names = names[1:]
		return person{ID: "42", Name: name}, nil
	}

	first, _ := cache.Load(ctx, "42", load)
	fmt.Println(first.Name)

	if err := cache.Invalidate(ctx, "42"); err != nil {
		panic(err)
	}

	second, _ := cache.Load(ctx, "42", load)
	fmt.Println(second.Name)

	// Output:
	// Ada
	// Grace
}
