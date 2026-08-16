package ristretto_test

import (
	"context"
	"testing"

	"github.com/unkn0wn-root/cascache/v4/provider"
	"github.com/unkn0wn-root/cascache/v4/provider/providertest"
	"github.com/unkn0wn-root/cascache/v4/provider/ristretto"
)

func TestStoreConformance(t *testing.T) {
	providertest.TestStore(t, providertest.Config{
		New: func(t testing.TB) provider.Store {
			s, err := ristretto.New(ristretto.Config{
				NumCounters: 10_000,
				MaxCost:     8 << 20,
				BufferItems: 64,
			})
			if err != nil {
				t.Fatalf("ristretto.New: %v", err)
			}
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			return s
		},
		Settle:      func(s provider.Store) { s.(*ristretto.Ristretto).Wait() },
		Rejects:     true,
		SupportsTTL: true,
	})
}

func TestNewValidatesConfig(t *testing.T) {
	cases := []ristretto.Config{
		{NumCounters: 0, MaxCost: 1, BufferItems: 1},
		{NumCounters: 1, MaxCost: 0, BufferItems: 1},
		{NumCounters: 1, MaxCost: 1, BufferItems: 0},
	}
	for _, cfg := range cases {
		if _, err := ristretto.New(cfg); err == nil {
			t.Fatalf("New(%+v) accepted an invalid config", cfg)
		}
	}
}
