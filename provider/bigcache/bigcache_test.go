package bigcache_test

import (
	"context"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/provider"
	"github.com/unkn0wn-root/cascache/v4/provider/bigcache"
	"github.com/unkn0wn-root/cascache/v4/provider/providertest"
)

func TestStoreConformance(t *testing.T) {
	providertest.TestStore(t, providertest.Config{
		New: func(t testing.TB) provider.Store {
			s, err := bigcache.New(context.Background(), bigcache.Config{
				LifeWindow:         time.Hour,
				Shards:             16,
				MaxEntriesInWindow: 160,
				MaxEntrySize:       64 << 10,
				HardMaxCacheSizeMB: 64,
			})
			if err != nil {
				t.Fatalf("bigcache.New: %v", err)
			}
			t.Cleanup(func() { _ = s.Close(context.Background()) })
			return s
		},
		SupportsTTL: false,
	})
}
