// Allocation counts are not stable under the race detector.
//
//go:build !race

package cascache_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/unkn0wn-root/cascache/v4"
	"github.com/unkn0wn-root/cascache/v4/backend"
	"github.com/unkn0wn-root/cascache/v4/codec"
	"github.com/unkn0wn-root/cascache/v4/internal/memstore"
)

// The counts include copies made by memstore and codec.Bytes.
func TestAllocs(t *testing.T) {
	ctx := context.Background()
	value := bytes.Repeat([]byte("v"), 256)

	b, err := backend.NewLocal(memstore.New(memstore.Options{}), backend.LocalOptions{
		InvalidationTTL: backend.NoExpiration,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	c, err := cascache.New(cascache.Options[[]byte]{
		Namespace:  "allocs",
		Backend:    b,
		Codec:      codec.Bytes{},
		DefaultTTL: cascache.NoExpiration,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := c.Snapshot(ctx, "hit")
	if err != nil {
		t.Fatal(err)
	}
	if res, err := c.Set(ctx, "hit", value, snapshot); err != nil || res.Outcome != cascache.SetOutcomeStored {
		t.Fatalf("Set = %v, %v", res.Outcome, err)
	}

	const runs = 1000
	// One fresh key per miss, plus one for the warm-up run.
	misses := make([]string, runs+1)
	for i := range misses {
		misses[i] = "miss:" + strconv.Itoa(i)
	}

	errMiss := errors.New("miss")
	loadMiss := func(context.Context) ([]byte, error) { return nil, errMiss }
	loadValue := func(context.Context) ([]byte, error) { return value, nil }
	tests := []struct {
		name string
		op   func() error
		want float64
	}{
		{"Get hit", func() error {
			_, ok, err := c.Get(ctx, "hit")
			if err == nil && !ok {
				err = errMiss
			}
			return err
		}, 5},
		{"Load hit", func() error {
			_, err := c.Load(ctx, "hit", loadMiss)
			return err
		}, 5},
		{"Load miss", func() error {
			key := misses[0]
			misses = misses[1:]
			_, err := c.Load(ctx, key, loadValue)
			return err
		}, 14},
		{"Invalidate", func() error {
			return c.Invalidate(ctx, "gone")
		}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			got := testing.AllocsPerRun(runs, func() {
				if err == nil {
					err = tt.op()
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %v allocs per run, want %v", got, tt.want)
			}
		})
	}
}
