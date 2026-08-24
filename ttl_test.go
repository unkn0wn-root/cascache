package cascache_test

import (
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4"
)

func TestJitterTTLStaysWithinItsRange(t *testing.T) {
	cases := []struct {
		name     string
		ttl      time.Duration
		floor    time.Duration
		fraction float64
		low      time.Duration
		high     time.Duration
	}{
		{"no jitter", time.Hour, 0, 0, time.Hour, time.Hour},
		{"full range", time.Hour, 0, 1, 0, time.Hour},
		{"fraction of the whole ttl", time.Hour, 0, 0.2, 48 * time.Minute, time.Hour},
		{"between floor and ttl", time.Hour, 30 * time.Minute, 1, 30 * time.Minute, time.Hour},
		{"fraction of the band", time.Hour, 30 * time.Minute, 0.5, 45 * time.Minute, time.Hour},
		{"fraction above one is clamped", time.Hour, 30 * time.Minute, 5, 30 * time.Minute, time.Hour},
		{"floor at or above ttl is ignored", time.Hour, 2 * time.Hour, 1, 0, time.Hour},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			compute := cascache.JitterTTL(tc.ttl, tc.floor, tc.fraction)

			var sawSpread bool
			var first time.Duration
			for i := range 500 {
				got, err := compute()
				if err != nil {
					t.Fatalf("JitterTTL: %v", err)
				}
				if got < tc.low || got > tc.high {
					t.Fatalf("TTL %v outside [%v, %v]", got, tc.low, tc.high)
				}
				if i == 0 {
					first = got
				} else if got != first {
					sawSpread = true
				}
			}

			if tc.low != tc.high && !sawSpread {
				t.Fatalf("every TTL was %v; the range [%v, %v] is not being used", first, tc.low, tc.high)
			}
			if tc.low == tc.high && sawSpread {
				t.Fatal("a fixed TTL varied")
			}
		})
	}
}

func TestJitterTTLLeavesNonPositiveTTLAlone(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		got, err := cascache.JitterTTL(ttl, 0, 0.5)()
		if err != nil {
			t.Fatal(err)
		}
		if got != ttl {
			t.Fatalf("JitterTTL(%v) = %v, want it unchanged", ttl, got)
		}
	}
}

func TestJitterTTLCanReachTheFloor(t *testing.T) {
	compute := cascache.JitterTTL(10*time.Nanosecond, 9*time.Nanosecond, 1)

	for range 1000 {
		got, err := compute()
		if err != nil {
			t.Fatal(err)
		}
		if got == 9*time.Nanosecond {
			return
		}
	}
	t.Fatal("the floor was never reached in 1000 draws")
}
