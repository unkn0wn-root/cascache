package backend

import (
	"testing"
	"time"
)

func TestClampTTL(t *testing.T) {
	cases := []struct {
		name     string
		ttl      time.Duration
		lifetime time.Duration
		want     time.Duration
	}{
		{"no lifetime keeps the ttl", time.Hour, 0, time.Hour},
		{"no lifetime keeps no expiry", 0, 0, 0},
		{"shorter ttl is kept", time.Minute, time.Hour, time.Minute},
		{"longer ttl is clamped", 48 * time.Hour, time.Hour, time.Hour},
		{"no expiry is clamped to the lifetime", 0, time.Hour, time.Hour},
		{"negative ttl is clamped to the lifetime", -1, time.Hour, time.Hour},
		{"negative ttl without a lifetime means no expiry", -1, 0, 0},
		{"equal ttl and lifetime", time.Hour, time.Hour, time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClampTTL(tc.ttl, tc.lifetime); got != tc.want {
				t.Fatalf("ClampTTL(%v, %v) = %v, want %v", tc.ttl, tc.lifetime, got, tc.want)
			}
		})
	}
}

func TestStoreStatusString(t *testing.T) {
	cases := map[StoreStatus]string{
		StoreUnknown:     "unknown",
		StoreStored:      "stored",
		StoreConflict:    "conflict",
		StoreRejected:    "rejected",
		StoreStatus(200): "unknown",
	}
	for status, want := range cases {
		if got := status.String(); got != want {
			t.Fatalf("StoreStatus(%d).String() = %q, want %q", status, got, want)
		}
	}
}
