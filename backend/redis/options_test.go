package redis

import (
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

func TestOptionsResolveInvalidationTTL(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want time.Duration
		bad  bool
	}{
		{name: "zero uses the default", want: DefaultInvalidationTTL},
		{name: "explicit", ttl: time.Hour, want: time.Hour},
		{name: "no expiration", ttl: backend.NoExpiration},
		{name: "other negatives are rejected", ttl: -5 * time.Second, bad: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (Options{InvalidationTTL: tt.ttl}).resolve()
			if tt.bad {
				if err == nil {
					t.Fatal("resolve accepted an invalid invalidation TTL")
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("resolve = %v, %v; want %v", got, err, tt.want)
			}
		})
	}
}
