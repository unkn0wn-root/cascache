// Package providertest tests implementations of [provider.Store].
package providertest

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/unkn0wn-root/cascache/v4/provider"
)

// Config describes the store under test.
type Config struct {
	// New returns a store for one subtest.
	New func(testing.TB) provider.Store

	// Settle waits for asynchronous writes. Nil means Set is immediately visible.
	Settle func(provider.Store)

	// SupportsTTL enables per-entry expiry checks.
	SupportsTTL bool

	// Rejects allows Set to return (false, nil).
	Rejects bool
}

// TestStore runs the full suite.
func TestStore(t *testing.T, cfg Config) {
	t.Helper()

	settle := func(s provider.Store) {
		if cfg.Settle != nil {
			cfg.Settle(s)
		}
	}

	set := func(t testing.TB, s provider.Store, key string, value []byte, ttl time.Duration) bool {
		t.Helper()
		ok, err := s.Set(context.Background(), key, value, 1, ttl)
		if err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
		if !ok && !cfg.Rejects {
			t.Fatalf("Set(%q) was declined by a store that should not decline", key)
		}
		settle(s)
		return ok
	}

	t.Run("absent key is a clean miss", func(t *testing.T) {
		s := cfg.New(t)
		value, found, err := s.Get(context.Background(), "providertest:absent")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if found || value != nil {
			t.Fatalf("Get of an absent key = %q, %v; want nil, false", value, found)
		}
	})

	t.Run("values survive byte for byte", func(t *testing.T) {
		cases := []struct {
			name  string
			value []byte
		}{
			{"empty", []byte{}},
			{"text", []byte("hello")},
			{"binary", []byte{0x00, 0x01, 0xff, 0xfe, 0x00, 0x7f, 0x80}},
			{"nul bytes only", []byte{0, 0, 0, 0}},
			{"large", bytes.Repeat([]byte{0xab}, 64<<10)},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				s := cfg.New(t)
				key := "providertest:roundtrip:" + tc.name

				if !set(t, s, key, tc.value, time.Hour) {
					t.Skip("the store declined the write")
				}

				got, found, err := s.Get(context.Background(), key)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if !found {
					t.Fatal("the value was not found after Set")
				}
				if !bytes.Equal(got, tc.value) {
					t.Fatalf("Get returned %q, want %q; the store is not byte transparent", got, tc.value)
				}
			})
		}
	})

	t.Run("set replaces", func(t *testing.T) {
		s := cfg.New(t)
		const key = "providertest:replace"

		if !set(t, s, key, []byte("first"), time.Hour) {
			t.Skip("the store declined the write")
		}
		if !set(t, s, key, []byte("second"), time.Hour) {
			t.Skip("the store declined the write")
		}

		got, found, err := s.Get(context.Background(), key)
		if err != nil || !found {
			t.Fatalf("Get = %v, %v", found, err)
		}
		if !bytes.Equal(got, []byte("second")) {
			t.Fatalf("Get returned %q, want the replacement", got)
		}
	})

	t.Run("del removes", func(t *testing.T) {
		s := cfg.New(t)
		const key = "providertest:delete"

		if !set(t, s, key, []byte("value"), time.Hour) {
			t.Skip("the store declined the write")
		}
		if err := s.Del(context.Background(), key); err != nil {
			t.Fatalf("Del: %v", err)
		}
		settle(s)

		if _, found, err := s.Get(context.Background(), key); err != nil || found {
			t.Fatalf("Get after Del = %v, %v; want a miss", found, err)
		}

		if err := s.Del(context.Background(), key); err != nil {
			t.Fatalf("Del of an absent key: %v", err)
		}
	})

	t.Run("keys stay separate", func(t *testing.T) {
		s := cfg.New(t)

		long := strings.Repeat("k", 200)
		keys := []string{
			"providertest:a", "providertest:a:b", "providertest:a:b:c",
			"providertest:a-b", "providertest:" + long + "1", "providertest:" + long + "2",
		}
		for _, key := range keys {
			if !set(t, s, key, []byte(key), time.Hour) {
				t.Skip("the store declined the write")
			}
		}
		for _, key := range keys {
			got, found, err := s.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			if !found {
				t.Fatalf("Get(%q) missed; the entry was displaced", key)
			}
			if !bytes.Equal(got, []byte(key)) {
				t.Fatalf("Get(%q) returned %q; two keys share storage", key, got)
			}
		}
	})

	if !cfg.SupportsTTL {
		return
	}

	t.Run("entries expire", func(t *testing.T) {
		s := cfg.New(t)
		const key = "providertest:expiring"

		if !set(t, s, key, []byte("value"), 100*time.Millisecond) {
			t.Skip("the store declined the write")
		}
		time.Sleep(400 * time.Millisecond)
		settle(s)

		if _, found, err := s.Get(context.Background(), key); err != nil || found {
			t.Fatalf("Get after the TTL = %v, %v; want a miss", found, err)
		}
	})

	t.Run("a nonpositive ttl means no expiry", func(t *testing.T) {
		s := cfg.New(t)
		const key = "providertest:permanent"

		if !set(t, s, key, []byte("value"), 0) {
			t.Skip("the store declined the write")
		}
		time.Sleep(200 * time.Millisecond)
		settle(s)

		if _, found, err := s.Get(context.Background(), key); err != nil || !found {
			t.Fatalf("Get = %v, %v; want the value to still be there", found, err)
		}
	})
}
