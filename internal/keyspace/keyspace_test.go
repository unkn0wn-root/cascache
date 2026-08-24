package keyspace

import "testing"

func TestKeyFormat(t *testing.T) {
	if got, want := New("user").Key("u:1").ID(), "s:4:user:u:1"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
}

func TestKeyEncodingAvoidsDelimiterCollisions(t *testing.T) {
	cases := []struct {
		nsA, keyA string
		nsB, keyB string
	}{
		{"user", "u:1", "user:u", "1"},
		{"a", "b:c", "a:b", "c"},
		{"", "a:b", "a", "b"},
	}
	for _, tc := range cases {
		a := New(tc.nsA).Key(tc.keyA)
		b := New(tc.nsB).Key(tc.keyB)
		if a.ID() == b.ID() {
			t.Fatalf("(%q, %q) and (%q, %q) both map to %q",
				tc.nsA, tc.keyA, tc.nsB, tc.keyB, a.ID())
		}
	}
}

func TestKeyIsAlwaysValid(t *testing.T) {
	for _, ns := range []string{"", "user", "user:u"} {
		for _, key := range []string{"", "u:1", "plain"} {
			if k := New(ns).Key(key); !k.Valid() {
				t.Fatalf("New(%q).Key(%q) is invalid", ns, key)
			}
		}
	}
}

func BenchmarkKey(b *testing.B) {
	space := New("user")
	b.ReportAllocs()
	for range b.N {
		_ = space.Key("some-reasonably-long-user-key")
	}
}
