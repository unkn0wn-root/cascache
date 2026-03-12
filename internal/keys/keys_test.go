package keys

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestSingleKeysAreInjectiveAcrossNamespaceBoundaries(t *testing.T) {
	left := NewKeyspace("app:prod").Single("users:42")
	right := NewKeyspace("app").Single("prod:users:42")

	if left.Cache == right.Cache {
		t.Fatalf("cache keys collided: %q", left.Cache)
	}
	if left.Value == right.Value {
		t.Fatalf("value keys collided: %q", left.Value)
	}
	if GenStorageKey(left.Cache) == GenStorageKey(right.Cache) {
		t.Fatalf("gen keys collided: %q", GenStorageKey(left.Cache))
	}
}

func TestBulkValueSortedUsesVersionedPrefixAndDigest(t *testing.T) {
	key, err := NewKeyspace("user").BulkValueSorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("BulkValueSorted: %v", err)
	}

	const prefix = "cas:v1:val:b:4:user:"
	if !strings.HasPrefix(key.String(), prefix) {
		t.Fatalf("bulk key prefix mismatch: %q", key)
	}

	digest := strings.TrimPrefix(key.String(), prefix)
	if len(digest) != 32 {
		t.Fatalf("bulk digest length = %d, want 32", len(digest))
	}
	if _, err := hex.DecodeString(digest); err != nil {
		t.Fatalf("bulk digest is not hex: %v", err)
	}
}

func TestBulkValueMatchesSortedVariant(t *testing.T) {
	space := NewKeyspace("user")

	unsorted, err := space.BulkValue([]string{"b", "a", "c"})
	if err != nil {
		t.Fatalf("BulkValue: %v", err)
	}
	sorted, err := space.BulkValueSorted([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("BulkValueSorted: %v", err)
	}
	if unsorted != sorted {
		t.Fatalf("bulk keys differ: %q vs %q", unsorted, sorted)
	}
}
