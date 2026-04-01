package keys

import (
	"encoding/hex"
	"errors"
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
	if VersionStorageKey(left.Cache) == VersionStorageKey(right.Cache) {
		t.Fatalf("version keys collided: %q", VersionStorageKey(left.Cache))
	}
}

func TestBatchValueSortedUsesStablePrefixAndDigest(t *testing.T) {
	key, err := NewKeyspace("user").BatchValueSorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("BatchValueSorted: %v", err)
	}

	const prefix = "cas:v:b:4:user:"
	if !strings.HasPrefix(key.String(), prefix) {
		t.Fatalf("batch key prefix mismatch: %q", key)
	}

	digest := strings.TrimPrefix(key.String(), prefix)
	if len(digest) != 32 {
		t.Fatalf("batch digest length = %d, want 32", len(digest))
	}
	if _, err := hex.DecodeString(digest); err != nil {
		t.Fatalf("batch digest is not hex: %v", err)
	}
}

func TestBatchValueSortedIsDeterministic(t *testing.T) {
	space := NewKeyspace("user")

	first, err := space.BatchValueSorted([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("BatchValueSorted first: %v", err)
	}
	second, err := space.BatchValueSorted([]string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("BatchValueSorted second: %v", err)
	}
	if first != second {
		t.Fatalf("batch keys differ: %q vs %q", first, second)
	}
}

func TestBatchValueSortedRejectsUnsortedInput(t *testing.T) {
	_, err := NewKeyspace("user").BatchValueSorted([]string{"b", "a"})
	if !errors.Is(err, errBatchKeysNotSorted) {
		t.Fatalf("BatchValueSorted error = %v, want %v", err, errBatchKeysNotSorted)
	}
}

func TestBatchValueSortedRejectsDuplicateKeys(t *testing.T) {
	_, err := NewKeyspace("user").BatchValueSorted([]string{"a", "a"})
	if !errors.Is(err, errBatchKeysNotUnique) {
		t.Fatalf("BatchValueSorted error = %v, want %v", err, errBatchKeysNotUnique)
	}
}

func TestBatchValueSortedEmptyInputIsDeterministic(t *testing.T) {
	space := NewKeyspace("user")

	first, err := space.BatchValueSorted(nil)
	if err != nil {
		t.Fatalf("BatchValueSorted(nil): %v", err)
	}
	second, err := space.BatchValueSorted([]string{})
	if err != nil {
		t.Fatalf("BatchValueSorted(empty): %v", err)
	}
	if first != second {
		t.Fatalf("empty batch keys differ: %q vs %q", first, second)
	}
}

func TestSingleValueAndVersionKeysShareRedisHashTag(t *testing.T) {
	single := NewKeyspace("user").Single("a")
	valueKey := single.Value.String()
	versionKey := VersionStorageKey(single.Cache)

	if !strings.HasPrefix(valueKey, "cas:v:{") {
		t.Fatalf("value key prefix mismatch: %q", valueKey)
	}
	if !strings.HasPrefix(versionKey, "cas:ver:{") {
		t.Fatalf("version key prefix mismatch: %q", versionKey)
	}

	valueTag := redisHashTag(valueKey)
	versionTag := redisHashTag(versionKey)
	if valueTag == "" || versionTag == "" {
		t.Fatalf(
			"expected both keys to have Redis hash tags: value=%q version=%q",
			valueKey,
			versionKey,
		)
	}
	if valueTag != versionTag {
		t.Fatalf("hash tag mismatch: value=%q version=%q", valueTag, versionTag)
	}
}

func TestSingleKeysWithSpecialCharactersKeepStableHashTag(t *testing.T) {
	single := NewKeyspace("app:{prod}:x").Single("user:{42}:name")
	valueTag := redisHashTag(single.Value.String())
	versionTag := redisHashTag(VersionStorageKey(single.Cache))

	if valueTag == "" || versionTag == "" {
		t.Fatalf(
			"expected both keys to have Redis hash tags: value=%q version=%q",
			single.Value,
			VersionStorageKey(single.Cache),
		)
	}
	if valueTag != versionTag {
		t.Fatalf("hash tag mismatch: value=%q version=%q", valueTag, versionTag)
	}
}

func redisHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return ""
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end < 0 {
		return ""
	}
	return key[start+1 : start+1+end]
}
