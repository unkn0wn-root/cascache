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
	if GenStorageKey(left.Cache) == GenStorageKey(right.Cache) {
		t.Fatalf("gen keys collided: %q", GenStorageKey(left.Cache))
	}
}

func TestBatchValueSortedUsesVersionedPrefixAndDigest(t *testing.T) {
	key, err := NewKeyspace("user").BatchValueSorted([]string{"a", "b"})
	if err != nil {
		t.Fatalf("BatchValueSorted: %v", err)
	}

	const prefix = "cas:v2:val:b:4:user:"
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

func TestSingleValueAndGenKeysShareRedisHashTag(t *testing.T) {
	single := NewKeyspace("user").Single("a")
	valueKey := single.Value.String()
	genKey := GenStorageKey(single.Cache)

	if !strings.HasPrefix(valueKey, "cas:v2:val:{") {
		t.Fatalf("value key prefix mismatch: %q", valueKey)
	}
	if !strings.HasPrefix(genKey, "cas:v2:gen:{") {
		t.Fatalf("gen key prefix mismatch: %q", genKey)
	}

	valueTag := redisHashTag(valueKey)
	genTag := redisHashTag(genKey)
	if valueTag == "" || genTag == "" {
		t.Fatalf("expected both keys to have Redis hash tags: value=%q gen=%q", valueKey, genKey)
	}
	if valueTag != genTag {
		t.Fatalf("hash tag mismatch: value=%q gen=%q", valueTag, genTag)
	}
}

func TestSingleKeysWithSpecialCharactersKeepStableHashTag(t *testing.T) {
	single := NewKeyspace("app:{prod}:x").Single("user:{42}:name")
	valueTag := redisHashTag(single.Value.String())
	genTag := redisHashTag(GenStorageKey(single.Cache))

	if valueTag == "" || genTag == "" {
		t.Fatalf("expected both keys to have Redis hash tags: value=%q gen=%q", single.Value, GenStorageKey(single.Cache))
	}
	if valueTag != genTag {
		t.Fatalf("hash tag mismatch: value=%q gen=%q", valueTag, genTag)
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
