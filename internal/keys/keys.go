package keys

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
)

var (
	errBatchKeyPartTooLong   = errors.New("batch key component exceeds uint32 framing limit")
	errBatchKeyFrameTooLarge = errors.New("batch key frame exceeds platform int capacity")
	errBatchKeysNotSorted    = errors.New("batch keys must be sorted in ascending order")
	errBatchKeysNotUnique    = errors.New("batch keys must be unique")
)

const (
	rootPrefix         = "cas:v3:"
	valueRoot          = rootPrefix + "val:"
	versionRoot        = rootPrefix + "ver:"
	singleKind         = "s:"
	batchKind          = "b:"
	maxBatchKeyPartLen = uint64(math.MaxUint32)
)

// CacheKey is the canonical single-key identity shared with the version store.
type CacheKey string

func (k CacheKey) String() string {
	return string(k)
}

// ValueKey is the provider storage key for an encoded cache value.
type ValueKey string

func (k ValueKey) String() string {
	return string(k)
}

// Single holds the canonical key identity used by the version store and provider.
type Single struct {
	Cache CacheKey
	Value ValueKey
}

// Keyspace owns the v3 keyspace for one logical namespace.
type Keyspace struct {
	singlePrefix string
	batchPrefix  string
}

// NewKeyspace precomputes the stable prefixes for one logical namespace.
func NewKeyspace(namespace string) Keyspace {
	fr := frameNamespace(namespace)
	sp := singleKind + fr

	return Keyspace{
		singlePrefix: sp,
		batchPrefix:  valueRoot + batchKind + fr,
	}
}

// SingleCacheKey returns the canonical identity for one logical key.
// This is the key seen by the version store and by slot-tag derivation.
func (s Keyspace) SingleCacheKey(userKey string) CacheKey {
	return CacheKey(s.singlePrefix + userKey)
}

// SingleValueKey returns the provider storage key for one logical key.
func (s Keyspace) SingleValueKey(userKey string) ValueKey {
	return storageKey(s.SingleCacheKey(userKey))
}

// Single returns both canonical keys for one logical key.
func (s Keyspace) Single(userKey string) Single {
	ck := s.SingleCacheKey(userKey)
	return Single{
		Cache: ck,
		Value: storageKey(ck),
	}
}

// VersionStorageKey returns the backing storage key for authoritative version state.
// Single value keys and version-state keys intentionally share the same Redis
// hash tag so backend-native single-key scripts can target one cluster slot.
func VersionStorageKey(cacheKey CacheKey) string {
	return versionRoot + slotPrefix(cacheKey) + string(cacheKey)
}

// BatchValueSorted returns the provider value key for a batch entry.
// sortedKeys must already be sorted in ascending order and contain no duplicates.
func (s Keyspace) BatchValueSorted(sortedKeys []string) (ValueKey, error) {
	d, err := digestSortedKeys(sortedKeys)
	if err != nil {
		return "", err
	}
	return ValueKey(s.batchPrefix + d), nil
}

// digestSortedKeys hashes a canonical sorted, duplicate-free logical key set.
// Length framing preserves boundaries so ["ab", "c"] and ["a", "bc"] do not
// collide before hashing.
func digestSortedKeys(sortedKeys []string) (string, error) {
	// exact buffer size: 4 bytes length + key bytes per key.
	total := uint64(0)
	for i, k := range sortedKeys {
		if i > 0 {
			prev := sortedKeys[i-1]
			switch {
			case k < prev:
				return "", errBatchKeysNotSorted
			case k == prev:
				return "", errBatchKeysNotUnique
			}
		}

		klen := uint64(len(k))
		if klen > maxBatchKeyPartLen {
			return "", errBatchKeyPartTooLong
		}
		total += 4 + klen
	}
	if total > uint64(math.MaxInt) {
		return "", errBatchKeyFrameTooLarge
	}

	buf := make([]byte, int(total))
	off := 0

	for _, k := range sortedKeys {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(k)))
		off += 4
		copy(buf[off:], k)
		off += len(k)
	}

	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:16]), nil
}

func frameNamespace(ns string) string {
	return strconv.Itoa(len(ns)) + ":" + ns + ":"
}

// storageKey derives the provider value key for one canonical key.
func storageKey(cacheKey CacheKey) ValueKey {
	return ValueKey(valueRoot + slotPrefix(cacheKey) + string(cacheKey))
}

// slotPrefix wraps the stable hash tag used to colocate related Redis keys.
func slotPrefix(cacheKey CacheKey) string {
	return "{" + slotTag(cacheKey) + "}:"
}

// slotTag derives the Redis Cluster hash tag from the canonical single-key
// identity rather than the raw user key so namespace framing stays part of the
// placement decision.
func slotTag(cacheKey CacheKey) string {
	s := sha256.Sum256([]byte(cacheKey))
	return hex.EncodeToString(s[:16])
}
