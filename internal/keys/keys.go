package keys

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"strconv"
)

var (
	errBulkKeyPartTooLong   = errors.New("bulk key component exceeds uint32 framing limit")
	errBulkKeyFrameTooLarge = errors.New("bulk key frame exceeds platform int capacity")
)

const (
	rootPrefix        = "cas:v1:"
	valueRoot         = rootPrefix + "val:"
	genRoot           = rootPrefix + "gen:"
	singleKind        = "s:"
	bulkKind          = "b:"
	maxBulkKeyPartLen = uint64(math.MaxUint32)
)

// CacheKey is the canonical single-key identity shared with the gen store.
type CacheKey string

func (k CacheKey) String() string {
	return string(k)
}

// ValueKey is the provider storage key for an encoded cache value.
type ValueKey string

func (k ValueKey) String() string {
	return string(k)
}

// Single holds the canonical key identity used by the GenStore and provider.
type Single struct {
	Cache CacheKey
	Value ValueKey
}

// Keyspace owns the v1 keyspace for one logical namespace.
type Keyspace struct {
	singleCachePrefix string
	singleValuePrefix string
	bulkValuePrefix   string
}

// NewKeyspace precomputes the stable prefixes for one logical namespace.
func NewKeyspace(namespace string) Keyspace {
	framed := frameNamespace(namespace)
	singleCachePrefix := singleKind + framed

	return Keyspace{
		singleCachePrefix: singleCachePrefix,
		singleValuePrefix: valueRoot + singleCachePrefix,
		bulkValuePrefix:   valueRoot + bulkKind + framed,
	}
}

func (s Keyspace) SingleCacheKey(userKey string) CacheKey {
	return CacheKey(s.singleCachePrefix + userKey)
}

func (s Keyspace) SingleValueKey(userKey string) ValueKey {
	return ValueKey(s.singleValuePrefix + userKey)
}

func (s Keyspace) Single(userKey string) Single {
	return Single{
		Cache: s.SingleCacheKey(userKey),
		Value: s.SingleValueKey(userKey),
	}
}

func GenStorageKey(cacheKey CacheKey) string {
	return genRoot + string(cacheKey)
}

func (s Keyspace) BulkValue(keys []string) (ValueKey, error) {
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	return s.BulkValueSorted(sorted)
}

// BulkValueSorted returns the provider value key for a bulk entry.
// sortedKeys must already be sorted in ascending order.
func (s Keyspace) BulkValueSorted(sortedKeys []string) (ValueKey, error) {
	digest, err := digestSortedKeys(sortedKeys)
	if err != nil {
		return "", err
	}
	return ValueKey(s.bulkValuePrefix + digest), nil
}

func digestSortedKeys(sortedKeys []string) (string, error) {
	// exact buffer size: 4 bytes length + key bytes per key.
	total := uint64(0)
	for _, k := range sortedKeys {
		klen := uint64(len(k))
		if klen > maxBulkKeyPartLen {
			return "", errBulkKeyPartTooLong
		}
		total += 4 + klen
	}
	if total > uint64(math.MaxInt) {
		return "", errBulkKeyFrameTooLarge
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
