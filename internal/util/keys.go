package util

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

// BulkKey returns "prefix:<first16hex(sha256(sorted(keys)))>" and sorts a copy.
func BulkKey(prefix string, keys []string) string {
	s := make([]string, len(keys))
	copy(s, keys)
	sort.Strings(s)
	return BulkKeySorted(prefix, s)
}

// BulkKeySorted returns the bulk key for an already-sorted slice of keys.
func BulkKeySorted(prefix string, sortedKeys []string) string {
	joined := strings.Join(sortedKeys, ",")
	sum := sha256.Sum256([]byte(joined))
	hex := fmt.Sprintf("%x", sum[:])
	return fmt.Sprintf("%s:%s", prefix, hex[:16])
}
