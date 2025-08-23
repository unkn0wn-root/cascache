package util

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
)

// BulkKey returns a deterministic composite key of sorted members with a short hash.
func BulkKey(prefix string, keys []string) string {
	s := make([]string, len(keys))
	copy(s, keys)
	sort.Strings(s)
	joined := strings.Join(s, ",")
	sum := sha256.Sum256([]byte(joined))
	return fmt.Sprintf("%s:%x", prefix, sum)[:len(prefix)+1+16] // prefix + ":" + first 16 hex chars
}
