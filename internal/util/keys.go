package util

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// sorts a copy before calling BulkKeySorted
func BulkKey(prefix string, keys []string) string {
	s := make([]string, len(keys))
	copy(s, keys)
	sort.Strings(s)
	return BulkKeySorted(prefix, s)
}

// length-prefix each key into one preallocated []byte
func BulkKeySorted(prefix string, sortedKeys []string) string {
	// Compute exact buffer size: 4 bytes length + key bytes per key.
	total := 0
	for _, k := range sortedKeys {
		total += 4 + len(k)
	}

	buf := make([]byte, total)
	off := 0

	for _, k := range sortedKeys {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(k)))
		off += 4
		copy(buf[off:], k)
		off += len(k)
	}

	sum := sha256.Sum256(buf)
	return prefix + ":" + hex16(sum[:])
}

func hex16(b []byte) string {
	const hexdigits = "0123456789abcdef"
	// first 8 bytes -> 16 hex chars
	out := make([]byte, 16)
	for i := 0; i < 8; i++ {
		v := b[i]
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}
