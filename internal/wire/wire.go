// Package wire contains the compact, versioned on-the-wire format used by cascache
// to store values in the underlying Provider. It provides zero-copy decoders and
// pre-sized encoders for both single entries and batch entries.
//
// Encoding choices:
//   - All integers are big-endian (network byte order).
//   - A 4-byte ASCII magic ("CASC") allows quick format discrimination.
//   - A 1-byte version enables forward/backward compatibility in place.
//   - "kind" distinguishes single vs batch payloads.
//   - Each cached item carries one opaque per-key fence token.
//   - The payload after the fixed header is codec-opaque ([]byte).
//   - Decoders are written for bounds safety: every slice operation is preceded by
//     length checks; on any mismatch they return ErrCorrupt.
//   - Decoders return subslices of the original buffer for payloads (zero-copy).
//     NOTE: holding any subslice is sufficient to keep the backing array alive.
//     If you need to retain or mutate the payload beyond the frame’s lifetime,
//     make your own copy.
//   - Encode paths pre-compute exact frame sizes and fill pre-sized byte slices.
//   - Batch decode allocates exactly one string per item to materialize the key
//     (stable map key semantics).
//
// Strict framing:
//   - Decoders require that a frame consume the entire buffer (no trailing bytes).
//     This detects corruption/foreign writers early.
package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/unkn0wn-root/cascache/version"
)

const (
	// wireVersion is the wire-format version. Increment only on incompatible layout changes.
	wireVersion   byte = 3
	kindSingle         = 1
	kindBatch          = 2
	fenceSize          = 16
	maxKeyLen          = 0xFFFF
	maxUint32Wire      = uint64(^uint32(0))

	// fixed prefix of a single-entry frame.
	// magic(4) | ver(1) | kind(1) | fence(16) | vlen(4)
	sHdr = 4 + 1 + 1 + fenceSize + 4

	// fixed prefix of a batch frame (before per-item data).
	// magic(4) | ver(1) | kind(1) | n(4)
	bHdr = 4 + 1 + 1 + 4

	// smallest possible per-item footprint.
	// keyLen(2) | minKey(1) | fence(16) | vlen(4)
	minBatchSize = 2 + 1 + fenceSize + 4
)

var (
	ErrCorrupt = errors.New("corrupt entry")

	// fixed 4-byte magic header ("CASC").
	casc = [...]byte{'C', 'A', 'S', 'C'}
)

// hasMagic reports whether b starts with the "CASC" header.
func hasMagic(b []byte) bool {
	return len(b) >= 4 && bytes.Equal(b[:4], casc[:])
}

// EncodeSingle encodes a single entry.
//
// Layout (big-endian):
//
//	magic(4) | ver(1) | kind(1=single) | fence(16) | vlen(u32) | payload(vlen)
//
// The payload is the codec-encoded value. fence is the per-key authoritative
// freshness token used for read-side validation. Payload length
// is limited to <= 2^32-1 bytes.
func EncodeSingle(fence version.Fence, payload []byte) ([]byte, error) {
	vlen, err := checkedUint32(uint64(len(payload)), "payload length")
	if err != nil {
		return nil, err
	}

	out := make([]byte, sHdr+len(payload))
	copy(out[:4], casc[:])
	out[4] = wireVersion
	out[5] = kindSingle

	off := 6
	off += len(fence.AppendBinary(out[off:off])) // append into pre-sized slack
	binary.BigEndian.PutUint32(out[off:off+4], vlen)
	off += 4
	copy(out[off:], payload)
	return out, nil
}

// DecodeSingle parses a single entry and returns (fence, payload).
// The returned payload is a zero-copy subslice of b and must be treated as read-only.
func DecodeSingle(b []byte) (version.Fence, []byte, error) {
	if len(b) < sHdr || !hasMagic(b) || b[4] != wireVersion || b[5] != kindSingle {
		return version.Fence{}, nil, ErrCorrupt
	}

	off := 6
	f, err := version.ParseFenceBinary(b[off : off+fenceSize])
	if err != nil {
		return version.Fence{}, nil, ErrCorrupt
	}
	off += fenceSize

	// vlen
	if off+4 > len(b) {
		return version.Fence{}, nil, ErrCorrupt
	}

	vlen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if vlen < 0 || off+vlen != len(b) { // no trailing bytes allowed
		return version.Fence{}, nil, ErrCorrupt
	}
	return f, b[off : off+vlen], nil
}

// BatchItem holds one member of a batch-encoded set.
type BatchItem struct {
	Key     string
	Fence   version.Fence
	Payload []byte
}

// EncodeBatch encodes a batch set of items in a single value.
//
// Layout (big-endian):
//
//	magic(4) | ver(1) | kind(1=batch) | n(u32)
//	repeated n times:
//	  keyLen(u16) | key(keyLen) | fence(16) | vlen(u32) | payload(vlen)
//
// Returns an error if item count or payload lengths exceed uint32, or if any
// key length is 0 or > 65535 (u16).
func EncodeBatch(items []BatchItem) ([]byte, error) {
	n, err := checkedUint32(uint64(len(items)), "batch item count")
	if err != nil {
		return nil, err
	}

	total := bHdr
	for _, it := range items {
		l := len(it.Key)
		if l == 0 || l > maxKeyLen {
			return nil, fmt.Errorf("invalid key length %d", l)
		}
		if _, err := checkedUint32(uint64(len(it.Payload)), "payload length"); err != nil {
			return nil, err
		}
		total += 2 + l + fenceSize + 4 + len(it.Payload)
	}

	out := make([]byte, total)
	copy(out[:4], casc[:])
	out[4] = wireVersion
	out[5] = kindBatch

	off := 6
	binary.BigEndian.PutUint32(out[off:off+4], n)
	off += 4

	for _, it := range items {
		binary.BigEndian.PutUint16(out[off:off+2], uint16(len(it.Key)))
		off += 2
		off += copy(out[off:], it.Key)
		off += len(it.Fence.AppendBinary(out[off:off])) // append into pre-sized slack
		binary.BigEndian.PutUint32(out[off:off+4], uint32(len(it.Payload)))
		off += 4
		off += copy(out[off:], it.Payload)
	}

	return out, nil
}

// DecodeBatch parses a batch entry into items.
// Each item’s Payload is a zero-copy subslice of b and must be treated as read-only.
//
// For each item, Payload is a zero-copy subslice of b. Key is converted to a
// string (one allocation per item). Duplicate keys in the stored items are
// allowed; the last occurrence wins.
func DecodeBatch(b []byte) ([]BatchItem, error) {
	if len(b) < bHdr || !hasMagic(b) || b[4] != wireVersion || b[5] != kindBatch {
		return nil, ErrCorrupt
	}

	off := 6
	n := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 {
		return nil, ErrCorrupt
	}

	// Cap preallocation by what the buffer could plausibly contain to avoid
	// adversarial OOM if n is corrupted or malicious.
	rem := len(b) - off
	mp := 0
	if rem >= minBatchSize {
		mp = rem / minBatchSize
	}

	cap := min(n, mp)
	items := make([]BatchItem, 0, cap)

	for range n {
		// keyLen
		if off+2 > len(b) {
			return nil, ErrCorrupt
		}

		klen := int(binary.BigEndian.Uint16(b[off : off+2]))
		off += 2
		if klen <= 0 || klen > len(b)-off {
			return nil, ErrCorrupt
		}

		// key (slice, then string alloc)
		keyBytes := b[off : off+klen]
		off += klen

		// fence
		if off+fenceSize > len(b) {
			return nil, ErrCorrupt
		}
		f, err := version.ParseFenceBinary(b[off : off+fenceSize])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += fenceSize

		// vlen
		if off+4 > len(b) {
			return nil, ErrCorrupt
		}

		vlen := int(binary.BigEndian.Uint32(b[off : off+4]))
		off += 4
		// guard against 32-bit int overflow (vlen < 0) and out-of-bounds.
		if vlen < 0 || vlen > len(b)-off {
			return nil, ErrCorrupt
		}

		payload := b[off : off+vlen]
		off += vlen

		items = append(items, BatchItem{
			Key:     string(keyBytes), // one expected alloc per item
			Fence:   f,
			Payload: payload,
		})
	}

	// no trailing bytes allowed (frame must consume entire buffer).
	if off != len(b) {
		return nil, ErrCorrupt
	}

	return items, nil
}

func checkedUint32(n uint64, field string) (uint32, error) {
	if n > maxUint32Wire {
		return 0, fmt.Errorf("%s %d exceeds uint32 wire limit", field, n)
	}
	return uint32(n), nil
}
