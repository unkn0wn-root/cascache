// Package wire contains the compact, versioned on-the-wire format used by cascache
// to store values in the underlying Provider. It provides zero-copy decoders and
// pre-sized encoders for both single entries and bulk entries.
//
// Encoding choices:
//   - All integers are big-endian (network byte order).
//   - A 4-byte ASCII magic ("CASC") allows quick format discrimination.
//   - A 1-byte version enables forward/backward compatibility in place.
//   - "kind" distinguishes single vs bulk payloads.
//   - The payload after the fixed header is codec-opaque ([]byte).
//   - Decoders are written for bounds safety: every slice operation is preceded by
//     length checks; on any mismatch they return ErrCorrupt.
//   - Decoders return subslices of the original buffer for payloads (zero-copy).
//     NOTE: holding any subslice is sufficient to keep the backing array alive.
//     If you need to retain or mutate the payload beyond the frame’s lifetime,
//     make your own copy.
//   - Encode paths pre-compute capacity and bytes.Buffer.Grow to avoid realloc.
//   - Bulk decode allocates exactly one string per item to materialize the key
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
)

const (
	// version is the wire-format version. Bump only on incompatible layout changes.
	version    byte = 1
	kindSingle      = 1
	kindBulk        = 2
)

var (
	// ErrCorrupt is returned when a byte slice doesn't conform to the expected
	// structure (bad magic/version/kind/lengths).
	ErrCorrupt = errors.New("cascache: corrupt entry")

	// magic4 is the fixed 4-byte magic header ("CASC").
	magic4 = [...]byte{'C', 'A', 'S', 'C'}
)

// hasMagic reports whether b starts with the "CASC" header.
func hasMagic(b []byte) bool {
	return len(b) >= 4 && bytes.Equal(b[:4], magic4[:])
}

// EncodeSingle encodes a single entry.
//
// Layout (big-endian):
//
//	magic(4) | ver(1) | kind(1=single) | gen(u64) | vlen(u32) | payload(vlen)
//
// The payload is the codec-encoded value. gen is the per-key generation used for
// read-side validation (CAS). Payload length is limited to <= 4 GiB (uint32).
func EncodeSingle(gen uint64, payload []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(4 + 1 + 1 + 8 + 4 + len(payload))

	// header
	buf.Write(magic4[:])
	buf.WriteByte(version)
	buf.WriteByte(kindSingle)

	// body
	var u8 [8]byte
	var u4 [4]byte

	binary.BigEndian.PutUint64(u8[:], gen)
	buf.Write(u8[:])

	binary.BigEndian.PutUint32(u4[:], uint32(len(payload)))
	buf.Write(u4[:])

	buf.Write(payload)
	return buf.Bytes()
}

// DecodeSingle parses a single entry and returns (gen, payload).
// The returned payload is a zero-copy subslice of b and must be treated as read-only.
func DecodeSingle(b []byte) (gen uint64, payload []byte, err error) {
	const hdr = 4 + 1 + 1 + 8 + 4
	if len(b) < hdr || !hasMagic(b) || b[4] != version || b[5] != kindSingle {
		return 0, nil, ErrCorrupt
	}

	off := 6

	// gen
	gen = binary.BigEndian.Uint64(b[off : off+8])
	off += 8

	// vlen
	if off+4 > len(b) {
		return 0, nil, ErrCorrupt
	}

	vlen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if vlen < 0 || off+vlen != len(b) { // no trailing bytes allowed
		return 0, nil, ErrCorrupt
	}
	return gen, b[off : off+vlen], nil
}

// BulkItem holds one member of a bulk-encoded set.
type BulkItem struct {
	Key     string
	Gen     uint64
	Payload []byte
}

// EncodeBulk encodes a bulk set of items in a single value.
//
// Layout (big-endian):
//
//	magic(4) | ver(1) | kind(1=bulk) | n(u32)
//	repeated n times:
//	  keyLen(u16) | key(keyLen) | gen(u64) | vlen(u32) | payload(vlen)
//
// Returns an error if any key length is 0 or > 65535 (u16).
func EncodeBulk(items []BulkItem) ([]byte, error) {
	total := 4 + 1 + 1 + 4
	for _, it := range items {
		l := len(it.Key)
		if l == 0 || l > 0xFFFF {
			return nil, fmt.Errorf("cascache: invalid key length %d", l)
		}
		total += 2 + l + 8 + 4 + len(it.Payload)
	}

	var buf bytes.Buffer
	buf.Grow(total)

	// header
	buf.Write(magic4[:])
	buf.WriteByte(version)
	buf.WriteByte(kindBulk)

	var u8 [8]byte
	var u4 [4]byte
	var u2 [2]byte

	binary.BigEndian.PutUint32(u4[:], uint32(len(items)))
	buf.Write(u4[:])

	for _, it := range items {
		binary.BigEndian.PutUint16(u2[:], uint16(len(it.Key)))
		buf.Write(u2[:])
		buf.WriteString(it.Key)

		binary.BigEndian.PutUint64(u8[:], it.Gen)
		buf.Write(u8[:])

		binary.BigEndian.PutUint32(u4[:], uint32(len(it.Payload)))
		buf.Write(u4[:])
		buf.Write(it.Payload)
	}

	return buf.Bytes(), nil
}

// DecodeBulk parses a bulk entry into items.
// Each item’s Payload is a zero-copy subslice of b and must be treated as read-only.
//
// For each item, Payload is a zero-copy subslice of b. Key is converted to a
// string (one allocation per item). Duplicate keys in the stored items are
// allowed; the last occurrence wins.
func DecodeBulk(b []byte) ([]BulkItem, error) {
	const hdr = 4 + 1 + 1 + 4
	if len(b) < hdr || !hasMagic(b) || b[4] != version || b[5] != kindBulk {
		return nil, ErrCorrupt
	}

	off := 6
	n := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 {
		return nil, ErrCorrupt
	}

	// cap preallocation by what the buffer could plausibly contain to avoid
	// adversarial OOM if n iss bogus. We assume the minimal per-item footprint:
	// klen(2) + min key(1) + gen(8) + vlen(4) + min payload(0) = 15 bytes.
	rem := len(b) - off
	const minItem = 2 + 1 + 8 + 4
	maxPlausible := 0
	if rem >= minItem {
		maxPlausible = rem / minItem
	}

	capHint := n
	if capHint > maxPlausible {
		capHint = maxPlausible
	}
	items := make([]BulkItem, 0, capHint)

	for i := 0; i < n; i++ {
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

		// gen
		if off+8 > len(b) {
			return nil, ErrCorrupt
		}
		gen := binary.BigEndian.Uint64(b[off : off+8])
		off += 8

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

		items = append(items, BulkItem{
			Key:     string(keyBytes), // one expected alloc per item
			Gen:     gen,
			Payload: payload,
		})
	}

	// no trailing bytes allowed (frame must consume entire buffer).
	if off != len(b) {
		return nil, ErrCorrupt
	}

	return items, nil
}
