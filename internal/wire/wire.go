package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	version    byte = 1
	kindSingle byte = 1
	kindBulk   byte = 2
)

var (
	ErrCorrupt = errors.New("cascache: corrupt entry")
	magic4     = [...]byte{'C', 'A', 'S', 'C'}
)

func hasMagic(b []byte) bool {
	return len(b) >= 4 && bytes.Equal(b[:4], magic4[:])
}

// Single: magic(4) | ver(1) | kind(1=single) | gen(u64 be) | vlen(u32 be) | payload(vlen)
func EncodeSingle(gen uint64, payload []byte) []byte {
	var buf bytes.Buffer
	buf.Grow(4 + 1 + 1 + 8 + 4 + len(payload))

	buf.Write(magic4[:])
	buf.WriteByte(version)
	buf.WriteByte(kindSingle)

	var u8 [8]byte
	var u4 [4]byte

	binary.BigEndian.PutUint64(u8[:], gen)
	buf.Write(u8[:])

	binary.BigEndian.PutUint32(u4[:], uint32(len(payload)))
	buf.Write(u4[:])

	buf.Write(payload)
	return buf.Bytes()
}

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
	if vlen < 0 || vlen > len(b)-off { // overflow-safe bound check
		return 0, nil, ErrCorrupt
	}

	return gen, b[off : off+vlen], nil
}

// Bulk:
//
//	magic(4) | ver(1) | kind(1=bulk) | n(u32 be)
//	keyLen(u16 be) | key(keyLen) | gen(u64 be) | vlen(u32 be) | payload(vlen) * n
type BulkItem struct {
	Key     string
	Gen     uint64
	Payload []byte
}

func EncodeBulk(items []BulkItem) []byte {
	total := 4 + 1 + 1 + 4
	for _, it := range items {
		total += 2 + len(it.Key) + 8 + 4 + len(it.Payload)
	}

	var buf bytes.Buffer
	buf.Grow(total)

	buf.Write(magic4[:])
	buf.WriteByte(version)
	buf.WriteByte(kindBulk)

	var u8 [8]byte
	var u4 [4]byte
	var u2 [2]byte

	binary.BigEndian.PutUint32(u4[:], uint32(len(items)))
	buf.Write(u4[:])

	for _, it := range items {
		if l := len(it.Key); l == 0 || l > 0xFFFF {
			panic("cascache: invalid key length in bulk")
		}
		binary.BigEndian.PutUint16(u2[:], uint16(len(it.Key)))
		buf.Write(u2[:])
		buf.WriteString(it.Key)

		binary.BigEndian.PutUint64(u8[:], it.Gen)
		buf.Write(u8[:])

		binary.BigEndian.PutUint32(u4[:], uint32(len(it.Payload)))
		buf.Write(u4[:])
		buf.Write(it.Payload)
	}

	return buf.Bytes()
}

func DecodeBulk(b []byte) ([]BulkItem, error) {
	const hdr = 4 + 1 + 1 + 4
	if len(b) < hdr || !hasMagic(b) || b[4] != version || b[5] != kindBulk {
		return nil, ErrCorrupt
	}

	off := 6

	// n
	n := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if n < 0 {
		return nil, ErrCorrupt
	}

	items := make([]BulkItem, 0, n)
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

	return items, nil
}
