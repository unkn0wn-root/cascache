package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/unkn0wn-root/cascache/v3/version"
)

func fenceForVersionID(id uint64) version.Fence {
	var token [16]byte
	token[0] = 0xA5
	binary.BigEndian.PutUint64(token[8:], id)
	fence, err := version.ParseFenceBinary(token[:])
	if err != nil {
		panic(err)
	}
	return fence
}

func fenceForGen(gen uint64) version.Fence {
	return fenceForVersionID(gen)
}

func mustDecodeSingle(t *testing.T, b []byte) (version.Fence, []byte) {
	t.Helper()
	fence, p, err := DecodeSingle(b)
	if err != nil {
		t.Fatalf("DecodeSingle error: %v", err)
	}
	return fence, p
}

func mustEncodeSingle(t *testing.T, fence version.Fence, payload []byte) []byte {
	t.Helper()
	b, err := EncodeSingle(fence, payload)
	if err != nil {
		t.Fatalf("EncodeSingle error: %v", err)
	}
	return b
}

func mustDecodeBatch(t *testing.T, b []byte) []BatchItem {
	t.Helper()
	it, err := DecodeBatch(b)
	if err != nil {
		t.Fatalf("DecodeBatch error: %v", err)
	}
	return it
}

func TestSingleRTEmptyAndNonEmpty(t *testing.T) {
	cases := []struct {
		gen     uint64
		payload []byte
	}{
		{0, nil},
		{42, []byte("hello")},
		{math.MaxUint64, []byte{0, 1, 2, 3, 4}},
	}
	for _, tc := range cases {
		fence := fenceForVersionID(tc.gen)
		enc := mustEncodeSingle(t, fence, tc.payload)
		gotFence, p := mustDecodeSingle(t, enc)
		if !gotFence.Equal(fence) {
			t.Fatalf("fence mismatch: got %+v want %+v", gotFence, fence)
		}
		if !bytes.Equal(p, tc.payload) {
			t.Fatalf("payload mismatch: got %x want %x", p, tc.payload)
		}
	}
}

func TestSingleRejectsTrailingBytes(t *testing.T) {
	enc := mustEncodeSingle(t, fenceForVersionID(7), []byte("x"))
	enc = append(enc, 0xDE, 0xAD) // add junk
	if _, _, err := DecodeSingle(enc); err == nil {
		t.Fatalf("expected error on trailing bytes")
	}
}

func TestSingleCorruptHeadersAndLengths(t *testing.T) {
	enc := mustEncodeSingle(t, fenceForVersionID(1), []byte("abc"))

	// bad magic
	badMagic := append([]byte(nil), enc...)
	badMagic[0] = 'X'
	if _, _, err := DecodeSingle(badMagic); err == nil {
		t.Fatalf("expected error on bad magic")
	}

	// wrong version
	badVer := append([]byte(nil), enc...)
	badVer[4] = wireVersion + 1
	if _, _, err := DecodeSingle(badVer); err == nil {
		t.Fatalf("expected error on bad version")
	}

	// wrong kind
	badKind := append([]byte(nil), enc...)
	badKind[5] = kindBatch
	if _, _, err := DecodeSingle(badKind); err == nil {
		t.Fatalf("expected error on bad kind")
	}

	// vlen too large (announce more than available)
	tooLong := append([]byte(nil), enc...)
	// vlen is at offset 22..25 (4 magic +1 ver +1 kind +16 fence)
	binary.BigEndian.PutUint32(tooLong[22:26], uint32(len("abc")+1))
	if _, _, err := DecodeSingle(tooLong); err == nil {
		t.Fatalf("expected error on vlen beyond buffer")
	}

	// truncated buffer
	trunc := enc[:len(enc)-1]
	if _, _, err := DecodeSingle(trunc); err == nil {
		t.Fatalf("expected error on truncated buffer")
	}
}

func TestSingleZeroCopyPayload(t *testing.T) {
	enc := mustEncodeSingle(t, fenceForVersionID(1), []byte("Z"))
	_, p := mustDecodeSingle(t, enc)
	if len(p) != 1 {
		t.Fatalf("unexpected payload len")
	}
	// mutate payload slice. should mutate underlying enc bytes (zero-copy)
	p[0] = 'Q'
	_, p2 := mustDecodeSingle(t, enc)
	if p2[0] != 'Q' {
		t.Fatalf("expected zero-copy slice into enc buffer")
	}
}

func TestSingleRejectsHighBitVlen(t *testing.T) {
	enc := mustEncodeSingle(t, fenceForVersionID(1), []byte("abc"))
	badVlen := append([]byte(nil), enc...)
	binary.BigEndian.PutUint32(badVlen[22:26], ^uint32(0))
	if _, _, err := DecodeSingle(badVlen); err == nil {
		t.Fatalf("expected error on high-bit vlen")
	}
}

func TestCheckedUint32(t *testing.T) {
	got, err := checkedUint32(maxUint32Wire, "payload length")
	if err != nil {
		t.Fatalf("checkedUint32 boundary error: %v", err)
	}
	if got != ^uint32(0) {
		t.Fatalf("checkedUint32 boundary mismatch: got %d want %d", got, ^uint32(0))
	}

	if _, err := checkedUint32(maxUint32Wire+1, "payload length"); err == nil {
		t.Fatalf("checkedUint32 should reject values above uint32")
	}
}

func TestBatchRoundTrip(t *testing.T) {
	cases := [][]BatchItem{
		nil, // n=0
		{{Key: "a", Fence: fenceForVersionID(1), Payload: []byte("x")}},
		{
			{Key: "a", Fence: fenceForVersionID(1), Payload: []byte("x")},
			{Key: "b", Fence: fenceForVersionID(2), Payload: nil}, // empty payload
			{Key: "c", Fence: fenceForVersionID(3), Payload: []byte{9, 8, 7}},
		},
		// duplicates allowed. decoder preserves both
		{
			{Key: "dup", Fence: fenceForVersionID(1), Payload: []byte("old")},
			{Key: "dup", Fence: fenceForVersionID(2), Payload: []byte("new")},
		},
	}
	for _, items := range cases {
		enc, err := EncodeBatch(items)
		if err != nil {
			t.Fatalf("EncodeBatch error: %v", err)
		}
		got := mustDecodeBatch(t, enc)
		if len(got) != len(items) {
			t.Fatalf("len mismatch: got %d want %d", len(got), len(items))
		}
		for i := range items {
			if got[i].Key != items[i].Key || !got[i].Fence.Equal(items[i].Fence) ||
				!bytes.Equal(got[i].Payload, items[i].Payload) {
				t.Fatalf("item %d mismatch: got=%+v want=%+v", i, got[i], items[i])
			}
		}
	}
}

func TestBatchRejectsTrailingBytes(t *testing.T) {
	enc, err := EncodeBatch(
		[]BatchItem{{Key: "k", Fence: fenceForVersionID(1), Payload: []byte("v")}},
	)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	enc = append(enc, 0xBE, 0xEF)
	if _, err := DecodeBatch(enc); err == nil {
		t.Fatalf("expected error on trailing bytes")
	}
}

func TestBatchWrongAndTruncation(t *testing.T) {
	// Wrong n (very large) with no items -> must error, not panic.
	var buf bytes.Buffer
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	buf.WriteByte(wireVersion)
	buf.WriteByte(kindBatch)
	var u4 [4]byte
	binary.BigEndian.PutUint32(u4[:], ^uint32(0)) // n = 0xFFFFFFFF
	buf.Write(u4[:])
	if _, err := DecodeBatch(buf.Bytes()); err == nil {
		t.Fatalf("expected error on bogus n with insufficient bytes")
	}

	// Declare n=1 but provide no item body -> error
	buf.Reset()
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	buf.WriteByte(wireVersion)
	buf.WriteByte(kindBatch)
	binary.BigEndian.PutUint32(u4[:], 1)
	buf.Write(u4[:])
	if _, err := DecodeBatch(buf.Bytes()); err == nil {
		t.Fatalf("expected error on truncated item list")
	}
}

func TestBatchKeyLengthValidation(t *testing.T) {
	// empty key -> error
	if _, err := EncodeBatch(
		[]BatchItem{{Key: "", Fence: fenceForGen(1), Payload: []byte("x")}},
	); err == nil {
		t.Fatalf("expected error on empty key")
	}
	// too long key (65536) -> error
	if _, err := EncodeBatch(
		[]BatchItem{{Key: strings.Repeat("a", 0x10000), Fence: fenceForGen(1)}},
	); err == nil {
		t.Fatalf("expected error on key length > 0xFFFF")
	}
	// boundary (65535) -> ok
	if _, err := EncodeBatch(
		[]BatchItem{{Key: strings.Repeat("b", 0xFFFF), Fence: fenceForGen(1)}},
	); err != nil {
		t.Fatalf("boundary key length should succeed: %v", err)
	}
}

func TestBatchCorruptHeadersAndLengths(t *testing.T) {
	enc, err := EncodeBatch([]BatchItem{
		{Key: "k", Fence: fenceForGen(9), Payload: []byte("xyz")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	// bad magic
	badMagic := append([]byte(nil), enc...)
	badMagic[0] = 'X'
	if _, err := DecodeBatch(badMagic); err == nil {
		t.Fatalf("expected error on bad magic")
	}

	// wrong version
	badVer := append([]byte(nil), enc...)
	badVer[4] = wireVersion + 1
	if _, err := DecodeBatch(badVer); err == nil {
		t.Fatalf("expected error on bad version")
	}

	// wrong kind
	badKind := append([]byte(nil), enc...)
	badKind[5] = kindSingle
	if _, err := DecodeBatch(badKind); err == nil {
		t.Fatalf("expected error on bad kind")
	}

	// vlen beyond remaining
	// Locate first item's vlen field:
	// header: 4 magic +1 ver +1 kind +4 n = 10 bytes
	// item: 2 klen + klen + 16 fence + 4 vlen + payload
	klen := 1                    // "k"
	offset := 10 + 2 + klen + 16 // start of vlen
	badVlen := append([]byte(nil), enc...)
	binary.BigEndian.PutUint32(badVlen[offset:offset+4], uint32(len("xyz")+1))
	if _, err := DecodeBatch(badVlen); err == nil {
		t.Fatalf("expected error on vlen beyond buffer")
	}

	// klen too large (announce more than available)
	badKlen := append([]byte(nil), enc...)
	// set klen=5 while only 1 byte of key is present
	binary.BigEndian.PutUint16(badKlen[10:12], uint16(5))
	if _, err := DecodeBatch(badKlen); err == nil {
		t.Fatalf("expected error on klen beyond buffer")
	}
}

func TestBatchRejectsHighBitVlen(t *testing.T) {
	enc, err := EncodeBatch([]BatchItem{
		{Key: "k", Fence: fenceForGen(9), Payload: []byte("xyz")},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}

	klen := 1
	offset := 10 + 2 + klen + 16
	badVlen := append([]byte(nil), enc...)
	binary.BigEndian.PutUint32(badVlen[offset:offset+4], ^uint32(0))
	if _, err := DecodeBatch(badVlen); err == nil {
		t.Fatalf("expected error on high-bit vlen")
	}
}

func TestBatchZeroCopyPayloadSlices(t *testing.T) {
	items := []BatchItem{
		{Key: "a", Fence: fenceForGen(1), Payload: []byte("X")},
		{Key: "b", Fence: fenceForGen(2), Payload: []byte("Y")},
	}
	enc, err := EncodeBatch(items)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	got := mustDecodeBatch(t, enc)
	if len(got) != 2 || len(got[0].Payload) != 1 {
		t.Fatalf("unexpected decoded items")
	}

	// mutate decoded payload. should mutate underlying enc bytes
	got[0].Payload[0] = 'Q'

	// re-decode from the same enc buffer. change should be visible
	got2 := mustDecodeBatch(t, enc)
	if got2[0].Payload[0] != 'Q' {
		t.Fatalf("expected zero-copy payload subslices into enc buffer")
	}
}
