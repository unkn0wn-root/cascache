package wire

import (
	"bytes"
	"encoding/binary"
	"math"
	"strings"
	"testing"
)

func mustDecodeSingle(t *testing.T, b []byte) (uint64, []byte) {
	t.Helper()
	gen, p, err := DecodeSingle(b)
	if err != nil {
		t.Fatalf("DecodeSingle error: %v", err)
	}
	return gen, p
}

func mustDecodeBulk(t *testing.T, b []byte) []BulkItem {
	t.Helper()
	it, err := DecodeBulk(b)
	if err != nil {
		t.Fatalf("DecodeBulk error: %v", err)
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
		enc := EncodeSingle(tc.gen, tc.payload)
		gen, p := mustDecodeSingle(t, enc)
		if gen != tc.gen {
			t.Fatalf("gen mismatch: got %d want %d", gen, tc.gen)
		}
		if !bytes.Equal(p, tc.payload) {
			t.Fatalf("payload mismatch: got %x want %x", p, tc.payload)
		}
	}
}

func TestSingleRejectsTrailingBytes(t *testing.T) {
	enc := EncodeSingle(7, []byte("x"))
	enc = append(enc, 0xDE, 0xAD) // add junk
	if _, _, err := DecodeSingle(enc); err == nil {
		t.Fatalf("expected error on trailing bytes")
	}
}

func TestSingleCorruptHeadersAndLengths(t *testing.T) {
	enc := EncodeSingle(1, []byte("abc"))

	// bad magic
	badMagic := append([]byte(nil), enc...)
	badMagic[0] = 'X'
	if _, _, err := DecodeSingle(badMagic); err == nil {
		t.Fatalf("expected error on bad magic")
	}

	// wrong version
	badVer := append([]byte(nil), enc...)
	badVer[4] = version + 1
	if _, _, err := DecodeSingle(badVer); err == nil {
		t.Fatalf("expected error on bad version")
	}

	// wrong kind
	badKind := append([]byte(nil), enc...)
	badKind[5] = kindBulk
	if _, _, err := DecodeSingle(badKind); err == nil {
		t.Fatalf("expected error on bad kind")
	}

	// vlen too large (announce more than available)
	tooLong := append([]byte(nil), enc...)
	// vlen is at offset 14..17 (4 magic +1 ver +1 kind +8 gen)
	binary.BigEndian.PutUint32(tooLong[14:18], uint32(len("abc")+1))
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
	enc := EncodeSingle(1, []byte("Z"))
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

func TestBulkRoundTrip(t *testing.T) {
	cases := [][]BulkItem{
		nil, // n=0
		{{Key: "a", Gen: 1, Payload: []byte("x")}},
		{
			{Key: "a", Gen: 1, Payload: []byte("x")},
			{Key: "b", Gen: 2, Payload: nil}, // empty payload
			{Key: "c", Gen: 3, Payload: []byte{9, 8, 7}},
		},
		// duplicates allowed. decoder preserves both
		{
			{Key: "dup", Gen: 1, Payload: []byte("old")},
			{Key: "dup", Gen: 2, Payload: []byte("new")},
		},
	}
	for _, items := range cases {
		enc, err := EncodeBulk(items)
		if err != nil {
			t.Fatalf("EncodeBulk error: %v", err)
		}
		got := mustDecodeBulk(t, enc)
		if len(got) != len(items) {
			t.Fatalf("len mismatch: got %d want %d", len(got), len(items))
		}
		for i := range items {
			if got[i].Key != items[i].Key || got[i].Gen != items[i].Gen || !bytes.Equal(got[i].Payload, items[i].Payload) {
				t.Fatalf("item %d mismatch: got=%+v want=%+v", i, got[i], items[i])
			}
		}
	}
}

func TestBulkRejectsTrailingBytes(t *testing.T) {
	enc, err := EncodeBulk([]BulkItem{{Key: "k", Gen: 1, Payload: []byte("v")}})
	if err != nil {
		t.Fatalf("EncodeBulk: %v", err)
	}
	enc = append(enc, 0xBE, 0xEF)
	if _, err := DecodeBulk(enc); err == nil {
		t.Fatalf("expected error on trailing bytes")
	}
}

func TestBulkWrongAndTruncation(t *testing.T) {
	// Wrong n (very large) with no items -> must error, not panic.
	var buf bytes.Buffer
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	buf.WriteByte(version)
	buf.WriteByte(kindBulk)
	var u4 [4]byte
	binary.BigEndian.PutUint32(u4[:], ^uint32(0)) // n = 0xFFFFFFFF
	buf.Write(u4[:])
	if _, err := DecodeBulk(buf.Bytes()); err == nil {
		t.Fatalf("expected error on bogus n with insufficient bytes")
	}

	// Declare n=1 but provide no item body -> error
	buf.Reset()
	buf.Write([]byte{'C', 'A', 'S', 'C'})
	buf.WriteByte(version)
	buf.WriteByte(kindBulk)
	binary.BigEndian.PutUint32(u4[:], 1)
	buf.Write(u4[:])
	if _, err := DecodeBulk(buf.Bytes()); err == nil {
		t.Fatalf("expected error on truncated item list")
	}
}

func TestBulkKeyLengthValidation(t *testing.T) {
	// empty key -> error
	if _, err := EncodeBulk([]BulkItem{{Key: "", Gen: 1, Payload: []byte("x")}}); err == nil {
		t.Fatalf("expected error on empty key")
	}
	// too long key (65536) -> error
	if _, err := EncodeBulk([]BulkItem{{Key: strings.Repeat("a", 0x10000), Gen: 1}}); err == nil {
		t.Fatalf("expected error on key length > 0xFFFF")
	}
	// boundary (65535) -> ok
	if _, err := EncodeBulk([]BulkItem{{Key: strings.Repeat("b", 0xFFFF), Gen: 1}}); err != nil {
		t.Fatalf("boundary key length should succeed: %v", err)
	}
}

func TestBulkCorruptHeadersAndLengths(t *testing.T) {
	enc, err := EncodeBulk([]BulkItem{
		{Key: "k", Gen: 9, Payload: []byte("xyz")},
	})
	if err != nil {
		t.Fatalf("EncodeBulk: %v", err)
	}

	// bad magic
	badMagic := append([]byte(nil), enc...)
	badMagic[0] = 'X'
	if _, err := DecodeBulk(badMagic); err == nil {
		t.Fatalf("expected error on bad magic")
	}

	// wrong version
	badVer := append([]byte(nil), enc...)
	badVer[4] = version + 1
	if _, err := DecodeBulk(badVer); err == nil {
		t.Fatalf("expected error on bad version")
	}

	// wrong kind
	badKind := append([]byte(nil), enc...)
	badKind[5] = kindSingle
	if _, err := DecodeBulk(badKind); err == nil {
		t.Fatalf("expected error on bad kind")
	}

	// vlen beyond remaining
	// Locate first item's vlen field:
	// header: 4 magic +1 ver +1 kind +4 n = 10 bytes
	// item: 2 klen + klen + 8 gen + 4 vlen + payload
	klen := 1                   // "k"
	offset := 10 + 2 + klen + 8 // start of vlen
	badVlen := append([]byte(nil), enc...)
	binary.BigEndian.PutUint32(badVlen[offset:offset+4], uint32(len("xyz")+1))
	if _, err := DecodeBulk(badVlen); err == nil {
		t.Fatalf("expected error on vlen beyond buffer")
	}

	// klen too large (announce more than available)
	badKlen := append([]byte(nil), enc...)
	// set klen=5 while only 1 byte of key is present
	binary.BigEndian.PutUint16(badKlen[10:12], uint16(5))
	if _, err := DecodeBulk(badKlen); err == nil {
		t.Fatalf("expected error on klen beyond buffer")
	}
}

func TestBulkZeroCopyPayloadSlices(t *testing.T) {
	items := []BulkItem{
		{Key: "a", Gen: 1, Payload: []byte("X")},
		{Key: "b", Gen: 2, Payload: []byte("Y")},
	}
	enc, err := EncodeBulk(items)
	if err != nil {
		t.Fatalf("EncodeBulk: %v", err)
	}
	got := mustDecodeBulk(t, enc)
	if len(got) != 2 || len(got[0].Payload) != 1 {
		t.Fatalf("unexpected decoded items")
	}

	// mutate decoded payload. should mutate underlying enc bytes
	got[0].Payload[0] = 'Q'

	// re-decode from the same enc buffer. change should be visible
	got2 := mustDecodeBulk(t, enc)
	if got2[0].Payload[0] != 'Q' {
		t.Fatalf("expected zero-copy payload subslices into enc buffer")
	}
}
