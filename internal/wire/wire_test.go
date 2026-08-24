package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

func mustEncode(t testing.TB, fence backend.Fence, payload []byte) []byte {
	t.Helper()
	b, err := Encode(fence, payload)
	if err != nil {
		t.Fatalf("Encode(%v, %d bytes): %v", fence, len(payload), err)
	}
	return b
}

func rawFrame(fence, payload []byte) []byte {
	b := make([]byte, headerLen+len(payload))
	copy(b[offMagic:offVersion], magic)
	b[offVersion] = version
	b[offKind] = kindSingle
	copy(b[offFence:offVLen], fence)
	binary.BigEndian.PutUint32(b[offVLen:offCRC], uint32(len(payload)))
	copy(b[headerLen:], payload)
	binary.BigEndian.PutUint32(b[offCRC:headerLen], checksum(b[offFence:offVLen], payload))
	return b
}

func TestHeaderLayoutIsStable(t *testing.T) {
	// Keep the persisted layout stable.
	if headerLen != 30 {
		t.Fatalf("header length = %d, want 30", headerLen)
	}
	if version != 4 || kindSingle != 1 {
		t.Fatalf("frame identity changed: version=%d kind=%d", version, kindSingle)
	}
}

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"small", []byte("hello")},
		{"binary", []byte{0, 1, 2, 3, 0xff, 0}},
		{"large", bytes.Repeat([]byte("x"), 64<<10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fence := backend.NewFence()
			frame := mustEncode(t, fence, tc.payload)

			got, payload, err := Decode(frame)
			if err != nil {
				t.Fatalf("Decode rejected a frame Encode produced: %v", err)
			}
			if !got.Equal(fence) {
				t.Fatalf("fence = %v, want %v", got, fence)
			}
			if !bytes.Equal(payload, tc.payload) {
				t.Fatalf("payload = %q, want %q", payload, tc.payload)
			}
		})
	}
}

func TestDecodeReturnsAViewOfItsInput(t *testing.T) {
	frame := mustEncode(t, backend.NewFence(), []byte("payload"))

	_, payload, err := Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) == 0 || &payload[0] != &frame[headerLen] {
		t.Fatal("Decode must return a subslice of its input, not a copy")
	}
}

func TestEncodeRejectsInvalidFence(t *testing.T) {
	_, err := Encode(backend.Fence{}, []byte("payload"))
	if !errors.Is(err, ErrInvalidFrame) || !errors.Is(err, backend.ErrInvalidFence) {
		t.Fatalf("Encode(zero fence) error = %v, want ErrInvalidFrame wrapping ErrInvalidFence", err)
	}
}

func TestDecodeClassifiesFailures(t *testing.T) {
	fence := backend.NewFence()
	payload := []byte("payload")

	damage := func(fn func([]byte) []byte) []byte {
		return fn(mustEncode(t, fence, payload))
	}

	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"nil", nil, ErrInvalidFrame},
		{"shorter than identity", []byte("CAS"), ErrInvalidFrame},
		{"foreign bytes", []byte("not a cascache frame at all, honestly"), ErrInvalidFrame},
		{"bad magic", damage(func(b []byte) []byte { b[0] = 'X'; return b }), ErrInvalidFrame},
		{
			"future version",
			damage(func(b []byte) []byte { b[offVersion] = version + 1; return b }),
			ErrUnsupportedFormat,
		},
		{"past version", damage(func(b []byte) []byte { b[offVersion] = version - 1; return b }), ErrUnsupportedFormat},
		{"unknown kind", damage(func(b []byte) []byte { b[offKind] = kindSingle + 1; return b }), ErrUnsupportedFormat},
		{"header truncated", damage(func(b []byte) []byte { return b[:headerLen-1] }), ErrInvalidFrame},
		{"body truncated", damage(func(b []byte) []byte { return b[:len(b)-1] }), ErrInvalidFrame},
		{"trailing bytes", damage(func(b []byte) []byte { return append(b, 0) }), ErrInvalidFrame},
		{"payload corrupted", damage(func(b []byte) []byte { b[headerLen]++; return b }), ErrInvalidFrame},
		{"fence corrupted", damage(func(b []byte) []byte { b[offFence]++; return b }), ErrInvalidFrame},
		{"length field lies", damage(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[offVLen:offCRC], uint32(len(payload)+1))
			return b
		}), ErrInvalidFrame},
		{"zero fence with a valid checksum", rawFrame(make([]byte, backend.FenceSize), payload), ErrInvalidFrame},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Decode(tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Decode error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestChecksumCoversFence(t *testing.T) {
	payload := []byte("payload")
	frame := mustEncode(t, backend.NewFence(), payload)

	other := backend.NewFence()
	copy(frame[offFence:offVLen], other.Bytes())

	if _, _, err := Decode(frame); !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("Decode of a frame with a swapped fence = %v, want ErrInvalidFrame", err)
	}
}

func FuzzDecode(f *testing.F) {
	fence := backend.NewFence()
	f.Add(mustEncode(f, fence, nil))
	f.Add(mustEncode(f, fence, []byte("payload")))
	f.Add([]byte("CASC"))
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, b []byte) {
		got, payload, err := Decode(b)
		if err != nil {
			return
		}
		again, err := Encode(got, payload)
		if err != nil {
			t.Fatalf("Encode rejected a decoded frame: %v", err)
		}
		if !bytes.Equal(again, b) {
			t.Fatalf("re-encode mismatch:\n got %x\nwant %x", again, b)
		}
	})
}

func BenchmarkEncode(b *testing.B) {
	fence := backend.NewFence()
	payload := bytes.Repeat([]byte("x"), 1024)
	b.ReportAllocs()
	for range b.N {
		if _, err := Encode(fence, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	frame := mustEncode(b, backend.NewFence(), bytes.Repeat([]byte("x"), 1024))
	b.ReportAllocs()
	for range b.N {
		if _, _, err := Decode(frame); err != nil {
			b.Fatal(err)
		}
	}
}
