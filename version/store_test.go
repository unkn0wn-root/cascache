package version

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestFenceAppendBinaryAppendsCanonical16Bytes(t *testing.T) {
	raw := [fenceTokenSize]byte{
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
	}
	f, err := ParseFenceBinary(raw[:])
	if err != nil {
		t.Fatalf("ParseFenceBinary error: %v", err)
	}

	prefix := []byte{0xAA, 0xBB, 0xCC}
	got := f.AppendBinary(append([]byte(nil), prefix...))
	want := append(append([]byte(nil), prefix...), raw[:]...)

	if len(got) != len(prefix)+fenceTokenSize {
		t.Fatalf("AppendBinary len = %d, want %d", len(got), len(prefix)+fenceTokenSize)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AppendBinary output mismatch: got %x want %x", got, want)
	}
}

func TestNewFenceRetriesZeroToken(t *testing.T) {
	orig := rand.Reader
	t.Cleanup(func() {
		rand.Reader = orig
	})

	rand.Reader = io.MultiReader(
		bytes.NewReader(make([]byte, fenceTokenSize)),
		bytes.NewReader([]byte{
			1, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
		}),
	)

	f, err := NewFence()
	if err != nil {
		t.Fatalf("NewFence error: %v", err)
	}
	if f == (Fence{}) {
		t.Fatalf("NewFence returned the zero fence")
	}
	if _, err := ParseFenceBinary(f.token[:]); err != nil {
		t.Fatalf("NewFence returned a parse-invalid fence: %v", err)
	}
}
