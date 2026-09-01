package codec

import (
	"bytes"
	"testing"
)

func TestBytesDecodeCopies(t *testing.T) {
	stored := []byte("cached payload")

	out, err := Bytes{}.Decode(stored)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(out, stored) {
		t.Fatalf("Decode = %q, want %q", out, stored)
	}

	out[0] = 'X'
	if stored[0] != 'c' {
		t.Fatalf("stored buffer mutated to %q, want unchanged", stored)
	}
}

func TestBytesDecodeNil(t *testing.T) {
	out, err := Bytes{}.Decode(nil)
	if err != nil {
		t.Fatalf("Decode(nil): %v", err)
	}
	if out != nil {
		t.Fatalf("Decode(nil) = %v, want nil", out)
	}
}

func TestBytesEncodeIdentity(t *testing.T) {
	in := []byte("payload")
	out, err := Bytes{}.Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if &out[0] != &in[0] {
		t.Fatal("Encode copied, want identity (cascache copies during Set)")
	}
}
