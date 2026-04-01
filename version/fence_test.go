package version

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestFenceAppendBinaryAppendsCanonical16Bytes(t *testing.T) {
	raw := [tokenSize]byte{
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

	if len(got) != len(prefix)+tokenSize {
		t.Fatalf("AppendBinary len = %d, want %d", len(got), len(prefix)+tokenSize)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("AppendBinary output mismatch: got %x want %x", got, want)
	}
}

func TestFenceTextEncodersStayEquivalent(t *testing.T) {
	nonZeroRaw := [tokenSize]byte{
		1, 2, 3, 4, 5, 6, 7, 8,
		9, 10, 11, 12, 13, 14, 15, 16,
	}
	nonZero, err := ParseFenceBinary(nonZeroRaw[:])
	if err != nil {
		t.Fatalf("ParseFenceBinary error: %v", err)
	}

	cases := []struct {
		name      string
		fence     Fence
		roundTrip bool
	}{
		{name: "zero", fence: Fence{}},
		{name: "non-zero", fence: nonZero, roundTrip: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotString := tc.fence.String()
			gotAppend := string(tc.fence.AppendText(nil))
			if gotString != gotAppend {
				t.Fatalf("String() = %q, want AppendText %q", gotString, gotAppend)
			}

			gotMarshal, err := tc.fence.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText error: %v", err)
			}
			if gotString != string(gotMarshal) {
				t.Fatalf("String() = %q, want MarshalText %q", gotString, gotMarshal)
			}

			if !tc.roundTrip {
				return
			}

			parsed, err := ParseFence(gotString)
			if err != nil {
				t.Fatalf("ParseFence error: %v", err)
			}
			if !parsed.Equal(tc.fence) {
				t.Fatalf("ParseFence(String()) = %v, want %v", parsed, tc.fence)
			}
		})
	}
}

func TestFenceStringMatchesAppendTextAcrossManyValues(t *testing.T) {
	for i := 0; i < 4096; i++ {
		var raw [tokenSize]byte
		for j := range raw {
			raw[j] = byte(i*31 + j*17 + (i >> uint(j%5)))
		}

		f := Fence{token: raw}
		gotString := f.String()
		wantString := string(f.AppendText(nil))
		if gotString != wantString {
			t.Fatalf("case %d: String() = %q, want %q", i, gotString, wantString)
		}

		if len(gotString) != tokenSize*2 {
			t.Fatalf("case %d: String() len = %d, want %d", i, len(gotString), tokenSize*2)
		}
	}
}

func TestNewFenceRetriesZeroToken(t *testing.T) {
	orig := rand.Reader
	t.Cleanup(func() {
		rand.Reader = orig
	})

	rand.Reader = io.MultiReader(
		bytes.NewReader(make([]byte, tokenSize)),
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
