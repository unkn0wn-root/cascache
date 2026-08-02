package backend

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestNewFenceIsValidAndUnique(t *testing.T) {
	const count = 10_000

	seen := make(map[Fence]struct{}, count)
	for range count {
		f := NewFence()
		if !f.Valid() {
			t.Fatal("NewFence returned the invalid zero fence")
		}
		if _, dup := seen[f]; dup {
			t.Fatalf("NewFence repeated %v; fences must never be reused", f)
		}
		seen[f] = struct{}{}
	}
}

func TestNewFenceConcurrentIsUnique(t *testing.T) {
	const (
		goroutines = 16
		perRoutine = 1_000
	)

	all := make([][]Fence, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fences := make([]Fence, perRoutine)
			for j := range fences {
				fences[j] = NewFence()
			}
			all[i] = fences
		}()
	}
	wg.Wait()

	seen := make(map[Fence]struct{}, goroutines*perRoutine)
	for _, fences := range all {
		for _, f := range fences {
			if !f.Valid() {
				t.Fatal("NewFence returned the invalid zero fence")
			}
			if _, dup := seen[f]; dup {
				t.Fatalf("concurrent NewFence repeated %v", f)
			}
			seen[f] = struct{}{}
		}
	}
}

func TestZeroFenceIsInvalid(t *testing.T) {
	var zero Fence
	if zero.Valid() {
		t.Fatal("the zero Fence must be invalid: it is what tells a missing fence from a present one")
	}
	if !zero.Equal(Fence{}) {
		t.Fatal("zero fences must compare equal")
	}
	if got := NewFence(); got.Equal(zero) {
		t.Fatal("NewFence must never return the zero fence")
	}
}

func TestFenceBinaryRoundTrip(t *testing.T) {
	f := NewFence()

	b := f.Bytes()
	if len(b) != FenceSize {
		t.Fatalf("Bytes returned %d bytes, want %d", len(b), FenceSize)
	}
	got, err := ParseFence(b)
	if err != nil {
		t.Fatalf("ParseFence(%x): %v", b, err)
	}
	if !got.Equal(f) {
		t.Fatalf("ParseFence round trip = %v, want %v", got, f)
	}
}

func TestFenceTextRoundTrip(t *testing.T) {
	f := NewFence()

	s := f.String()
	if len(s) != fenceTextSize {
		t.Fatalf("String returned %d characters, want %d", len(s), fenceTextSize)
	}
	got, err := ParseFenceText(s)
	if err != nil {
		t.Fatalf("ParseFenceText(%q): %v", s, err)
	}
	if !got.Equal(f) {
		t.Fatalf("ParseFenceText round trip = %v, want %v", got, f)
	}
}

func TestParseFenceRejectsBadInput(t *testing.T) {
	valid := NewFence().Bytes()

	cases := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"short", valid[:FenceSize-1]},
		{"long", append(append([]byte{}, valid...), 0)},
		{"zero token", make([]byte, FenceSize)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseFence(tc.in); !errors.Is(err, ErrInvalidFence) {
				t.Fatalf("ParseFence(%x) error = %v, want ErrInvalidFence", tc.in, err)
			}
		})
	}
}

func TestParseFenceTextRejectsBadInput(t *testing.T) {
	valid := NewFence().String()

	cases := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"short", valid[:len(valid)-1]},
		{"long", valid + "0"},
		{"not hex", strings.Repeat("z", fenceTextSize)},
		{"zero token", strings.Repeat("0", fenceTextSize)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseFenceText(tc.in); !errors.Is(err, ErrInvalidFence) {
				t.Fatalf("ParseFenceText(%q) error = %v, want ErrInvalidFence", tc.in, err)
			}
		})
	}
}

func TestFenceAppendDoesNotOverwrite(t *testing.T) {
	f := NewFence()
	prefix := []byte("prefix")

	binary := f.AppendBinary(append([]byte{}, prefix...))
	if string(binary[:len(prefix)]) != string(prefix) {
		t.Fatalf("AppendBinary overwrote dst: %q", binary)
	}
	if got, err := ParseFence(binary[len(prefix):]); err != nil || !got.Equal(f) {
		t.Fatalf("AppendBinary payload = %v, %v; want %v", got, err, f)
	}

	text := f.AppendText(append([]byte{}, prefix...))
	if string(text[:len(prefix)]) != string(prefix) {
		t.Fatalf("AppendText overwrote dst: %q", text)
	}
	if got, err := ParseFenceText(string(text[len(prefix):])); err != nil || !got.Equal(f) {
		t.Fatalf("AppendText payload = %v, %v; want %v", got, err, f)
	}
}

func TestFenceMarshalRoundTrip(t *testing.T) {
	type envelope struct {
		Fence Fence `json:"fence"`
	}

	want := envelope{Fence: NewFence()}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got envelope
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal(%s): %v", raw, err)
	}
	if !got.Fence.Equal(want.Fence) {
		t.Fatalf("JSON round trip = %v, want %v", got.Fence, want.Fence)
	}

	bin, err := want.Fence.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var back Fence
	if err := back.UnmarshalBinary(bin); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if !back.Equal(want.Fence) {
		t.Fatalf("binary round trip = %v, want %v", back, want.Fence)
	}
}

func TestFenceMarshalRejectsZero(t *testing.T) {
	var zero Fence

	if _, err := zero.MarshalText(); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("MarshalText of zero fence error = %v, want ErrInvalidFence", err)
	}
	if _, err := zero.MarshalBinary(); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("MarshalBinary of zero fence error = %v, want ErrInvalidFence", err)
	}

	var f Fence
	if err := f.UnmarshalText([]byte(strings.Repeat("0", fenceTextSize))); !errors.Is(err, ErrInvalidFence) {
		t.Fatalf("UnmarshalText of zero token error = %v, want ErrInvalidFence", err)
	}
}

func BenchmarkNewFence(b *testing.B) {
	for range b.N {
		_ = NewFence()
	}
}
