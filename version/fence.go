package version

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// fenceTokenSize is the canonical binary size of a Fence.
// Important: Changing it is a wire-format change.
// In another words - don't or be careful
const tokenSize = 16

// Fence is the authoritative compare-only freshness token for one key.
//
// Fence values are opaque. Callers and Store implementations may compare
// them for equality and may store/restore them through the canonical text or
// binary helpers below, but must not infer ordering or other semantics from the
// token contents.
//
// For correctness, a live key must receive a fresh fence whenever authoritative
// state is created or bumped. This includes recreation after retention cleanup
// or TTL expiry so old cached values cannot become valid again through ABA.
type Fence struct {
	token [tokenSize]byte
}

func (f Fence) Equal(other Fence) bool {
	return f.token == other.token
}

func (f Fence) String() string {
	// encode into a fixed size stack buffer so String avoids the extra []byte alloc.
	var buf [tokenSize * 2]byte
	return string(f.AppendText(buf[:0]))
}

// AppendBinary appends the canonical fixed-width binary form of f to dst.
// The appended suffix is always exactly fenceTokenSize bytes.
// Callers that encode fixed-layout frames may rely on this width remaining
// stable for the current wire format.
func (f Fence) AppendBinary(dst []byte) []byte {
	return append(dst, f.token[:]...)
}

func (f Fence) AppendText(dst []byte) []byte {
	return hex.AppendEncode(dst, f.token[:])
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (f Fence) MarshalBinary() ([]byte, error) {
	return f.AppendBinary(nil), nil
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (f *Fence) UnmarshalBinary(b []byte) error {
	p, err := ParseFenceBinary(b)
	if err != nil {
		return err
	}
	*f = p
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (f Fence) MarshalText() ([]byte, error) {
	return f.AppendText(nil), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (f *Fence) UnmarshalText(text []byte) error {
	p, err := ParseFence(string(text))
	if err != nil {
		return err
	}
	*f = p
	return nil
}

// ParseFence parses the canonical text form produced by Fence.String.
func ParseFence(raw string) (Fence, error) {
	if len(raw) != hex.EncodedLen(tokenSize) {
		return Fence{}, fmt.Errorf("invalid fence length %d", len(raw))
	}

	b, err := hex.DecodeString(raw)
	if err != nil {
		return Fence{}, fmt.Errorf("decode fence: %w", err)
	}
	return ParseFenceBinary(b)
}

// ParseFenceBinary parses the canonical binary form of a fence.
func ParseFenceBinary(b []byte) (Fence, error) {
	if len(b) != tokenSize {
		return Fence{}, fmt.Errorf("invalid fence length %d", len(b))
	}

	var f Fence
	copy(f.token[:], b)
	if f == (Fence{}) {
		return Fence{}, fmt.Errorf("invalid zero fence")
	}
	return f, nil
}

// NewFence returns a fresh random authoritative fence token.
func NewFence() (Fence, error) {
	var f Fence
	for {
		if _, err := rand.Read(f.token[:]); err != nil {
			return Fence{}, err
		}
		if f != (Fence{}) {
			return f, nil
		}
	}
}
