package backend

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

const (
	// FenceSize is the fixed binary size of a fence.
	FenceSize     = 16
	fenceTextSize = FenceSize * 2
	// Buffer entropy to avoid a system call for each fence.
	fenceBlockSize = 4096
)

// ErrInvalidFence reports a missing, zero, or malformed fence.
var ErrInvalidFence = errors.New("cascache/backend: invalid fence")

// Fence is an opaque token that identifies the current version of a key. It can
// only be tested for equality. The zero value is invalid, and generated fences
// are not reused. A missing fence therefore causes a miss instead of making an
// old value current again.
type Fence struct {
	token [FenceSize]byte
}

// Serve tokens from buffered entropy.
type fenceSource struct {
	block [fenceBlockSize]byte
	left  []byte
}

var fenceSources = sync.Pool{New: func() any { return new(fenceSource) }}

// NewFence returns a fresh, unguessable fence.
//
// It panics if the operating system cannot supply entropy; there is no safe
// fallback for a version token.
func NewFence() Fence {
	src := fenceSources.Get().(*fenceSource)
	defer fenceSources.Put(src)

	for {
		if len(src.left) < FenceSize {
			if _, err := rand.Read(src.block[:]); err != nil {
				src.left = nil
				panic(fmt.Errorf("cascache/backend: read entropy: %w", err))
			}
			src.left = src.block[:]
		}

		var f Fence
		copy(f.token[:], src.left)
		src.left = src.left[FenceSize:]

		// Never return the invalid zero fence.
		if f.Valid() {
			return f
		}
	}
}

// ParseFence reads the binary form written by [Fence.Bytes].
func ParseFence(b []byte) (Fence, error) {
	if len(b) != FenceSize {
		return Fence{}, fmt.Errorf("%w: length %d", ErrInvalidFence, len(b))
	}
	var f Fence
	copy(f.token[:], b)
	if !f.Valid() {
		return Fence{}, fmt.Errorf("%w: zero token", ErrInvalidFence)
	}
	return f, nil
}

// ParseFenceText reads the hexadecimal form written by [Fence.String].
func ParseFenceText(s string) (Fence, error) {
	if len(s) != fenceTextSize {
		return Fence{}, fmt.Errorf("%w: text length %d", ErrInvalidFence, len(s))
	}
	var b [FenceSize]byte
	if _, err := hex.Decode(b[:], []byte(s)); err != nil {
		return Fence{}, fmt.Errorf("%w: %w", ErrInvalidFence, err)
	}
	return ParseFence(b[:])
}

// Valid reports whether f can be stored and compared. The zero Fence is not.
func (f Fence) Valid() bool { return f != Fence{} }

// Equal reports whether f and other are the same fence.
func (f Fence) Equal(other Fence) bool { return f == other }

// AppendBinary appends the binary form of f to dst.
func (f Fence) AppendBinary(dst []byte) []byte { return append(dst, f.token[:]...) }

// Bytes returns the binary storage form of f.
func (f Fence) Bytes() []byte { return f.AppendBinary(nil) }

// AppendText appends the hexadecimal form of f to dst.
func (f Fence) AppendText(dst []byte) []byte {
	var buf [fenceTextSize]byte
	hex.Encode(buf[:], f.token[:])
	return append(dst, buf[:]...)
}

// String returns the hexadecimal form of f.
func (f Fence) String() string {
	var buf [fenceTextSize]byte
	hex.Encode(buf[:], f.token[:])
	return string(buf[:])
}

func (f Fence) MarshalText() ([]byte, error) {
	if !f.Valid() {
		return nil, ErrInvalidFence
	}
	return f.AppendText(nil), nil
}

func (f *Fence) UnmarshalText(b []byte) error {
	parsed, err := ParseFenceText(string(b))
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}

func (f Fence) MarshalBinary() ([]byte, error) {
	if !f.Valid() {
		return nil, ErrInvalidFence
	}
	return f.Bytes(), nil
}

func (f *Fence) UnmarshalBinary(b []byte) error {
	parsed, err := ParseFence(b)
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}
