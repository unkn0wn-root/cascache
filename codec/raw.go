package codec

import "bytes"

// Bytes is a codec for byte slices. Encode returns its input; cascache copies it
// into the stored frame. Decode returns a copy so callers cannot modify memory
// owned by a provider.
type Bytes struct{}

func (Bytes) Encode(b []byte) ([]byte, error) { return b, nil }
func (Bytes) Decode(b []byte) ([]byte, error) { return bytes.Clone(b), nil }

// String encodes strings as bytes without validating UTF-8.
type String struct{}

func (String) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (String) Decode(b []byte) (string, error) { return string(b), nil }

var (
	_ Codec[[]byte] = Bytes{}
	_ Codec[string] = String{}
)
