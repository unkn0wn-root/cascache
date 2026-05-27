package codec

import "bytes"

// Bytes is an identity codec for []byte values. Encode/Decode return the input
// unchanged, so callers must treat byte slices as immutable after handing them
// to the cache and after receiving them from the cache.
type Bytes struct{}

func (Bytes) Encode(b []byte) ([]byte, error) { return b, nil }
func (Bytes) Decode(b []byte) ([]byte, error) { return b, nil }

// BytesClone is a defensive []byte codec. Encode and Decode return copies via
// bytes.Clone, so caller mutations cannot alias codec output and cached bytes
// cannot be mutated through a returned slice. Like Bytes, nil clones to nil and
// an empty slice clones to empty; unlike Bytes, the result never shares backing
// storage with its input.
type BytesClone struct{}

func (BytesClone) Encode(b []byte) ([]byte, error) { return bytes.Clone(b), nil }
func (BytesClone) Decode(b []byte) ([]byte, error) { return bytes.Clone(b), nil }

// String is a trivial codec for Go string values. Encode converts to []byte,
// and Decode converts back to string. By convention this assumes UTF-8 and
// performs no validation.
type String struct{}

func (String) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (String) Decode(b []byte) (string, error) { return string(b), nil }
