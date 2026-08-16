package codec

import "bytes"

// Bytes is a codec for []byte values, useful when your value type is already
// a raw byte slice and you only need cascache's wire framing and validation.
//
// Encode returns the input unchanged - cascache copies it into its wire frame
// during Set so the caller's slice is never retained. Decode returns a copy:
// wire decoding is zero-copy and providers may return buffers whose backing
// arrays are shared with the cache, so handing out an alias would let callers
// corrupt cached state by mutating the result.
type Bytes struct{}

func (Bytes) Encode(b []byte) ([]byte, error) { return b, nil }
func (Bytes) Decode(b []byte) ([]byte, error) { return bytes.Clone(b), nil }

// String is a trivial codec for Go string values. Encode converts to []byte,
// and Decode converts back to string. By convention this assumes UTF-8 and
// performs no validation.
type String struct{}

func (String) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (String) Decode(b []byte) (string, error) { return string(b), nil }

var (
	_ Codec[[]byte] = Bytes{}
	_ Codec[string] = String{}
)
