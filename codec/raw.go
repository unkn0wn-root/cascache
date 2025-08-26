package codec

// Bytes is an identity codec for []byte values. Encode/Decode return the
// input unchanged. Useful when your value type is already a raw byte slice
// and you only need cascache's wire framing and validation.
type Bytes struct{}

func (Bytes) Encode(b []byte) ([]byte, error) { return b, nil }
func (Bytes) Decode(b []byte) ([]byte, error) { return b, nil }

// String is a trivial codec for Go string values. Encode converts to []byte,
// and Decode converts back to string. By convention this assumes UTF-8 and
// performs no validation.
type String struct{}

func (String) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (String) Decode(b []byte) (string, error) { return string(b), nil }
