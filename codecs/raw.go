// codecs/raw.go
package codecs

type BytesCodec struct{}

func (BytesCodec) Encode(b []byte) ([]byte, error) { return b, nil }
func (BytesCodec) Decode(b []byte) ([]byte, error) { return b, nil }

// For string payloads
type StringCodec struct{}

func (StringCodec) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (StringCodec) Decode(b []byte) (string, error) { return string(b), nil }
