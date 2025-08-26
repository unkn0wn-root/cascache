package codec

type Bytes struct{}

func (Bytes) Encode(b []byte) ([]byte, error) { return b, nil }
func (Bytes) Decode(b []byte) ([]byte, error) { return b, nil }

// For string payloads
type String struct{}

func (String) Encode(s string) ([]byte, error) { return []byte(s), nil }
func (String) Decode(b []byte) (string, error) { return string(b), nil }
