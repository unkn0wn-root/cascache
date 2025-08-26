package codec

import (
	"github.com/fxamacker/cbor/v2"
)

// CBOR is a Codec that serializes values using fxamacker/cbor.
// The zero value is NOT ready to use. Construct with NewCBOR or MustCBOR.
//
// Use deterministic=true for canonical encoding (RFC 8949 Core Deterministic)
// when you need byte-for-byte stable outputs (e.g., hashing/content addressing).
// Otherwise PreferredUnsortedEncOptions are used (sensible defaults).
// Time values are encoded as RFC3339Nano for stable, human-readable timestamps.
type CBOR[V any] struct {
	enc cbor.EncMode
	dec cbor.DecMode
}

var _ Codec[struct{}] = CBOR[struct{}]{}

// NewCBOR constructs a CBOR codec.
//   - Deterministic is true, uses CoreDetEncOptions (RFC 8949).
//   - Otherwise uses PreferredUnsortedEncOptions (smaller/faster defaults).
//
// Also sets time encoding to RFC3339Nano.
func NewCBOR[V any](deterministic bool) (CBOR[V], error) {
	var eo cbor.EncOptions
	if deterministic {
		eo = cbor.CoreDetEncOptions()
	} else {
		eo = cbor.PreferredUnsortedEncOptions()
	}
	eo.Time = cbor.TimeRFC3339Nano

	em, err := eo.EncMode()
	if err != nil {
		return CBOR[V]{}, err
	}
	dm, err := (cbor.DecOptions{}).DecMode()
	if err != nil {
		return CBOR[V]{}, err
	}
	return CBOR[V]{enc: em, dec: dm}, nil
}

// MustCBOR is like NewCBOR but panics on error.
// Should not use for prod just handy for package-level variables in tests/examples.
func MustCBOR[V any](deterministic bool) CBOR[V] {
	c, err := NewCBOR[V](deterministic)
	if err != nil {
		panic(err)
	}
	return c
}

// Encode encodes v as CBOR using the configured EncMode.
func (c CBOR[V]) Encode(v V) ([]byte, error) {
	return c.enc.Marshal(v)
}

// Decode decodes b into a V using the configured DecMode.
func (c CBOR[V]) Decode(b []byte) (V, error) {
	var v V
	err := c.dec.Unmarshal(b, &v)
	return v, err
}
