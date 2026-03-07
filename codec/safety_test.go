package codec

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLimitCodecNilInnerReturnsError(t *testing.T) {
	t.Parallel()

	c := LimitCodec[string]{}

	if _, err := c.Encode("x"); !errors.Is(err, ErrNilInnerCodec) {
		t.Fatalf("Encode error mismatch: %v", err)
	}
	if _, err := c.Decode([]byte("x")); !errors.Is(err, ErrNilInnerCodec) {
		t.Fatalf("Decode error mismatch: %v", err)
	}
}

func TestCBORZeroValueReturnsError(t *testing.T) {
	t.Parallel()

	var c CBOR[map[string]string]

	if _, err := c.Encode(map[string]string{"a": "b"}); !errors.Is(err, ErrUninitializedCBOR) {
		t.Fatalf("Encode error mismatch: %v", err)
	}
	if _, err := c.Decode([]byte{0xa0}); !errors.Is(err, ErrUninitializedCBOR) {
		t.Fatalf("Decode error mismatch: %v", err)
	}
}

func TestProtobufZeroValueReturnsError(t *testing.T) {
	t.Parallel()

	var c Protobuf[*emptypb.Empty]

	if _, err := c.Decode(nil); !errors.Is(err, ErrUninitializedProtobuf) {
		t.Fatalf("Decode error mismatch: %v", err)
	}
}

func TestProtobufNilCtorResultReturnsError(t *testing.T) {
	t.Parallel()

	c := NewProtobuf(func() *emptypb.Empty { return nil })

	if _, err := c.Decode(nil); !errors.Is(err, ErrUninitializedProtobuf) {
		t.Fatalf("Decode error mismatch: %v", err)
	}
}
