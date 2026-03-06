package codec

import "errors"

var ErrNilInnerCodec = errors.New("cascache/codec: nil inner codec")

var ErrUninitializedCBOR = errors.New("cascache/codec: cbor codec is not initialized")

var ErrUninitializedProtobuf = errors.New("cascache/codec: protobuf codec is not initialized")
