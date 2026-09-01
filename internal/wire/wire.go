package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"github.com/unkn0wn-root/cascache/v4/backend"
)

const (
	magic = "CASC"

	// version must change when the layout changes incompatibly.
	version    byte = 4
	kindSingle byte = 1
)

const (
	offMagic   = 0
	offVersion = offMagic + len(magic)        // 4
	offKind    = offVersion + 1               // 5
	offFence   = offKind + 1                  // 6
	offVLen    = offFence + backend.FenceSize // 22
	offCRC     = offVLen + 4                  // 26

	identityLen = offFence   // 6
	headerLen   = offCRC + 4 // 30
)

// maxPayload is uint64 so the length check is also correct on 32-bit platforms,
// where a uint32 length does not fit in an int.
const maxPayload uint64 = math.MaxUint32

var (
	ErrInvalidFrame      = errors.New("not a valid frame")
	ErrUnsupportedFormat = errors.New("unsupported frame format")
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Encode builds a frame stamped with fence. Version 4 uses a 30-byte,
// big-endian header:
//
//	magic "CASC" (4) | version (1) | kind (1) | fence (16) | payload length (4) | CRC32C (4) | payload
//
// The checksum covers the fence and payload.
func Encode(fence backend.Fence, payload []byte) ([]byte, error) {
	if !fence.Valid() {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFrame, backend.ErrInvalidFence)
	}
	if uint64(len(payload)) > maxPayload || len(payload) > math.MaxInt-headerLen {
		return nil, fmt.Errorf("payload of %d bytes exceeds the frame limit of %d", len(payload), maxPayload)
	}

	b := make([]byte, headerLen+len(payload))
	copy(b[offMagic:offVersion], magic)
	b[offVersion] = version
	b[offKind] = kindSingle
	_ = fence.AppendBinary(b[:offFence])
	binary.BigEndian.PutUint32(b[offVLen:offCRC], uint32(len(payload)))
	copy(b[headerLen:], payload)
	binary.BigEndian.PutUint32(b[offCRC:headerLen], checksum(b[offFence:offVLen], payload))
	return b, nil
}

// Decode validates a frame and returns its fence and a read-only view of its
// payload. ErrInvalidFrame means the entry is safe to delete;
// ErrUnsupportedFormat may have been written by another supported version and
// must not be deleted.
func Decode(b []byte) (backend.Fence, []byte, error) {
	if len(b) < identityLen || string(b[offMagic:offVersion]) != magic {
		return backend.Fence{}, nil, ErrInvalidFrame
	}
	if b[offVersion] != version || b[offKind] != kindSingle {
		return backend.Fence{}, nil, ErrUnsupportedFormat
	}

	// headerLen applies only after the version check.
	if len(b) < headerLen {
		return backend.Fence{}, nil, ErrInvalidFrame
	}
	head, body := b[:headerLen], b[headerLen:]

	if uint64(binary.BigEndian.Uint32(head[offVLen:offCRC])) != uint64(len(body)) {
		return backend.Fence{}, nil, ErrInvalidFrame
	}

	raw := head[offFence:offVLen]
	if checksum(raw, body) != binary.BigEndian.Uint32(head[offCRC:headerLen]) {
		return backend.Fence{}, nil, ErrInvalidFrame
	}

	fence, err := backend.ParseFence(raw)
	if err != nil {
		return backend.Fence{}, nil, ErrInvalidFrame
	}
	return fence, body, nil
}

func checksum(fence, payload []byte) uint32 {
	return crc32.Update(crc32.Update(0, crcTable, fence), crcTable, payload)
}
