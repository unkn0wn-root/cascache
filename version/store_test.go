package version

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestNewFenceRetriesZeroToken(t *testing.T) {
	orig := rand.Reader
	t.Cleanup(func() {
		rand.Reader = orig
	})

	rand.Reader = io.MultiReader(
		bytes.NewReader(make([]byte, fenceTokenSize)),
		bytes.NewReader([]byte{
			1, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
		}),
	)

	f, err := NewFence()
	if err != nil {
		t.Fatalf("NewFence error: %v", err)
	}
	if f == (Fence{}) {
		t.Fatalf("NewFence returned the zero fence")
	}
	if _, err := ParseFenceBinary(f.token[:]); err != nil {
		t.Fatalf("NewFence returned a parse-invalid fence: %v", err)
	}
}
