// Package provider defines stores for cached byte slices. Stores may evict,
// expire, or reject values; backend fences decide whether a value is current.
package provider

import (
	"context"
	"time"
)

// Store reads and writes byte slices without interpreting them.
type Store interface {
	// Get returns the stored value. A missing key is (nil, false, nil).
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Set stores a value. Admission rejection returns (false, nil); other
	// failures return an error. cost may be ignored. A nonpositive ttl means no
	// expiry when supported.
	Set(ctx context.Context, key string, value []byte, cost int64, ttl time.Duration) (ok bool, err error)

	// Del removes a key. Removing a key that is not there is not an error.
	Del(ctx context.Context, key string) error
}
