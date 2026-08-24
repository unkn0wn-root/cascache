package cascache

import (
	"math/rand/v2"
	"time"
)

// JitterTTL returns a [TTLFunc] that randomly shortens ttl. fraction is clamped
// to [0, 1]. With a positive floor below ttl, jitter is taken from the range
// between them; otherwise it is taken from ttl. A non-positive ttl or fraction
// leaves ttl unchanged.
func JitterTTL(ttl, floor time.Duration, fraction float64) TTLFunc {
	fixed := func() (time.Duration, error) { return ttl, nil }
	if ttl <= 0 || fraction <= 0 {
		return fixed
	}
	if fraction > 1 {
		fraction = 1
	}

	window := ttl
	if floor > 0 && floor < ttl {
		window = ttl - floor
	}

	// Include the upper bound so the result can equal floor.
	span := time.Duration(float64(window) * fraction)
	if span <= 0 {
		return fixed
	}

	return func() (time.Duration, error) {
		return ttl - rand.N(span+1), nil
	}
}
