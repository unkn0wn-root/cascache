package cascache

import vp "github.com/unkn0wn-root/cascache/v3/version"

// Version is the per-key freshness token returned by the cache.
// Callers should treat it as an compare-only value and pass it back
// unchanged to versioned write APIs.
//
// The zero value is the missing-version token.
type Version struct {
	fence  vp.Fence
	exists bool
}

func (v Version) Equal(other Version) bool {
	if v.exists != other.exists {
		return false
	}
	if !v.exists {
		return true
	}
	return v.fence.Equal(other.fence)
}

func (v Version) IsMissing() bool {
	return !v.exists
}

func (v Version) snapshot() vp.Snapshot {
	if !v.exists {
		return vp.Snapshot{}
	}
	return vp.Snapshot{
		Fence:  v.fence,
		Exists: true,
	}
}

func versionFromSnapshot(s vp.Snapshot) Version {
	if !s.Exists {
		return Version{}
	}
	return Version{
		fence:  s.Fence,
		exists: true,
	}
}
