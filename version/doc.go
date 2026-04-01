// Package version contains the lower-level primitives behind cascache's
// authoritative version state.
//
// Most applications should use cascache.Version through the main cache API.
// This package is mainly for Store implementations and advanced integrations
// that need to work with Fence, Snapshot, or CacheKey values directly.
package version
