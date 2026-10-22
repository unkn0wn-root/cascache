// Package cascache caches values until their invalidation state changes.
//
// Each value carries a token called a fence. Invalidating a key changes its
// fence, so all copies of the old value become unreadable.
//
// Cache methods can be called from multiple goroutines. Custom backends, stores,
// codecs, and callbacks must handle this too.
//
// [Cache.Load] returns a cached value or calls the loader after a miss.
// Concurrent calls for the same key share one loader run. A later load cannot
// undo an invalidation. Without invalidation, old values can stay until expiry.
//
// When data changes, write it to the source first, then call [Cache.Invalidate].
// Retry it if it returns an error.
//
// For manual writes, take a [Cache.Snapshot] before reading the source, then
// pass it to [Cache.Set]. An invalidation between those calls blocks the write.
//
// Fences have no valid zero and are never reused. A missing fence means a miss.
// A counter cannot tell a lost record from a key that was never invalidated.
//
// Use backend.NewLocal for one process, redis.New to keep values and fences
// together in Redis, or redis.NewShared for local values with shared fences.
// Redis reads must reach the primary: a replica can return an old value with
// its matching old fence. The Redis backend rejects known replica reads unless
// explicitly allowed.
//
// The cache never closes what you pass to it. Close the backend if it has a
// Close method, and close the store or Redis client yourself.
package cascache
