// Package backend defines how cascache stores values and invalidation state.
//
// # How it works
//
// Each cached value is stored with a [Fence]. A read returns the value only if
// its fence is still current. Invalidating a key creates a new fence, which
// makes values stored with the previous fence invalid. Fences have no valid
// zero value and are never reused. If fence state is missing, the cache returns
// a miss instead of an old value.
//
// # Available backends
//
// [Backend] defines the interface. Use [backend/backendtest.TestBackend] to test
// an implementation.
//
// [Local] combines a caller-owned value store with process-local invalidation
// state. [Composite] and [FenceStore] can be used to build another backend. The
// redis subpackage can store values and invalidation state together in Redis,
// or combine local values with invalidation state stored in Redis.
//
// # Storage layout
//
// Backends use [StorageKeys] so implementations sharing a store use the same
// layout.
package backend
