// Package backend defines how cascache stores values and invalidation state.
//
// Values carry a [Fence] and are served only while it matches the current fence.
// Invalidation replaces the fence. A missing fence means a cache miss; zero
// fences are invalid and old fences are never reused.
//
// [Local] keeps fences in one process. The redis subpackage shares fences across
// processes and can also store values. [Composite] pairs a value store with a
// [FenceStore] to build other backends.
//
// Implement [Backend], use [StorageKeys] for the shared key layout, and check
// the implementation with backend/backendtest.TestBackend.
package backend
