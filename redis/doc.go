// Package redis contains the Redis backend for cascache.
//
// Most callers should use New.
//
// The lower-level constructors exist for custom topologies:
//   - NewVersionStore when values stay outside Redis but version state must be shared.
//   - NewProvider when values live in Redis but the cache is wired manually.
//   - NewKeyMutator when manual wiring still wants the Redis-native single-key
//     compare-and-write and invalidate path.
package redis
