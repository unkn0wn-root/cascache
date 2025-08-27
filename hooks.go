package cascache

// Hooks lightweight callbacks for high-signal events.
// Implementations MUST be cheap and non-blocking.
// The cache calls them on hot paths.
type Hooks interface {
	// A single entry was deleted by the cache on read.
	// reason ∈ {"corrupt", "gen_mismatch", "value_decode"}
	SelfHealSingle(storageKey, reason string)

	// A bulk read path was rejected and fell back to singles.
	// reason ∈ {"decode_error", "invalid_or_stale", "snapshot_error"}
	BulkRejected(namespace string, requested int, reason string)

	// Provider returned ok=false on Set (backpressure/eviction).
	ProviderSetRejected(storageKey string, isBulk bool)

	// GenStore errors (snapshot or bump).
	// count is number of keys involved (1 for Snapshot/Bump, N for SnapshotMany).
	GenSnapshotError(count int, err error)
	GenBumpError(storageKey string, err error)

	// Both gen bump and delete failed during Invalidate (likely backend outage).
	InvalidateOutage(key string, bumpErr, delErr error)

	// Bulk is enabled with a local GenStore (stale bulks possible across replicas).
	LocalGenWithBulk()
}

// NopHooks is the default no-op
type NopHooks struct{}

func (NopHooks) SelfHealSingle(string, string)         {}
func (NopHooks) BulkRejected(string, int, string)      {}
func (NopHooks) ProviderSetRejected(string, bool)      {}
func (NopHooks) GenSnapshotError(int, error)           {}
func (NopHooks) GenBumpError(string, error)            {}
func (NopHooks) InvalidateOutage(string, error, error) {}
func (NopHooks) LocalGenWithBulk()                     {}
