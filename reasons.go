package cascache

// SelfHealReason classifies why a single entry was deleted on read.
type SelfHealReason string

const (
	// single-entry wire envelope was invalid.
	SelfHealReasonCorrupt SelfHealReason = "corrupt"
	// stored generation no longer matched the current generation.
	SelfHealReasonGenMismatch SelfHealReason = "gen_mismatch"
	// payload could not be decoded by the configured codec.
	SelfHealReasonValueDecode SelfHealReason = "value_decode"
	// an authoritative read guard rejected the entry.
	SelfHealReasonReadGuardReject SelfHealReason = "read_guard_reject"
	// an authoritative read guard errored, so the entry was conservatively dropped.
	SelfHealReasonReadGuardError SelfHealReason = "read_guard_error"
)

// BulkRejectReason classifies why a bulk entry was rejected.
type BulkRejectReason string

const (
	// at least one bulk payload could not be decoded by the codec.
	BulkRejectReasonValueDecode BulkRejectReason = "value_decode"
	// bulk entry was missing members or contained stale generations.
	BulkRejectReasonInvalidOrStale BulkRejectReason = "invalid_or_stale"
	// bulk wire envelope was invalid.
	BulkRejectReasonDecodeError BulkRejectReason = "decode_error"
	// caller omitted at least one observed generation.
	BulkRejectReasonMissingObservedGen BulkRejectReason = "missing_observed_gen"
	// current generations could not be loaded.
	BulkRejectReasonGenSnapshotError BulkRejectReason = "gen_snapshot_error"
	// one observed generation no longer matched.
	BulkRejectReasonGenMismatch BulkRejectReason = "gen_mismatch"
	// an authoritative read guard rejected at least one requested member.
	BulkRejectReasonReadGuardReject BulkRejectReason = "read_guard_reject"
	// an authoritative read guard failed, so the bulk was conservatively dropped.
	BulkRejectReasonReadGuardError BulkRejectReason = "read_guard_error"
)
