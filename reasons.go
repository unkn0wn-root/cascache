package cascache

// SelfHealReason classifies why a single entry was deleted on read.
type SelfHealReason string

const (
	// single-entry wire envelope was invalid.
	SelfHealReasonCorrupt SelfHealReason = "corrupt"
	// no authoritative version state existed for the key.
	SelfHealReasonVersionMissing SelfHealReason = "version_missing"
	// stored version fence no longer matched the current authoritative fence.
	SelfHealReasonVersionMismatch SelfHealReason = "version_mismatch"
	// payload could not be decoded by the configured codec.
	SelfHealReasonValueDecode SelfHealReason = "value_decode"
	// an authoritative read guard rejected the entry.
	SelfHealReasonReadGuardReject SelfHealReason = "read_guard_reject"
	// an authoritative read guard errored, so the entry was conservatively dropped.
	SelfHealReasonReadGuardError SelfHealReason = "read_guard_error"
)

// BatchRejectReason classifies why a batch entry was rejected.
type BatchRejectReason string

const (
	// at least one batch payload could not be decoded by the codec.
	BatchRejectReasonValueDecode BatchRejectReason = "value_decode"
	// at least one requested member was absent from the stored batch entry.
	BatchRejectReasonIncompleteBatch BatchRejectReason = "incomplete_batch"
	// authoritative version state was missing for at least one requested member.
	BatchRejectReasonVersionMissing BatchRejectReason = "version_missing"
	// at least one requested member no longer matched authoritative version state.
	BatchRejectReasonVersionMismatch BatchRejectReason = "version_mismatch"
	// batch wire envelope was invalid.
	BatchRejectReasonDecodeError BatchRejectReason = "decode_error"
	// an authoritative read guard rejected at least one requested member.
	BatchRejectReasonReadGuardReject BatchRejectReason = "read_guard_reject"
	// an authoritative read guard failed, so the batch was conservatively dropped.
	BatchRejectReasonReadGuardError BatchRejectReason = "read_guard_error"
)
