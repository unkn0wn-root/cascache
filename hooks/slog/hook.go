// Package sloghook provides a slog-based implementation of cascache.Hooks.
// It is optional: the core cascache package has no logging dependency.
package sloghook

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync/atomic"

	"github.com/unkn0wn-root/cascache"
	"github.com/unkn0wn-root/cascache/version"
)

// Options configures the slog-based Hooks.
type Options struct {
	// Sampling to avoid log floods; 0 or 1 = log every event.
	SelfHealEvery    uint64
	BatchRejectEvery uint64

	// Optional key redactor. If nil, a short SHA-256 prefix is used.
	Redact func(string) string
}

// Hooks is a slog-backed implementation of cascache.Hooks.
type Hooks struct {
	l    *slog.Logger
	opts Options

	selfHealCtr    atomic.Uint64
	batchRejectCtr atomic.Uint64
}

var _ cascache.Hooks = (*Hooks)(nil)

// New creates a slog-based Hooks.
func New(l *slog.Logger, opts Options) *Hooks {
	return &Hooks{l: l, opts: opts}
}

func (h *Hooks) redact(k string) string {
	if h.opts.Redact != nil {
		return h.opts.Redact(k)
	}
	sum := sha256.Sum256([]byte(k))
	return hex.EncodeToString(sum[:8]) // short prefix
}

func sample(n uint64, ctr *atomic.Uint64) bool {
	if n == 0 || n == 1 {
		return true
	}
	// Log the Nth, 2N-th, ... event.
	return ctr.Add(1)%n == 0
}

func (h *Hooks) SelfHealSingle(storageKey string, reason cascache.SelfHealReason) {
	if h.l == nil || !sample(h.opts.SelfHealEvery, &h.selfHealCtr) {
		return
	}
	h.l.Debug("cascache.self_heal_single",
		"key", h.redact(storageKey),
		"reason", string(reason))
}

func (h *Hooks) BatchRejected(ns string, requested int, reason cascache.BatchRejectReason) {
	if h.l == nil || !sample(h.opts.BatchRejectEvery, &h.batchRejectCtr) {
		return
	}
	h.l.Info("cascache.batch_rejected",
		"ns", ns,
		"requested", requested,
		"reason", string(reason))
}

func (h *Hooks) ProviderSetRejected(storageKey string, isBatch bool) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.provider_set_rejected",
		"key", h.redact(storageKey),
		"is_batch", isBatch)
}

func (h *Hooks) VersionSnapshotError(count int, err error) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.version_snapshot_error",
		"count", count,
		"err", err)
}

func (h *Hooks) VersionCreateError(cacheKey version.CacheKey, err error) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.version_create_error",
		"key", h.redact(cacheKey.String()),
		"err", err)
}

func (h *Hooks) VersionAdvanceError(cacheKey version.CacheKey, err error) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.version_advance_error",
		"key", h.redact(cacheKey.String()),
		"err", err)
}

func (h *Hooks) InvalidateOutage(key string, bumpErr, delErr error) {
	if h.l == nil {
		return
	}
	h.l.Error("cascache.invalidate_outage",
		"key", h.redact(key),
		"bump_err", bumpErr,
		"del_err", delErr)
}

func (h *Hooks) LocalVersionStoreWithBatch() {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.local_version_store_with_batch",
		"msg", "batch enabled with local version store; stale batches possible in multi-replica")
}
