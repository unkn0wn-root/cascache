package sloghooks

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync/atomic"

	"github.com/unkn0wn-root/cascache"
)

type Options struct {
	// Sampling to avoid floods; 0/1 = log all.
	SelfHealEvery   uint64
	BulkRejectEvery uint64
	// Optional key redactor. Defaults to SHA-256 prefix.
	Redact func(string) string
}

type Hooks struct {
	l    *slog.Logger
	opts Options

	selfHealCtr   atomic.Uint64
	bulkRejectCtr atomic.Uint64
}

var _ cascache.Hooks = (*Hooks)(nil)

func New(l *slog.Logger, opts Options) *Hooks {
	return &Hooks{l: l, opts: opts}
}

func (h *Hooks) redact(k string) string {
	if h.opts.Redact != nil {
		return h.opts.Redact(k)
	}
	sum := sha256.Sum256([]byte(k))
	return hex.EncodeToString(sum[:8])
}

func sample(n uint64, ctr *atomic.Uint64) bool {
	if n == 0 || n == 1 {
		return true
	}
	return ctr.Add(1)%n == 0
}

func (h *Hooks) SelfHealSingle(storageKey, reason string) {
	if h.l == nil || !sample(h.opts.SelfHealEvery, &h.selfHealCtr) {
		return
	}
	h.l.Debug("cascache.self_heal_single",
		"key", h.redact(storageKey),
		"reason", reason)
}

func (h *Hooks) BulkRejected(ns string, requested int, reason string) {
	if h.l == nil || !sample(h.opts.BulkRejectEvery, &h.bulkRejectCtr) {
		return
	}
	h.l.Info("cascache.bulk_rejected",
		"ns", ns,
		"requested", requested,
		"reason", reason)
}

func (h *Hooks) ProviderSetRejected(storageKey string, isBulk bool) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.provider_set_rejected",
		"key", h.redact(storageKey),
		"is_bulk", isBulk)
}

func (h *Hooks) GenSnapshotError(count int, err error) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.gen_snapshot_error",
		"count", count,
		"err", err)
}

func (h *Hooks) GenBumpError(storageKey string, err error) {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.gen_bump_error",
		"key", h.redact(storageKey),
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

func (h *Hooks) LocalGenWithBulk() {
	if h.l == nil {
		return
	}
	h.l.Warn("cascache.local_gen_with_bulk",
		"msg", "bulk enabled with local genstore; stale bulks possible in multi-replica")
}
