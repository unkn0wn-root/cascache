// Package sloghook logs cascache events with log/slog.
package sloghook

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/unkn0wn-root/cascache/v4"
)

// Options configure the returned observer.
type Options struct {
	// RejectEvery logs every Nth rejected entry. Zero logs all entries.
	RejectEvery uint64

	// Redact rewrites logged keys. Nil logs a short SHA-256 prefix.
	Redact func(string) string
}

// New returns an observer that logs to l. A nil logger returns nil, which the
// cache accepts and skips.
func New(l *slog.Logger, opts Options) cascache.Observer {
	if l == nil {
		return nil
	}
	return &observer{log: l, opts: opts}
}

type observer struct {
	log  *slog.Logger
	opts Options

	rejects atomic.Uint64
}

func (o *observer) Observe(e cascache.Event) {
	switch e.Type {
	case cascache.EventEntryRejected:
		o.rejected(e)
	case cascache.EventStoreRejected:
		o.log.Warn("cascache.store_rejected",
			"op", e.Op.String(),
			"key", o.redact(e.Key))
	case cascache.EventCleanupFailed:
		o.log.Warn("cascache.cleanup_failed",
			"op", e.Op.String(),
			"key", o.redact(e.Key),
			"err", e.Err)
	case cascache.EventOperationFailed:
		o.log.Error("cascache.operation_failed",
			"op", e.Op.String(),
			"key", o.redact(e.Key),
			"err", e.Err)
	case cascache.EventLoaderPanic:
		o.panicked(e)
	default:
		// Ignore event types added by newer versions.
	}
}

func (o *observer) rejected(e cascache.Event) {
	if n := o.opts.RejectEvery; n > 1 && o.rejects.Add(1)%n != 0 {
		return
	}

	// Missing fences usually mean the fence TTL is too short.
	level := slog.LevelDebug
	if e.Reason == cascache.RejectStateMissing {
		level = slog.LevelWarn
	}
	o.log.Log(context.Background(), level, "cascache.entry_rejected",
		"op", e.Op.String(),
		"key", o.redact(e.Key),
		"reason", e.Reason.String(),
		"err", e.Err)
}

func (o *observer) panicked(e cascache.Event) {
	attrs := []any{"key", o.redact(e.Key), "err", e.Err}

	var panicErr *cascache.PanicError
	if errors.As(e.Err, &panicErr) {
		attrs = []any{
			"key", o.redact(e.Key),
			"panic", panicErr.Value,
			"stack", string(panicErr.Stack),
		}
	}
	o.log.Error("cascache.loader_panic", attrs...)
}

func (o *observer) redact(key string) string {
	if o.opts.Redact != nil {
		return o.opts.Redact(key)
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}
