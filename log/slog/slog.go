//go:build go1.21

package slog

import (
	"context"
	stdslog "log/slog"

	"github.com/unkn0wn-root/cascache"
)

var _ cascache.Logger = Logger{}

type Logger struct{ L *stdslog.Logger }

func (s Logger) Debug(msg string, f cascache.Fields) {
	s.L.LogAttrs(context.Background(), stdslog.LevelDebug, msg, attrs(f)...)
}
func (s Logger) Info(msg string, f cascache.Fields) {
	s.L.LogAttrs(context.Background(), stdslog.LevelInfo, msg, attrs(f)...)
}
func (s Logger) Warn(msg string, f cascache.Fields) {
	s.L.LogAttrs(context.Background(), stdslog.LevelWarn, msg, attrs(f)...)
}
func (s Logger) Error(msg string, f cascache.Fields) {
	s.L.LogAttrs(context.Background(), stdslog.LevelError, msg, attrs(f)...)
}

func attrs(f cascache.Fields) []stdslog.Attr {
	if len(f) == 0 {
		return nil
	}
	out := make([]stdslog.Attr, 0, len(f))
	for k, v := range f {
		out = append(out, stdslog.Any(k, v))
	}
	return out
}
