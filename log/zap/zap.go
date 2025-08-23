package zap

import (
	"github.com/unkn0wn-root/cascache"
	"go.uber.org/zap"
)

type ZapLogger struct{ L *zap.Logger }

func (z ZapLogger) Debug(msg string, f cascache.Fields) { z.L.Debug(msg, zf(f)...) }
func (z ZapLogger) Info(msg string, f cascache.Fields)  { z.L.Info(msg, zf(f)...) }
func (z ZapLogger) Warn(msg string, f cascache.Fields)  { z.L.Warn(msg, zf(f)...) }
func (z ZapLogger) Error(msg string, f cascache.Fields) { z.L.Error(msg, zf(f)...) }

func zf(f cascache.Fields) []zap.Field {
	if len(f) == 0 {
		return nil
	}
	out := make([]zap.Field, 0, len(f))
	for k, v := range f {
		out = append(out, zap.Any(k, v))
	}
	return out
}
