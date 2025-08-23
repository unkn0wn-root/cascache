package cascache

// Fields is a minimal structured field map for logs.
type Fields map[string]any

// Logger is a tiny leveled logger. Provide an adapter around logging stack.
// If Logger is nil in Options, logging is disabled.
type Logger interface {
	Debug(msg string, f Fields)
	Info(msg string, f Fields)
	Warn(msg string, f Fields)
	Error(msg string, f Fields)
}

type NopLogger struct{}

func (NopLogger) Debug(string, Fields) {}
func (NopLogger) Info(string, Fields)  {}
func (NopLogger) Warn(string, Fields)  {}
func (NopLogger) Error(string, Fields) {}
