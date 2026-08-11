package sloghook_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/unkn0wn-root/cascache/v4"
	sloghook "github.com/unkn0wn-root/cascache/v4/hooks/slog"
)

func newLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})
	return slog.New(handler), &buf
}

func lines(t testing.TB, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestNewWithoutALoggerIsNil(t *testing.T) {
	if obs := sloghook.New(nil, sloghook.Options{}); obs != nil {
		t.Fatalf("New(nil) = %v, want nil", obs)
	}
}

func TestLogLevels(t *testing.T) {
	cases := []struct {
		name  string
		event cascache.Event
		msg   string
		level string
	}{
		{
			name: "retired entry",
			event: cascache.Event{
				Type:   cascache.EventEntryRejected,
				Op:     cascache.OpGet,
				Reason: cascache.RejectRetired,
			},
			msg:   "cascache.entry_rejected",
			level: "DEBUG",
		},
		{
			name: "missing fence",
			event: cascache.Event{
				Type:   cascache.EventEntryRejected,
				Op:     cascache.OpGet,
				Reason: cascache.RejectStateMissing,
			},
			msg:   "cascache.entry_rejected",
			level: "WARN",
		},
		{
			name:  "store rejected",
			event: cascache.Event{Type: cascache.EventStoreRejected, Op: cascache.OpSet},
			msg:   "cascache.store_rejected",
			level: "WARN",
		},
		{
			name:  "cleanup failed",
			event: cascache.Event{Type: cascache.EventCleanupFailed, Op: cascache.OpInvalidate},
			msg:   "cascache.cleanup_failed",
			level: "WARN",
		},
		{
			name:  "operation failed",
			event: cascache.Event{Type: cascache.EventOperationFailed, Op: cascache.OpGet},
			msg:   "cascache.operation_failed",
			level: "ERROR",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, buf := newLogger(slog.LevelDebug)
			sloghook.New(logger, sloghook.Options{}).Observe(tc.event)

			got := lines(t, buf)
			if len(got) != 1 {
				t.Fatalf("logged %d lines, want 1: %s", len(got), buf)
			}
			if got[0]["msg"] != tc.msg {
				t.Fatalf("msg = %v, want %v", got[0]["msg"], tc.msg)
			}
			if got[0]["level"] != tc.level {
				t.Fatalf("level = %v, want %v", got[0]["level"], tc.level)
			}
		})
	}
}

func TestLoaderPanicLogsTheStack(t *testing.T) {
	logger, buf := newLogger(slog.LevelDebug)

	sloghook.New(logger, sloghook.Options{}).Observe(cascache.Event{
		Type: cascache.EventLoaderPanic,
		Op:   cascache.OpLoad,
		Key:  "42",
		Err:  &cascache.PanicError{Value: "boom", Stack: []byte("goroutine 1")},
	})

	got := lines(t, buf)
	if len(got) != 1 {
		t.Fatalf("logged %d lines, want 1", len(got))
	}
	if got[0]["panic"] != "boom" {
		t.Fatalf("panic = %v, want boom", got[0]["panic"])
	}
	if stack, _ := got[0]["stack"].(string); !strings.Contains(stack, "goroutine 1") {
		t.Fatalf("stack = %v, want the recorded stack", got[0]["stack"])
	}
}

func TestKeysAreRedactedByDefault(t *testing.T) {
	logger, buf := newLogger(slog.LevelDebug)

	sloghook.New(logger, sloghook.Options{}).Observe(cascache.Event{
		Type: cascache.EventOperationFailed,
		Op:   cascache.OpGet,
		Key:  "personal-identifier",
	})

	if strings.Contains(buf.String(), "personal-identifier") {
		t.Fatalf("the raw key was logged: %s", buf)
	}
	if got := lines(t, buf); got[0]["key"] == "" {
		t.Fatal("no key was logged at all")
	}
}

func TestRedactIsUsed(t *testing.T) {
	logger, buf := newLogger(slog.LevelDebug)

	sloghook.New(logger, sloghook.Options{
		Redact: func(string) string { return "redacted" },
	}).Observe(cascache.Event{Type: cascache.EventOperationFailed, Key: "secret"})

	if got := lines(t, buf); got[0]["key"] != "redacted" {
		t.Fatalf("key = %v, want redacted", got[0]["key"])
	}
}

func TestRejectEverySamples(t *testing.T) {
	logger, buf := newLogger(slog.LevelDebug)
	obs := sloghook.New(logger, sloghook.Options{RejectEvery: 10})

	for range 100 {
		obs.Observe(cascache.Event{
			Type:   cascache.EventEntryRejected,
			Reason: cascache.RejectRetired,
		})
	}

	if got := len(lines(t, buf)); got != 10 {
		t.Fatalf("logged %d of 100 rejections, want 10", got)
	}
}

func TestUnknownEventTypeIsIgnored(t *testing.T) {
	logger, buf := newLogger(slog.LevelDebug)

	sloghook.New(logger, sloghook.Options{}).Observe(cascache.Event{Type: cascache.EventType(200)})

	if buf.Len() != 0 {
		t.Fatalf("an unknown event type logged: %s", buf)
	}
}
