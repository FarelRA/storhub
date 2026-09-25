package logging

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type capturedRecord struct {
	msg   string
	attrs []slog.Attr
}

type captureHandler struct {
	minLevel slog.Level
	records  []capturedRecord
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.minLevel }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	var attrs []slog.Attr
	r.Attrs(func(a slog.Attr) bool { attrs = append(attrs, a); return true })
	h.records = append(h.records, capturedRecord{r.Message, attrs})
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func capture(level slog.Level) (*slog.Logger, *captureHandler) {
	h := &captureHandler{minLevel: level}
	return slog.New(h), h
}

func attrKeys(attrs []slog.Attr) []string {
	keys := make([]string, len(attrs))
	for i, a := range attrs {
		keys[i] = a.Key
	}
	return keys
}

func TestStartLogsOpStartWithCallerAttrsFirst(t *testing.T) {
	t.Parallel()
	logger, h := capture(slog.LevelDebug)
	Start(logger, "node-get", "path", "/a.txt")
	if len(h.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(h.records))
	}
	rec := h.records[0]
	if rec.msg != "node-get start" {
		t.Fatalf("message = %q, want %q", rec.msg, "node-get start")
	}
	keys := attrKeys(rec.attrs)
	if len(keys) != 1 || keys[0] != "path" {
		t.Fatalf("caller attrs mangled: %v", keys)
	}
	if rec.attrs[0].Value.String() != "/a.txt" {
		t.Fatalf("caller attr value = %v, want /a.txt", rec.attrs[0].Value)
	}
}

func TestFinishSuccessAppendsElapsedLast(t *testing.T) {
	t.Parallel()
	logger, h := capture(slog.LevelDebug)
	Finish(logger, "node-get", time.Now().Add(-time.Second), nil, "path", "/a.txt")
	if len(h.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(h.records))
	}
	rec := h.records[0]
	if rec.msg != "node-get complete" {
		t.Fatalf("message = %q, want %q", rec.msg, "node-get complete")
	}
	keys := attrKeys(rec.attrs)
	if len(keys) != 2 || keys[0] != "path" || keys[1] != "elapsed" {
		t.Fatalf("key order = %v, want [path elapsed]", keys)
	}
}

func TestFinishFailureAppendsElapsedThenErrLast(t *testing.T) {
	t.Parallel()
	logger, h := capture(slog.LevelDebug)
	boom := errors.New("boom")
	Finish(logger, "node-get", time.Now().Add(-time.Second), boom, "path", "/a.txt")
	if len(h.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(h.records))
	}
	rec := h.records[0]
	if rec.msg != "node-get failed" {
		t.Fatalf("message = %q, want %q", rec.msg, "node-get failed")
	}
	keys := attrKeys(rec.attrs)
	if len(keys) != 3 || keys[0] != "path" || keys[1] != "elapsed" || keys[2] != "err" {
		t.Fatalf("key order = %v, want [path elapsed err]", keys)
	}
	if rec.attrs[2].Value.Any() != boom {
		t.Fatalf("err attr = %v, want the passed error", rec.attrs[2].Value.Any())
	}
}

func TestEnabledReportsHandlerLevel(t *testing.T) {
	t.Parallel()
	logger, _ := capture(slog.LevelWarn)
	if Enabled(logger, slog.LevelDebug) || Enabled(logger, slog.LevelInfo) {
		t.Fatal("debug/info must be disabled at warn level")
	}
	if !Enabled(logger, slog.LevelWarn) || !Enabled(logger, slog.LevelError) {
		t.Fatal("warn/error must be enabled at warn level")
	}
	if Enabled(nil, slog.LevelError) != true {
		t.Fatal("nil logger resolves to default; error must be enabled")
	}
}
