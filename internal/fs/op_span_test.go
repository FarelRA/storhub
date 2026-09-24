package fs

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

// spanProbeBackend reuses the shared test backend but serves a caller-owned
// logger so span tests can pin which lines each level emits.
type spanProbeBackend struct {
	*testBackend
	logger *slog.Logger
}

func (b *spanProbeBackend) Logger() *slog.Logger { return b.logger }

func newSpanProbeService(level slog.Level, buf *bytes.Buffer) *Service {
	backend := &spanProbeBackend{
		testBackend: newTestBackend(900),
		logger:      slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: level})),
	}
	return NewService(backend)
}

func adminCtx() context.Context {
	return WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
}

// A successful op at the default level must leave the log sink untouched:
// no start line, no completion line.
func TestWithOpSilentOnSuccessAtDefaultLevel(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	svc := newSpanProbeService(slog.LevelWarn, &buf)
	ctx := adminCtx()
	if _, err := svc.CreateFileContext(ctx, "demo", "quiet.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := svc.StatPathContext(ctx, "demo", "quiet.txt"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("success at Warn level must log nothing, got %q", buf.String())
	}
}

// A failed op at the default level must still surface its Error line,
// without any start line alongside it.
func TestWithOpFailureVisibleAtDefaultLevel(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	svc := newSpanProbeService(slog.LevelWarn, &buf)
	if _, err := svc.StatPathContext(adminCtx(), "demo", "missing.txt"); err == nil {
		t.Fatal("expected missing path error")
	}
	out := buf.String()
	if !strings.Contains(out, "failed") || !strings.Contains(out, "stat-path") {
		t.Fatalf("failure at Warn level must log the failed line, got %q", out)
	}
	if strings.Contains(out, "start") || strings.Contains(out, "complete") {
		t.Fatalf("failure at Warn level must not log start/complete lines, got %q", out)
	}
}

// At Debug the full span (start plus completion or failure) is emitted.
func TestWithOpFullSpanAtDebugLevel(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	svc := newSpanProbeService(slog.LevelDebug, &buf)
	ctx := adminCtx()
	if _, err := svc.CreateFileContext(ctx, "demo", "loud.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := svc.StatPathContext(ctx, "demo", "missing.txt"); err == nil {
		t.Fatal("expected missing path error")
	}
	out := buf.String()
	for _, want := range []string{"create start", "create complete", "stat-path start", "stat-path failed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("debug span must contain %q, got %q", want, out)
		}
	}
}

// The success path at the default level must stay within a tight allocation
// bound: the Enabled gate runs before the message concat, the slog record,
// and the finish-path slice growth. Args are built once outside the measured
// loop so the bound covers the funnel only.
func TestWithOpSuccessAllocBound(t *testing.T) {
	var buf bytes.Buffer
	svc := newSpanProbeService(slog.LevelWarn, &buf)
	args := []any{"path", "docs/probe.txt", "size", int64(7)}
	probe := func() {
		if err := svc.withOp("demo", "probe", false, args, func() error { return nil }); err != nil {
			t.Fatalf("probe: %v", err)
		}
	}
	probe()
	allocs := testing.AllocsPerRun(200, probe)
	if buf.Len() != 0 {
		t.Fatalf("probe at Warn level must log nothing, got %q", buf.String())
	}
	// Measured 0 allocs/op on go1.26 linux/amd64; the unguarded funnel
	// measured 3 (message concat, finish-path slice growth, and the slog
	// record handoff on top). The bound of 1 leaves headroom for
	// toolchain wobble while still catching a return to unguarded
	// logging.
	if allocs > 1 {
		t.Fatalf("success withOp at Warn level allocates %.1f/op, want <= 1", allocs)
	}
	t.Logf("success withOp at Warn level: %.1f allocs/op", allocs)
}
