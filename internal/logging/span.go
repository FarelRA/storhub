package logging

import (
	"context"
	"log/slog"
	"time"
)

// Operation span convention for the whole tree: every operation logs
// Debug "<op> start" with identifying attrs via Start, then Error
// "<op> failed" with elapsed plus err or Debug "<op> complete" with
// elapsed via Finish. No hand-rolled Debug/Error "<op>
// start/complete/failed" triples: they evade the CI span gates. Thin
// per-layer withOp helpers must call Start/Finish, not re-spell the gate.
//
// Levels: Info is only for process-lifetime milestones (serve listening,
// mount listening, shutdown initiated/complete). Warn is only for
// recoverable conditions; a terminal upstream failure splits the message:
// "<op> failed" at Error (5xx) vs "<op> rejected" at Warn
// (4xx/terminal-retryable), never one message at two levels.
//
// Ops are kebab-case identifiers in the message ("node-get", never
// "node get" or "node_get"); there is no separate op attr. New ops reuse
// an existing token where one fits and add a token only for a new verb.
//
// Key order is op attrs, then elapsed, then err, with err last; status
// and body stay in the op-attrs region. Finish appends elapsed then err
// so callers keep this order by construction.
//
// Elapsed uses time.Since(started); code under test clocks uses its
// injected clock instead. All logs go to stderr via slog, never stdout.
// Hot paths guard with Enabled BEFORE building args so disabled levels
// cost zero heap.

// Enabled reports whether logger enables level. Hot paths call it BEFORE
// building args so disabled levels cost zero heap.
//
// It evaluates against context.Background: no handler built in this tree
// (NewLogger) is context-sensitive, so a request-scoped context cannot
// change the answer. If a context-sensitive handler is ever introduced,
// this must take a ctx parameter instead.
func Enabled(logger *slog.Logger, level slog.Level) bool {
	return resolve(logger).Enabled(context.Background(), level)
}

// Start logs Debug "<op> start" with the given identifying attrs. It
// performs no enabled-check itself: hot paths must guard with Enabled
// BEFORE building args, since variadic boxing plus string concat cost heap
// even when the level is off. The helpers stay dumb on purpose; an
// unguarded-call grep gate in CI owns enforcement.
func Start(logger *slog.Logger, op string, args ...any) {
	resolve(logger).Debug(op+" start", args...)
}

// Finish logs Error "<op> failed" with elapsed plus err, or Debug
// "<op> complete" with elapsed. Args keep the tree-wide key order: op
// attrs first, then elapsed, then err (err last). New call sites name
// the slices args, the start time started, and the failure err. Like
// Start it performs no enabled-check itself: callers pass failures
// through unguarded so the Error line is reachable at the default level,
// and guard only the success path with Enabled before building args.
func Finish(logger *slog.Logger, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		resolve(logger).Error(op+" failed", args...)
		return
	}
	resolve(logger).Debug(op+" complete", args...)
}
