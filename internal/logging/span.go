package logging

import (
	"context"
	"log/slog"
	"time"
)

// Operation span convention for the whole tree: every operation logs Debug
// "<op> start" with identifying attrs, then Error "<op> failed" with
// elapsed plus err or Debug "<op> complete" with elapsed. Info is only for
// low-volume milestones, Warn is only for recoverable conditions. Key order
// is op attrs, then elapsed, then err. All logs go to stderr via slog,
// never stdout. Hot paths guard BEFORE building args so disabled levels
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
// attrs first, then elapsed, then err. Like Start it performs no
// enabled-check itself: callers pass failures through unguarded so the
// Error line is reachable at the default level, and guard only the
// success path with Enabled before building args.
func Finish(logger *slog.Logger, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		resolve(logger).Error(op+" failed", args...)
		return
	}
	resolve(logger).Debug(op+" complete", args...)
}
