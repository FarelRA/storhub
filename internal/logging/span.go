package logging

import (
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

// Start logs Debug "<op> start" with the given identifying attrs.
func Start(logger *slog.Logger, op string, args ...any) {
	resolve(logger).Debug(op+" start", args...)
}

// Finish logs Error "<op> failed" with elapsed plus err, or Debug
// "<op> complete" with elapsed. Args keep the tree-wide key order: op
// attrs first, then elapsed, then err.
func Finish(logger *slog.Logger, op string, started time.Time, err error, args ...any) {
	args = append(args, "elapsed", time.Since(started))
	if err != nil {
		args = append(args, "err", err)
		resolve(logger).Error(op+" failed", args...)
		return
	}
	resolve(logger).Debug(op+" complete", args...)
}
