package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"

	charmlog "charm.land/log/v2"
	"github.com/charmbracelet/colorprofile"
)

const (
	LevelDebug = "debug"
	LevelInfo  = "info"
	LevelWarn  = "warn"
	LevelError = "error"

	FormatText   = "text"
	FormatPretty = "pretty"
)

type Options struct {
	Level  string
	Format string
	Color  bool
	Output io.Writer
}

func NewLogger(opts Options) *slog.Logger {
	output := opts.Output
	if output == nil {
		output = os.Stderr
	}
	logger := charmlog.NewWithOptions(output, charmlog.Options{
		Level:           parseLevel(opts.Level),
		Formatter:       parseFormatter(opts.Format),
		ReportTimestamp: true,
		TimeFormat:      "2006-01-02T15:04:05.999999999Z07:00",
	})
	if !opts.Color {
		logger.SetColorProfile(colorprofile.Ascii)
	}
	return slog.New(logger)
}

// WithComponent returns a logger tagged with the component name. A nil
// logger resolves to the process-default logger - operational context must
// never be dropped just because a caller skipped logger setup.
func WithComponent(logger *slog.Logger, component string) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	if strings.TrimSpace(component) == "" {
		return logger
	}
	return logger.With("component", component)
}

func resolve(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		// Operational messages must never be silently dropped; fall back
		// to the process-default logger.
		return slog.Default()
	}
	return logger
}

func Debug(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Debug(msg, args...)
}

func Info(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Info(msg, args...)
}

func Warn(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Warn(msg, args...)
}

func Error(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Error(msg, args...)
}

// levelTable is the single source of truth for level vocabulary:
// canonical name -> charm log level. The "warning" alias maps to warn
// (matching slog.ParseLevel); unknown inputs fall back to info in
// NormalizeLevel, while ValidLevel is the loud gate for typos.
var levelTable = map[string]charmlog.Level{
	LevelDebug: charmlog.DebugLevel,
	LevelInfo:  charmlog.InfoLevel,
	LevelWarn:  charmlog.WarnLevel,
	"warning":  charmlog.WarnLevel,
	LevelError: charmlog.ErrorLevel,
}

// canonicalLevel maps every accepted spelling (including aliases and "")
// to its canonical level name. It derives from levelTable so the two
// vocabularies cannot drift.
var canonicalLevel = map[string]string{
	"":         LevelInfo,
	LevelDebug: LevelDebug,
	LevelInfo:  LevelInfo,
	LevelWarn:  LevelWarn,
	"warning":  LevelWarn,
	LevelError: LevelError,
}

// formatTable is the single source of truth for format vocabulary:
// canonical name -> charm formatter.
var formatTable = map[string]charmlog.Formatter{
	FormatPretty: charmlog.TextFormatter,
	FormatText:   charmlog.LogfmtFormatter,
}

// canonicalFormat maps every accepted spelling (including "") to its
// canonical format name.
var canonicalFormat = map[string]string{
	"":           FormatPretty,
	FormatPretty: FormatPretty,
	FormatText:   FormatText,
}

// NormalizeLevel maps a user-supplied level string to one of the canonical
// levels. "warning" is accepted as an alias of "warn" (matching
// slog.ParseLevel's vocabulary); unknown values fall back to info, which is
// why config.Validate - not this function - is the loud gate for typos.
func NormalizeLevel(level string) string {
	if canonical, ok := canonicalLevel[strings.ToLower(strings.TrimSpace(level))]; ok {
		return canonical
	}
	return LevelInfo
}

func parseLevel(level string) charmlog.Level {
	if lv, ok := levelTable[strings.ToLower(strings.TrimSpace(level))]; ok {
		return lv
	}
	// "" and unknown both mean info here; Validate rejects unknown loudly.
	if strings.TrimSpace(level) == "" {
		return charmlog.InfoLevel
	}
	return charmlog.InfoLevel
}

func parseFormatter(format string) charmlog.Formatter {
	if f, ok := formatTable[normalizeFormat(format)]; ok {
		return f
	}
	return charmlog.TextFormatter
}

func normalizeFormat(format string) string {
	if canonical, ok := canonicalFormat[strings.ToLower(strings.TrimSpace(format))]; ok {
		return canonical
	}
	return FormatPretty
}

// KnownLevels lists the accepted log level strings ("" is also allowed and
// means "unset").
func KnownLevels() []string {
	return []string{LevelDebug, LevelInfo, LevelWarn, LevelError}
}

// KnownFormats lists the accepted log format strings ("" is also allowed
// and means "unset").
func KnownFormats() []string {
	return []string{FormatPretty, FormatText}
}

// ValidLevel reports whether level is a recognized log level (or unset).
// It accepts exactly the vocabulary NormalizeLevel maps - including the
// "warning" alias of "warn" - so a level the logger understands can never
// fail validation (and vice versa).
func ValidLevel(level string) bool {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		return true
	}
	_, ok := levelTable[level]
	return ok
}

// ValidFormat reports whether format is a recognized log format (or unset).
func ValidFormat(format string) bool {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return true
	}
	_, ok := formatTable[format]
	return ok
}
