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
	// LevelDebug enables debug logs.
	LevelDebug = "debug"
	// LevelInfo enables info logs.
	LevelInfo = "info"
	// LevelWarn enables warning logs.
	LevelWarn = "warn"
	// LevelError enables error logs.
	LevelError = "error"

	// FormatText selects plain text log output.
	FormatText = "text"
	// FormatPretty selects human-friendly log output.
	FormatPretty = "pretty"
)

// Options configures the logger: level, format, color, and output.
type Options struct {
	Level  string
	Format string
	Color  bool
	Output io.Writer
}

// NewLogger builds a structured logger from the options.
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

// Debug logs a debug message through the logger.
func Debug(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Debug(msg, args...)
}

// Info logs an info message through the logger.
func Info(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Info(msg, args...)
}

// Warn logs a warning message through the logger.
func Warn(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Warn(msg, args...)
}

// Error logs an error message through the logger.
func Error(logger *slog.Logger, msg string, args ...any) {
	resolve(logger).Error(msg, args...)
}

// levelVocab is the single source of truth for level vocabulary: each
// canonical name, its charm log level, and its accepted aliases ("warning"
// maps to warn, matching slog.ParseLevel). levelTable and canonicalLevel
// below derive from it so the vocabularies cannot drift.
var levelVocab = []struct {
	canonical string
	level     charmlog.Level
	aliases   []string
}{
	{LevelDebug, charmlog.DebugLevel, nil},
	{LevelInfo, charmlog.InfoLevel, nil},
	{LevelWarn, charmlog.WarnLevel, []string{"warning"}},
	{LevelError, charmlog.ErrorLevel, nil},
}

// levelTable maps every accepted level spelling to its charm log level.
var levelTable = func() map[string]charmlog.Level {
	m := make(map[string]charmlog.Level, len(levelVocab)+1)
	for _, v := range levelVocab {
		m[v.canonical] = v.level
		for _, a := range v.aliases {
			m[a] = v.level
		}
	}
	return m
}()

// canonicalLevel maps every accepted spelling (including aliases and "")
// to its canonical level name. It derives from levelVocab so the two
// vocabularies cannot drift.
var canonicalLevel = func() map[string]string {
	m := map[string]string{"": LevelInfo}
	for _, v := range levelVocab {
		m[v.canonical] = v.canonical
		for _, a := range v.aliases {
			m[a] = v.canonical
		}
	}
	return m
}()

// formatVocab is the single source of truth for format vocabulary, shaped
// like levelVocab so formatTable and canonicalFormat derive from it.
var formatVocab = []struct {
	canonical string
	formatter charmlog.Formatter
	aliases   []string
}{
	{FormatPretty, charmlog.TextFormatter, nil},
	{FormatText, charmlog.LogfmtFormatter, nil},
}

// formatTable maps every accepted format spelling to its charm formatter.
var formatTable = func() map[string]charmlog.Formatter {
	m := make(map[string]charmlog.Formatter, len(formatVocab))
	for _, v := range formatVocab {
		m[v.canonical] = v.formatter
		for _, a := range v.aliases {
			m[a] = v.formatter
		}
	}
	return m
}()

// canonicalFormat maps every accepted spelling (including "") to its
// canonical format name. It derives from formatVocab.
var canonicalFormat = func() map[string]string {
	m := map[string]string{"": FormatPretty}
	for _, v := range formatVocab {
		m[v.canonical] = v.canonical
		for _, a := range v.aliases {
			m[a] = v.canonical
		}
	}
	return m
}()

// NormalizeLevel maps a user-supplied level string to one of the canonical
// levels. "warning" is accepted as an alias of "warn" (matching
// slog.ParseLevel's vocabulary); unknown values fall back to info. That
// fallback is silent by design: Config.WithDefaults preserves unknown
// values untouched so config.Validate, not this function, is the loud gate
// for typos.
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
	// Unknown (and "") means info here; unknown values never arrive through
	// Config, where WithDefaults preserves them so Validate rejects loudly.
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
