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

func NormalizeLevel(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", LevelInfo:
		return LevelInfo
	case LevelDebug:
		return LevelDebug
	case LevelWarn, "warning":
		return LevelWarn
	case LevelError:
		return LevelError
	default:
		return LevelInfo
	}
}

func NormalizeFormat(format string) string {
	return normalizeFormat(format)
}

func parseLevel(level string) charmlog.Level {
	switch NormalizeLevel(level) {
	case LevelDebug:
		return charmlog.DebugLevel
	case LevelWarn:
		return charmlog.WarnLevel
	case LevelError:
		return charmlog.ErrorLevel
	default:
		return charmlog.InfoLevel
	}
}

func parseFormatter(format string) charmlog.Formatter {
	// normalizeFormat already maps unknown values to the default, so the
	// switch is exhaustive over its outputs.
	switch normalizeFormat(format) {
	case FormatText:
		return charmlog.LogfmtFormatter
	default:
		return charmlog.TextFormatter
	}
}

func normalizeFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", FormatPretty:
		return FormatPretty
	case FormatText:
		return FormatText
	default:
		return FormatPretty
	}
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
func ValidLevel(level string) bool {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		return true
	}
	for _, known := range KnownLevels() {
		if level == known {
			return true
		}
	}
	return false
}

// ValidFormat reports whether format is a recognized log format (or unset).
func ValidFormat(format string) bool {
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return true
	}
	for _, known := range KnownFormats() {
		if format == known {
			return true
		}
	}
	return false
}
