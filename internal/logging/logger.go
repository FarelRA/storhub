package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	charmlog "github.com/charmbracelet/log"
	"github.com/muesli/termenv"
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
		logger.SetColorProfile(termenv.Ascii)
	}
	return slog.New(logger)
}

func WithComponent(logger *slog.Logger, component string) *slog.Logger {
	if logger == nil {
		logger = NewLogger(Options{})
	}
	if strings.TrimSpace(component) == "" {
		return logger
	}
	return logger.With("component", component)
}

func Debug(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Debug(msg, args...)
	}
}

func Info(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Info(msg, args...)
	}
}

func Warn(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Warn(msg, args...)
	}
}

func Error(logger *slog.Logger, msg string, args ...any) {
	if logger != nil {
		logger.Error(msg, args...)
	}
}

func Enabled(logger *slog.Logger, level slog.Level) bool {
	if logger == nil {
		return false
	}
	return logger.Enabled(context.Background(), level)
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
