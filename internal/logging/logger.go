package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
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
	return slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{
		Level: parseLevel(opts.Level),
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			switch attr.Key {
			case slog.TimeKey:
				if attr.Value.Kind() == slog.KindTime {
					attr.Value = slog.StringValue(attr.Value.Time().UTC().Format(time.RFC3339Nano))
				}
			case slog.LevelKey:
				level := strings.ToUpper(attr.Value.String())
				if normalizeFormat(opts.Format) == FormatPretty {
					attr.Value = slog.StringValue(colorizeLevel(level, opts.Color))
				} else {
					attr.Value = slog.StringValue(level)
				}
			}
			return attr
		},
	}))
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

func parseLevel(level string) slog.Level {
	switch NormalizeLevel(level) {
	case LevelDebug:
		return slog.LevelDebug
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
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

func colorizeLevel(level string, enabled bool) string {
	if !enabled {
		return level
	}
	switch level {
	case "DEBUG":
		return "\x1b[36mDEBUG\x1b[0m"
	case "INFO":
		return "\x1b[32mINFO\x1b[0m"
	case "WARN":
		return "\x1b[33mWARN\x1b[0m"
	case "ERROR":
		return "\x1b[31mERROR\x1b[0m"
	default:
		return level
	}
}
