package cli

import (
	"fmt"
	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/storhub"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// failFastRateMaxWait is the one-shot rate policy: any negative wait opts
// into fail-fast downstream (see resolveRateConfig). Spelled in
// PatienceUnit so the value reads in the unit system, not as a magic
// duration: the magnitude is irrelevant, only the sign.
const failFastRateMaxWait = -1 * storcfg.PatienceUnit

// envUnset mirrors the envOr reader below: blank counts as unset.
func envUnset(key string) bool {
	return strings.TrimSpace(os.Getenv(key)) == ""
}

// newHubConfig builds the hub configuration every CLI constructor shares.
// longRunning selects the rate-limit policy documented on applyRateEnv:
// interactive/daemon surfaces (mount, rest, serve) pause up to the reset,
// one-shot commands fail fast. Log knobs, the journal location, and the
// chunksize validation are applied for ALL commands - a surface that silently
// ignores --loglevel is a bug waiting to be filed.
// Invalid values fail loud: a typo'd STORHUB_* var is a usage error, never
// a silent fallback (matching config.Validate's philosophy and the strict
// STORHUB_LOG_COLOR path in LoadFile).
func newHubConfig(apiBase string, chunkSize int64, public bool, log logSettings, longRunning bool) (storcfg.Config, error) {
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	if chunkSize != 0 {
		normalized, err := normalizeCLIChunkSize(chunkSize)
		if err != nil {
			return cfg, err
		}
		cfg.ChunkSize = normalized
	}
	cfg.CreatePublicRepo = public
	cfg.LogLevel = log.level
	cfg.LogFormat = log.format
	cfg.LogColor = log.color
	// The write-ahead journal lives beneath the cache base: crash-safe
	// replay is exactly what the mutating CLI surfaces need, so the CLI
	// opts in by default (config.go documents this defaulting).
	cfg.JournalDir = filepath.Join(storcfg.CacheBase(), "journal")
	if err := applyRateEnv(&cfg, longRunning); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// applyRateEnv layers the rate-governor environment variables onto a hub
// config. One-shot commands fail fast on rate limits by default (a wait
// cannot help a script), while long-running commands - rest, mount, serve
// - may pause up to the documented reset before giving up.
// Every variable is strict: blanks fall back, garbage fails loud with the
// key named. Zero is passed through untouched: the library owns the
// zero-means-default mapping downstream (resolveRateConfig), so the CLI
// never reinterprets a value it did not mint.
func applyRateEnv(cfg *storcfg.Config, longRunning bool) error {
	defaultMaxWait := failFastRateMaxWait
	if longRunning {
		defaultMaxWait = 180 * storcfg.PatienceUnit
	}
	var err error
	if cfg.RateReserve, err = parseEnvInt64(storcfg.EnvRateReserve, cfg.RateReserve); err != nil {
		return err
	}
	if cfg.RateMaxWait, err = parseEnvDuration(storcfg.EnvRateMaxWait, defaultMaxWait); err != nil {
		return err
	}
	if cfg.RatePointsPerMin, err = parseEnvInt64(storcfg.EnvRatePointsPerMin, cfg.RatePointsPerMin); err != nil {
		return err
	}
	if cfg.RateContentPerMin, err = parseEnvInt64(storcfg.EnvRateContentPerMin, cfg.RateContentPerMin); err != nil {
		return err
	}
	if cfg.MaxConcurrentRequests, err = parseEnvInt64(storcfg.EnvMaxConcurrent, cfg.MaxConcurrentRequests); err != nil {
		return err
	}
	var consecutive int64
	if consecutive, err = parseEnvInt64(storcfg.EnvMaxConsecutiveFailures, int64(cfg.MaxConsecutiveCommitFailures)); err != nil {
		return err
	}
	cfg.MaxConsecutiveCommitFailures = int(consecutive)
	if cfg.TransferThroughput, err = parseEnvInt64(storcfg.EnvTransferThroughput, cfg.TransferThroughput); err != nil {
		return err
	}
	return nil
}

// envOr is the single generic env reader: empty means fallback, parse
// errors fail loud with the key named. The typed wrappers below exist so
// call sites read as policy, not parse plumbing.
func envOr[T any](key string, fallback T, parse func(string) (T, error)) (T, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := parse(value)
	if err != nil {
		var zero T
		return zero, &usageError{fmt.Errorf("invalid %s=%q: %w", key, value, err)}
	}
	return parsed, nil
}

func parseEnvInt64(key string, fallback int64) (int64, error) {
	return envOr(key, fallback, func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})
}

func parseEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	return envOr(key, fallback, time.ParseDuration)
}

// resolveToken prefers the explicit --token value and falls back to
// $GITHUB_TOKEN. The token is never rendered into help output.
func resolveToken(flagValue string) string {
	return flagOrEnv(flagValue, "GITHUB_TOKEN")
}

// flagOrEnv is the single flag-over-env resolver: an explicitly set flag
// wins, otherwise the trimmed environment value applies, otherwise "".
// sessionHandle, shareSigningKey, serveAuthOptions, and resolveToken all
// spell the same precedence through this helper.
func flagOrEnv(flagValue, envKey string) string {
	if strings.TrimSpace(flagValue) != "" {
		return flagValue
	}
	return strings.TrimSpace(os.Getenv(envKey))
}

func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func parseEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		warnEnvParse(key, value, err)
		return fallback
	}
	return parsed
}

// strictEnvBool reports whether the named env var holds a valid bool when
// set: used at startup (PersistentPreRunE) so a typo'd STORHUB_LOG_COLOR
// fails loud like every other strict STORHUB_* var, instead of warning
// and running at the wrong setting.
func strictEnvBool(key string) error {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}
	if _, err := strconv.ParseBool(value); err != nil {
		return &usageError{fmt.Errorf("invalid %s=%q: %w", key, value, err)}
	}
	return nil
}

// warnEnvParse reports an invalid STORHUB_* value that fell back to its
// default. Only the pre-App log-color path still warns (defaultLogSettings
// cannot return an error); every hub-construction env read fails loud.
func warnEnvParse(key, value string, err error) {
	warnfWithAttrs(warnSink(), "invalid env value; using default",
		[]any{"key", key, "value", value, "err", err})
}

// minCLIChunkSize is kept as the documented floor for help text: values
// below it are a usage error (see normalizeCLIChunkSize), not a clamp.
const minCLIChunkSize int64 = 32 * 1024 * 1024

// normalizeCLIChunkSize maps a --chunksize flag value to the size actually
// used. Zero means "unset" (the default applies); negatives are usage
// errors at the call site (app_file.go). Values below the 32 MiB floor
// fail loud instead of clamping: a silent upscale runs the upload at a
// size the operator never asked for, contradicting Validate's loud-reject
// philosophy. Values above the GitHub release-asset ceiling clamp down
// through chunking.NormalizedSize - the single owner of the ceiling - so
// the chunker's plan and the uploader's windows can never disagree.
func normalizeCLIChunkSize(size int64) (int64, error) {
	if size <= 0 {
		return size, nil
	}
	if size < minCLIChunkSize {
		return 0, &usageError{fmt.Errorf("--chunksize must be at least %d (32 MiB), got %d", minCLIChunkSize, size)}
	}
	clamped, adjusted := chunking.NormalizedSize(size)
	if adjusted {
		warnfWithAttrs(warnSink(), "--chunksize outside range; using fallback",
			[]any{"key", "chunksize", "value", size, "min", minCLIChunkSize, "max", chunking.MaxReleaseAssetSize, "fallback", clamped})
	}
	return clamped, nil
}
