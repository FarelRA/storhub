package cli

import (
	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	"github.com/FarelRA/storhub/storhub"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// envUnset mirrors the envOr reader below: blank counts as unset.
func envUnset(key string) bool {
	return strings.TrimSpace(os.Getenv(key)) == ""
}

// newHubConfig builds the hub configuration every CLI constructor shares.
// longRunning selects the rate-limit policy documented on applyRateEnv:
// interactive/daemon surfaces (mount, rest, serve) pause up to the reset,
// one-shot commands fail fast. Log knobs, the journal location, and the
// chunksize clamps are applied for ALL commands - a surface that silently
// ignores --loglevel is a bug waiting to be filed.
func newHubConfig(apiBase string, chunkSize int64, public bool, log logSettings, longRunning bool) storcfg.Config {
	cfg := storhub.DefaultConfig()
	if strings.TrimSpace(apiBase) != "" {
		cfg.APIBaseURL = apiBase
	}
	if normalized := normalizeCLIChunkSize(chunkSize); normalized > 0 {
		if normalized != chunkSize {
			warnf("--chunksize %d outside [%d, %d]; using %d",
				chunkSize, minCLIChunkSize, chunking.MaxReleaseAssetSize, normalized)
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
	applyRateEnv(&cfg, longRunning)
	return cfg
}

// applyRateEnv layers the rate-governor environment variables onto a hub
// config. One-shot commands fail fast on rate limits by default (a wait
// cannot help a script), while long-running commands - rest, mount, serve
// - may pause up to the documented reset before giving up.
func applyRateEnv(cfg *storcfg.Config, longRunning bool) {
	defaultMaxWait := -1 * time.Second // fail-fast sentinel, not a duration: do not unit-derive
	if longRunning {
		defaultMaxWait = 180 * storcfg.PatienceUnit
	}
	cfg.RateReserve = parseEnvInt64("STORHUB_RATE_RESERVE", cfg.RateReserve)
	cfg.RateMaxWait = parseEnvDuration("STORHUB_RATE_MAX_WAIT", defaultMaxWait)
	if _, set := os.LookupEnv("STORHUB_RATE_MAX_WAIT"); set && cfg.RateMaxWait == 0 {
		warnf("STORHUB_RATE_MAX_WAIT=0 means \"not configured\": the library default (15m) applies; use a negative duration such as -1s for fail-fast")
	}
	cfg.RatePointsPerMin = parseEnvInt64("STORHUB_RATE_POINTS_PER_MIN", cfg.RatePointsPerMin)
	cfg.RateContentPerMin = parseEnvInt64("STORHUB_RATE_CONTENT_PER_MIN", cfg.RateContentPerMin)
	cfg.MaxConcurrentRequests = parseEnvInt64("STORHUB_MAX_CONCURRENT", cfg.MaxConcurrentRequests)
	cfg.MaxConsecutiveCommitFailures = int(parseEnvInt64("STORHUB_MAX_CONSECUTIVE_FAILURES", int64(cfg.MaxConsecutiveCommitFailures)))
	cfg.TransferThroughput = parseEnvInt64("STORHUB_TRANSFER_THROUGHPUT", cfg.TransferThroughput)
	for _, neg := range []struct {
		key   string
		value int64
	}{
		{"STORHUB_RATE_POINTS_PER_MIN", cfg.RatePointsPerMin},
		{"STORHUB_RATE_CONTENT_PER_MIN", cfg.RateContentPerMin},
		{"STORHUB_MAX_CONCURRENT", cfg.MaxConcurrentRequests},
	} {
		if neg.value < 0 {
			warnf("%s=%d is negative; the rate governor silently replaces it with the library default", neg.key, neg.value)
		}
	}
}

// envOr is the single generic env reader: empty means fallback, parse
// errors warn uniformly via warnEnvParse and fall back. The typed wrappers
// below exist so call sites read as policy, not parse plumbing.
func envOr[T any](key string, fallback T, parse func(string) (T, error)) T {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := parse(value)
	if err != nil {
		warnEnvParse(key, value, err)
		return fallback
	}
	return parsed
}

func parseEnvInt64(key string, fallback int64) int64 {
	return envOr(key, fallback, func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	})
}

func parseEnvDuration(key string, fallback time.Duration) time.Duration {
	return envOr(key, fallback, time.ParseDuration)
}

// resolveToken prefers the explicit --token value and falls back to
// $GITHUB_TOKEN. The token is never rendered into help output.
func resolveToken(flagValue string) string {
	if strings.TrimSpace(flagValue) != "" {
		return flagValue
	}
	return os.Getenv("GITHUB_TOKEN")
}

func envOrDefault(key, fallback string) string {
	return envOr(key, fallback, func(s string) (string, error) { return s, nil })
}

func parseEnvBool(key string, fallback bool) bool {
	return envOr(key, fallback, strconv.ParseBool)
}

// warnEnvParse reports an invalid STORHUB_* value that fell back to its
// default. The fallback preserves behavior; the warning makes the
// misconfiguration visible instead of silent.
func warnEnvParse(key, value string, err error) {
	warnf("invalid %s=%q (%v); using default", key, value, err)
}
