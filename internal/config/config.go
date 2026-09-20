// Package config defines StorHub's programmatic configuration surface.
// Values come from embedder code plus the STORHUB_* environment (the CLI
// layers env over Default()); there is no config-file loader - nothing
// here reads a config.yaml, and documentation must not promise one.
package config

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	"github.com/FarelRA/storhub/internal/logging"
)

const (
	defaultAPIBaseURL      = "https://api.github.com"
	defaultAPIVersion      = "2022-11-28"
	defaultRequestTimeout  = 60 * PatienceUnit
	defaultRepoDescription = "StorHub storage project"
	// DefaultChunkSize and DefaultBufferSize are aliases of the chunking
	// package's constants: the GitHub release-asset ceiling has one owner,
	// so a limit change cannot drift between the two spellings.
	DefaultChunkSize = chunking.DefaultChunkSize
	// DefaultBufferSize is the default per-operation transfer buffer size.
	DefaultBufferSize = chunking.DefaultBufferSize
	// MaxBufferSize bounds the per-operation transfer buffer Validate
	// accepts. Buffers are allocated per sync.Pool New, so an unbounded
	// value turns a typo into an instant OOM; 64 MiB is already 64x the
	// default and far past any sane interactive use.
	MaxBufferSize = 64 << 20
	// defaultHistoryWarnObjects is the advisory commit-time threshold
	// Default() ships with; an explicit zero disables the warning.
	defaultHistoryWarnObjects = 5000
	// defaultMaxConsecutiveCommitFailures is the degraded-mode trip point
	// Default() ships with: 8 consecutive commit failures degrade one
	// project. Against the suite's injected-failure counts (at most 2
	// consecutive commit failures in any existing test, plus up to 3
	// asset-layer upload attempts that never touch the commit streak) 8
	// is 4x headroom over the observed max, so transient blips can never
	// trip it, while a genuinely sick backend degrades after a handful
	// of failures instead of piling up unbounded uncommitted work.
	defaultMaxConsecutiveCommitFailures = 8
)

// AtimePolicy selects the access-time update policy.
type AtimePolicy string

const (
	// AtimeRelatime updates atime at most once per interval.
	AtimeRelatime AtimePolicy = "relatime"
	// AtimeStrict updates atime on every read.
	AtimeStrict AtimePolicy = "strictatime"
	// AtimeNo never updates atime.
	AtimeNo AtimePolicy = "noatime"
)

// Config tunes a StorHub client: endpoints, sizing, retries, and logging.
type Config struct {
	// Logger, when set, is the logger. Supplying it together with any of
	// the LogLevel/LogFormat/LogColor/LogOutput knobs fails Validate
	// loudly: silent precedence between overlapping mechanisms hides
	// configuration mistakes. Those knobs exist only to build a logger
	// when none was supplied.
	Logger *slog.Logger
	// LogOutput is the destination used when building a default logger.
	LogOutput io.Writer
	// LogLevel/LogFormat are normalized (trimmed, lowercased) by
	// WithDefaults and validated by Validate; unknown values fail loudly
	// instead of silently mapping to something else.
	LogLevel  string
	LogFormat string
	LogColor  bool

	APIBaseURL       string
	APIVersion       string
	HTTPClient       *http.Client
	ChunkSize        int64
	BufferSize       int
	RepoDescription  string
	CreatePublicRepo bool
	MaxRetries       int
	BaseRetryDelay   time.Duration
	MaxRetryDelay    time.Duration

	// RevivalTimeout bounds how long a dirty-mutation revival waits for an
	// evicted project's commit loop to finish exiting before it revives the
	// entry without swapping the loop's channels. Zero takes the library
	// default (5s); embedders and tests may shorten it.
	RevivalTimeout time.Duration

	// RateReserve keeps this many requests of the hourly GitHub budget
	// unspent as headroom for recovery operations. Negative restores the
	// client default.
	RateReserve int64
	// RateMaxWait bounds any single rate-limit wait the client may take.
	// Zero takes the library default (15m, for long-running processes);
	// a negative value opts into fail-fast behavior.
	RateMaxWait time.Duration
	// RatePointsPerMin caps secondary rate-limit points per minute
	// (GET=1, writes=5; GitHub's documented ceiling is 900).
	RatePointsPerMin int64
	// RateContentPerMin caps content-generating requests per minute
	// such as release-asset uploads (documented ceiling: 80).
	RateContentPerMin int64
	// MaxConcurrentRequests bounds in-flight API requests (GitHub's
	// documented ceiling is 100).
	MaxConcurrentRequests int64

	// TransferThroughput is the conservative bytes-per-second assumption
	// used to size upload/download deadlines; large transfers get
	// size/throughput seconds instead of a fixed timeout. Zero takes the
	// library default (1 MiB/s).
	TransferThroughput int64

	AtimePolicy AtimePolicy
	// MaxTrackedProjects bounds how many projects' metadata stay resident
	// in memory. The cap is applied when a new project joins: the least-
	// recently-used clean entry is evicted (dirty entries always survive).
	// Each entry pins one RepoMetadata clone - potentially MBs for large
	// repos - plus one parked goroutine.
	MaxTrackedProjects int
	GitCacheDir        string
	// JournalDir is where the client-side op journal is written
	// (<dir>/<project>.jsonl): a write-ahead mirror of uncommitted
	// metadata operations that replays onto freshly loaded remote state
	// after a crash. Empty disables journaling; embedders and the CLI
	// opt in explicitly (the CLI defaults it beneath CacheBase).
	JournalDir string
	// StrictConflicts opts out of automatic conflict resolution: a
	// rebase that would resolve a path conflict (per-path last-writer-
	// wins by default) fails the commit loudly instead. For callers that
	// require humans (or higher layers) to arbitrate every clash.
	StrictConflicts   bool
	DisableGitBackend bool
	// ObjectCacheMaxEntries bounds the per-project content-addressed object
	// cache (LRU). Eviction only costs a later refetch, never correctness.
	ObjectCacheMaxEntries int
	// HistoryWarnObjects is the advisory object-count threshold at which a
	// commit logs a once-per-window warning pointing at `storhub prune`:
	// full history is retained by design, so object accumulation is
	// surfaced rather than silently pruned. Zero disables the warning
	// (Default() enables it; WithDefaults preserves an explicit zero
	// instead of overwriting the disable).
	HistoryWarnObjects uint64
	// MaxConsecutiveCommitFailures is the degraded-mode trip point: a
	// project whose commits fail this many times consecutively stops
	// admitting new mutations until an explicit ReEnableProject. It is
	// count-valued, so unlike the timeout/patience knobs it is NOT
	// expressed in TickUnit/PatienceUnit bases. Zero takes Default()
	// (WithDefaults fills it); negative fails Validate.
	MaxConsecutiveCommitFailures int
	Now                          func() time.Time
	Sleep                        func(context.Context, time.Duration) error
}

// Time units: every timeout, patience, wait, and TTL in the system
// derives from these two bases — no independent magic durations.
//
//   - TickUnit (50ms) is the micro scale: hot-path latencies where the
//     event already fired and only coalescing remains.
//   - PatienceUnit (5s) is the macro scale: teardowns, backstops, and
//     staleness, budgeted in whole units by the autonomy of the waited
//     party (kernel < local drain < human/network).
//
// A value that cannot be expressed exactly in its tier is a design
// smell: either the tier is wrong or the value is. Server-dictated
// waits (Retry-After, token buckets) are expressed here too — the
// server owns the resume instant, but our spelling of it stays
// symmetrical.
const (
	TickUnit     = 50 * time.Millisecond
	PatienceUnit = 5 * time.Second
)

// Default returns Config with all defaults applied.
func Default() Config {
	return Config{
		APIBaseURL:            defaultAPIBaseURL,
		APIVersion:            defaultAPIVersion,
		HTTPClient:            newDefaultHTTPClient(),
		ChunkSize:             DefaultChunkSize,
		BufferSize:            DefaultBufferSize,
		RepoDescription:       defaultRepoDescription,
		CreatePublicRepo:      false,
		MaxRetries:            4,
		BaseRetryDelay:        10 * TickUnit,
		MaxRetryDelay:         160 * TickUnit,
		RevivalTimeout:        1 * PatienceUnit,
		RateReserve:           25,
		RateMaxWait:           180 * PatienceUnit,
		RatePointsPerMin:      720,
		RateContentPerMin:     60,
		MaxConcurrentRequests: 16,
		TransferThroughput:    1 << 20,
		// Nil: the caller opts into a destination by setting LogOutput
		// (or supplying Logger). NewLogger falls back to os.Stderr for
		// a nil Output, so library import alone never opens a stream.
		LogOutput: nil,
		// Warn, not debug: a mount performs thousands of FS operations
		// per minute, and a debug-level default turns every one of them
		// into formatted stderr traffic (the "looks idle but burns"
		// component). Explicit opt-in (--log-level debug / LogLevel)
		// still gets the full trace.
		LogLevel:    logging.LevelWarn,
		LogFormat:   logging.FormatPretty,
		LogColor:    true,
		AtimePolicy: AtimeNo,
		// 64 entries is a generous working set for interactive use while
		// bounding worst-case residency; embedders touching thousands of
		// projects should size it deliberately.
		MaxTrackedProjects:    64,
		GitCacheDir:           defaultGitCacheDir(),
		ObjectCacheMaxEntries: 8192,
		HistoryWarnObjects:    defaultHistoryWarnObjects,
		// 8 consecutive commit failures degrade one project: 4x headroom
		// over the suite's injected-failure max (2), fast enough to stop
		// new work piling onto a sick backend within a handful of tries.
		MaxConsecutiveCommitFailures: defaultMaxConsecutiveCommitFailures,
		Now:                          time.Now,
		Sleep:                        SleepWithContext,
	}
}

// WithDefaults fills every unset field from Default(). There is no
// zero-config fast path: field-by-field filling is cheap and a shortcut
// that forgets a field silently discards user configuration.
//
// "Unset" means exactly zero, never "zero or negative": silently replacing
// nonsense like ChunkSize: -1 with a default hides embedder typos before
// Validate ever sees them. Negative values pass through untouched so
// Validate rejects them loudly.
//
// Fields intentionally NOT filled here (zero is a live value with a
// documented downstream default — no silent third state):
//   - MaxRetries: zero is kept as-is (zero retries downstream);
//     negative fails Validate.
//   - RevivalTimeout: zero takes the library default (5s) downstream.
//   - RateReserve: negative restores the client default (25); zero is a
//     live "no headroom floor" setting, not unset.
//   - RateMaxWait: zero takes the library default (15m); negative opts
//     into fail-fast.
//   - RatePointsPerMin/RateContentPerMin/MaxConcurrentRequests: <=0 takes
//     the library defaults (720/60/16) downstream.
//   - TransferThroughput: zero takes the library default (1 MiB/s).
//   - JournalDir: empty disables journaling; the CLI opts in beneath
//     CacheBase, embedders set it explicitly.
//   - StrictConflicts/DisableGitBackend/CreatePublicRepo: booleans where
//     false is the meaningful default.
//   - HistoryWarnObjects: zero is the documented "disabled" value (the
//     storage consumer checks it), so overwriting it would make the
//     disable path unreachable. Default() still enables the warning.
func (c Config) WithDefaults() Config {
	defaults := Default()
	if c.APIBaseURL == "" {
		c.APIBaseURL = defaults.APIBaseURL
	}
	if c.APIVersion == "" {
		c.APIVersion = defaults.APIVersion
	}
	if c.HTTPClient == nil {
		c.HTTPClient = defaults.HTTPClient
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = defaults.ChunkSize
	}
	if c.BufferSize == 0 {
		c.BufferSize = defaults.BufferSize
	}
	if c.RepoDescription == "" {
		c.RepoDescription = defaults.RepoDescription
	}
	if c.BaseRetryDelay == 0 {
		c.BaseRetryDelay = defaults.BaseRetryDelay
	}
	if c.MaxRetryDelay == 0 {
		c.MaxRetryDelay = defaults.MaxRetryDelay
	}
	if c.LogOutput == nil {
		c.LogOutput = defaults.LogOutput
	}
	// Normalize case/whitespace but never map unknown values to something
	// else: Validate rejects them loudly.
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))
	if c.LogLevel == "" {
		c.LogLevel = defaults.LogLevel
	}
	c.LogFormat = strings.ToLower(strings.TrimSpace(c.LogFormat))
	if c.LogFormat == "" {
		c.LogFormat = defaults.LogFormat
	}
	if c.Logger == nil {
		c = c.resolveLogger()
	}
	if c.AtimePolicy == "" {
		c.AtimePolicy = defaults.AtimePolicy
	}
	if c.MaxTrackedProjects == 0 {
		c.MaxTrackedProjects = defaults.MaxTrackedProjects
	}
	if c.GitCacheDir == "" {
		c.GitCacheDir = defaults.GitCacheDir
	}
	if c.ObjectCacheMaxEntries == 0 {
		c.ObjectCacheMaxEntries = defaults.ObjectCacheMaxEntries
	}
	if c.MaxConsecutiveCommitFailures == 0 {
		c.MaxConsecutiveCommitFailures = defaults.MaxConsecutiveCommitFailures
	}
	// HistoryWarnObjects: see the WithDefaults godoc — zero disables.
	if c.Now == nil {
		c.Now = defaults.Now
	}
	if c.Sleep == nil {
		c.Sleep = defaults.Sleep
	}
	return c
}

// resolveLogger builds the default logger from the already-normalized log
// knobs, and clears the consumed knobs so the single-mechanism invariant
// (Validate rejects Logger+knobs) holds for every defaulted config. It is a
// no-op when the caller supplied a logger. Split out of WithDefaults so
// default-filling and logger construction read as separate steps.
func (c Config) resolveLogger() Config {
	if c.Logger != nil {
		return c
	}
	c.Logger = logging.NewLogger(logging.Options{
		Level:  c.LogLevel,
		Format: c.LogFormat,
		Color:  c.LogColor,
		Output: c.LogOutput,
	})
	c.LogLevel, c.LogFormat, c.LogColor, c.LogOutput = "", "", false, nil
	return c
}

// CacheBase returns the root directory for storhub's local caches.
// Component caches live beneath it: git/ for backend working repos,
// journal/ for the write-ahead op journal, fuse/<project>/ for overlays.
func CacheBase() string {
	if custom := cacheBaseFromEnv(); custom != "" {
		return custom
	}
	return defaultCacheBase()
}

// cacheBaseFromEnv reports $STORHUB_CACHE_DIR (trimmed), or "" when unset:
// the explicit operator override, checked first.
func cacheBaseFromEnv() string {
	return strings.TrimSpace(os.Getenv("STORHUB_CACHE_DIR"))
}

// defaultCacheBase is the platform user cache dir (~/.cache/storhub on
// Linux per XDG), falling back to a temp directory when no home is
// available. Split out of CacheBase so the env override and the platform
// default read as separate steps.
func defaultCacheBase() string {
	if userCache, err := os.UserCacheDir(); err == nil && userCache != "" {
		return filepath.Join(userCache, "storhub")
	}
	return filepath.Join(os.TempDir(), "storhub")
}

// defaultGitCacheDir returns the git backend's cache root beneath
// CacheBase. A shared /tmp/storhub would let concurrent processes (or
// other users on multi-user machines) collide on the same git workspaces.
func defaultGitCacheDir() string {
	return filepath.Join(CacheBase(), "git")
}

// DefaultGitCacheBase exposes the git cache base for callers that create
// per-process roots beneath it.
func DefaultGitCacheBase() string {
	return defaultGitCacheDir()
}

// ObjectCacheDir returns the root directory for content-addressed index
// object caches (CacheBase()/objects). Per-project caches live beneath it.
func (c Config) ObjectCacheDir() string {
	return DefaultObjectCacheBase()
}

// DefaultObjectCacheBase exposes the object-cache root for callers that sweep
// or create per-project caches beneath it without holding a Config value (the
// orphan reaper). It is the single source of the layout so the reaper and the
// storage layer can never disagree on where a project's cache lives.
func DefaultObjectCacheBase() string {
	return filepath.Join(CacheBase(), "objects")
}

func newDefaultHTTPClient() *http.Client {
	// Clone the default transport when possible; never assume its concrete
	// type, a custom DefaultTransport must not panic the process.
	var transport *http.Transport
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = t.Clone()
	} else {
		transport = &http.Transport{}
	}
	// Transfers are strictly sequential: a couple of pooled connections
	// per host is plenty, and small pools avoid socket buildup.
	transport.MaxIdleConns = 4
	transport.MaxIdleConnsPerHost = 2
	transport.MaxConnsPerHost = 4
	transport.IdleConnTimeout = 18 * PatienceUnit
	return &http.Client{Timeout: defaultRequestTimeout, Transport: transport}
}

// SleepWithContext is the default config Sleep implementation.
func SleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Validate rejects configurations that would otherwise fail silently or
// behave surprisingly at operation time. Empty optional fields are fine;
// unknown values are not.
func (c Config) Validate() error {
	if c.Logger != nil {
		var conflicts []string
		if c.LogLevel != "" {
			conflicts = append(conflicts, "LogLevel")
		}
		if c.LogFormat != "" {
			conflicts = append(conflicts, "LogFormat")
		}
		if c.LogColor {
			conflicts = append(conflicts, "LogColor")
		}
		if c.LogOutput != nil {
			conflicts = append(conflicts, "LogOutput")
		}
		if len(conflicts) > 0 {
			return fmt.Errorf("logger and log knobs %v are mutually exclusive: configure one mechanism", conflicts)
		}
	}
	if !logging.ValidLevel(c.LogLevel) {
		return fmt.Errorf("invalid log level %q (known: %v)", c.LogLevel, logging.KnownLevels())
	}
	if !logging.ValidFormat(c.LogFormat) {
		return fmt.Errorf("invalid log format %q (known: %v)", c.LogFormat, logging.KnownFormats())
	}
	switch c.AtimePolicy {
	case "", AtimeRelatime, AtimeStrict, AtimeNo:
	default:
		return fmt.Errorf("invalid atime policy %q (known: %s, %s, %s)", c.AtimePolicy, AtimeRelatime, AtimeStrict, AtimeNo)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("MaxRetries must be >= 0, got %d", c.MaxRetries)
	}
	if c.MaxTrackedProjects < 0 {
		return fmt.Errorf("MaxTrackedProjects must be >= 0, got %d", c.MaxTrackedProjects)
	}
	if c.ObjectCacheMaxEntries < 0 {
		return fmt.Errorf("ObjectCacheMaxEntries must be >= 0, got %d", c.ObjectCacheMaxEntries)
	}
	if c.MaxConsecutiveCommitFailures < 0 {
		return fmt.Errorf("MaxConsecutiveCommitFailures must be >= 0, got %d", c.MaxConsecutiveCommitFailures)
	}
	if c.BaseRetryDelay < 0 {
		return fmt.Errorf("BaseRetryDelay must be >= 0, got %v", c.BaseRetryDelay)
	}
	if c.MaxRetryDelay < 0 {
		return fmt.Errorf("MaxRetryDelay must be >= 0, got %v", c.MaxRetryDelay)
	}
	// Chunk windows must fit in one release asset: a value above the
	// ceiling would make the chunker's plan and the uploader's windows
	// disagree, failing mid-upload.
	if c.ChunkSize < 0 {
		return fmt.Errorf("ChunkSize must be >= 0, got %d", c.ChunkSize)
	}
	if c.ChunkSize > chunking.MaxReleaseAssetSize {
		return fmt.Errorf("ChunkSize %d exceeds the GitHub release-asset ceiling %d", c.ChunkSize, chunking.MaxReleaseAssetSize)
	}
	if c.BufferSize < 0 {
		return fmt.Errorf("BufferSize must be >= 0, got %d", c.BufferSize)
	}
	if c.BufferSize > MaxBufferSize {
		return fmt.Errorf("BufferSize %d exceeds the per-operation buffer cap %d", c.BufferSize, MaxBufferSize)
	}
	return nil
}
