package config

// This file owns the canonical environment-variable table: every STORHUB_*
// name the process reads, its owner, and its fallback. Both composition
// paths (LoadFile for embedders, the CLI flag/env/file layering) consult
// this table, so a key can never drift between two spellings again.
// Downstream docs consume this table as the single source of names.
//
// Owners: "file-env" keys are applied by applyFileEnv (config_file.go) and
// therefore visible to both LoadFile and the CLI file path. "cli" keys are
// applied by the CLI rate/terminal layer (internal/cli/app_config.go) which
// owns types the file schema cannot express (durations, int64 governors).
// A file key and a CLI key never share a name.
//
// Canonical table:
//
//	STORHUB_API_BASE_URL              file-env  GitHub API base URL
//	STORHUB_LOG_LEVEL                 file-env  debug, info, warn, error
//	STORHUB_LOG_FORMAT                file-env  pretty, text
//	STORHUB_LOG_COLOR                 file-env  bool (strict: garbage fails loud)
//	STORHUB_CACHE_DIR                 config    cache root (CacheBase, read per call)
//	STORHUB_RATE_RESERVE              cli       int64 governor headroom
//	STORHUB_RATE_MAX_WAIT             cli       duration bound (negative = fail-fast)
//	STORHUB_RATE_POINTS_PER_MIN       cli       int64 governor
//	STORHUB_RATE_CONTENT_PER_MIN      cli       int64 governor
//	STORHUB_MAX_CONCURRENT            cli       int64 governor
//	STORHUB_MAX_CONSECUTIVE_FAILURES  cli       int64 trip point
//	STORHUB_TRANSFER_THROUGHPUT       cli       int64 bytes-per-second assumption
//	STORHUB_REST_AUTH_FILE            cli       rest/serve --authfile fallback
//	STORHUB_SHARE_SIGNING_KEY         cli       rest/serve --sharekey fallback
//	STORHUB_HANDLE                    cli       session --handle fallback
//
// Retired (phaseout, read with a deprecation warning, never documented
// for new use):
//
//	STORHUB_REST_SIGNING_KEY  alias of STORHUB_SHARE_SIGNING_KEY
//
// Credential note: GITHUB_TOKEN is the CLI hub token (--token fallback).
// It is terminal auth, never a REST credential, and lives outside this
// table on purpose.
//
// Keys with no STORHUB_* spelling (flags or file only): chunk_size and
// create_public_repo travel by --chunksize/--public flag or the JSON file.
// The CLI takes them as flags only; LoadFile takes them from the file
// only. This asymmetry is deliberate and pinned here so neither path
// invents a new env name unilaterally.
const (
	EnvAPIBaseURL             = "STORHUB_API_BASE_URL"
	EnvLogLevel               = "STORHUB_LOG_LEVEL"
	EnvLogFormat              = "STORHUB_LOG_FORMAT"
	EnvLogColor               = "STORHUB_LOG_COLOR"
	EnvCacheDir               = "STORHUB_CACHE_DIR"
	EnvRateReserve            = "STORHUB_RATE_RESERVE"
	EnvRateMaxWait            = "STORHUB_RATE_MAX_WAIT"
	EnvRatePointsPerMin       = "STORHUB_RATE_POINTS_PER_MIN"
	EnvRateContentPerMin      = "STORHUB_RATE_CONTENT_PER_MIN"
	EnvMaxConcurrent          = "STORHUB_MAX_CONCURRENT"
	EnvMaxConsecutiveFailures = "STORHUB_MAX_CONSECUTIVE_FAILURES"
	EnvTransferThroughput     = "STORHUB_TRANSFER_THROUGHPUT"
	EnvRESTAuthFile           = "STORHUB_REST_AUTH_FILE"
	EnvShareSigningKey        = "STORHUB_SHARE_SIGNING_KEY"
	EnvSessionHandle          = "STORHUB_HANDLE"
	EnvLegacyShareSigningKey  = "STORHUB_REST_SIGNING_KEY"
)
