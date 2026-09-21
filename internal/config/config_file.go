package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/FarelRA/storhub/internal/logging"
)

// FileConfig is the sparse subset of Config a JSON config file may set.
// Every field is a pointer so "absent" stays distinguishable from "set":
// nil means the file says nothing and the normal chain (flags, then env,
// then defaults) applies untouched. The CLI honors exactly these keys;
// anything else fails loudly at load time instead of being silently
// ignored by a layer that cannot apply it. Range and value checks are
// left to Validate downstream, matching the WithDefaults contract that
// only exact zero counts as unset and nonsense stays visible.
type FileConfig struct {
	APIBaseURL       *string `json:"api_base_url,omitempty"`
	ChunkSize        *int64  `json:"chunk_size,omitempty"`
	CreatePublicRepo *bool   `json:"create_public_repo,omitempty"`
	LogLevel         *string `json:"log_level,omitempty"`
	LogFormat        *string `json:"log_format,omitempty"`
	LogColor         *bool   `json:"log_color,omitempty"`
}

// ReadFileConfig reads path as a JSON object into a sparse FileConfig.
// An empty path or a missing file is a no-op success returning the zero
// value, so callers without --config observe no behavior change. Unknown
// keys, malformed JSON, and trailing data fail loudly.
func ReadFileConfig(path string) (FileConfig, error) {
	if strings.TrimSpace(path) == "" {
		return FileConfig{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return FileConfig{}, nil
		}
		return FileConfig{}, fmt.Errorf("read config file %q: %w", path, err)
	}
	return parseFileConfig(data, path)
}

// LoadFile composes the full precedence chain for direct users:
// Default, then file values, then the STORHUB_* environment. An empty
// path or a missing file returns Default() unchanged without consulting
// the environment, so a no-file run is exactly the default run. The CLI
// prefers ReadFileConfig plus its own flag and env layering, which cover
// the same keys; LoadFile exists for embedders and tests that want one
// call. Callers still run WithDefaults and Validate before use.
func LoadFile(path string) (Config, error) {
	cfg := Default()
	if strings.TrimSpace(path) == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return Config{}, fmt.Errorf("read config file %q: %w", path, err)
	}
	fc, err := parseFileConfig(data, path)
	if err != nil {
		return Config{}, err
	}
	fc.applyTo(&cfg)
	if err := applyFileEnv(&cfg); err != nil {
		return Config{}, err
	}
	// Load milestone at Debug, never Info: file loads are routine and the
	// logger is not built yet (WithDefaults runs later), so the
	// process-default logger carries it.
	logging.Debug(cfg.Logger, "config file loaded", "path", path)
	return cfg, nil
}

// parseFileConfig decodes one JSON object with unknown keys rejected.
// Empty input decodes to the zero value: an empty file sets nothing.
func parseFileConfig(data []byte, path string) (FileConfig, error) {
	var fc FileConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		if err == io.EOF {
			return FileConfig{}, nil
		}
		return FileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("unexpected trailing data")
		}
		return FileConfig{}, fmt.Errorf("decode config file %q: %w", path, err)
	}
	return fc, nil
}

// applyTo overlays every key the file set onto cfg, leaving the rest.
func (f FileConfig) applyTo(cfg *Config) {
	if f.APIBaseURL != nil {
		cfg.APIBaseURL = *f.APIBaseURL
	}
	if f.ChunkSize != nil {
		cfg.ChunkSize = *f.ChunkSize
	}
	if f.CreatePublicRepo != nil {
		cfg.CreatePublicRepo = *f.CreatePublicRepo
	}
	if f.LogLevel != nil {
		cfg.LogLevel = *f.LogLevel
	}
	if f.LogFormat != nil {
		cfg.LogFormat = *f.LogFormat
	}
	if f.LogColor != nil {
		cfg.LogColor = *f.LogColor
	}
}

// applyFileEnv layers the STORHUB_* environment over file values so env
// keeps overriding the file. Only keys with a matching variable take
// part; an invalid boolean fails loudly instead of silently keeping the
// file value. It runs only when a file was loaded (see LoadFile): the
// no-file path returns Default() before this is reached.
func applyFileEnv(cfg *Config) error {
	if v := strings.TrimSpace(os.Getenv("STORHUB_API_BASE_URL")); v != "" {
		cfg.APIBaseURL = v
	}
	if v := strings.TrimSpace(os.Getenv("STORHUB_LOG_LEVEL")); v != "" {
		cfg.LogLevel = v
	}
	if v := strings.TrimSpace(os.Getenv("STORHUB_LOG_FORMAT")); v != "" {
		cfg.LogFormat = v
	}
	if v := strings.TrimSpace(os.Getenv("STORHUB_LOG_COLOR")); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid STORHUB_LOG_COLOR=%q: %w", v, err)
		}
		cfg.LogColor = parsed
	}
	return nil
}
