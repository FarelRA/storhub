package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/FarelRA/storhub/internal/chunking"
	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

func TestServeRESTLoadsAuthFile(t *testing.T) {
	t.Cleanup(func() {
	})
	app, _, stderr := newTestApp(t)
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"realm":"demo","token_signing_key":"secretkey","users":[{"username":"admin","password":"pass","uid":0,"primary_gid":0,"admin":true}]}`), 0o644); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	app.seams.newRESTHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	app.seams.newREST = func(_ *storhub.StorHub, opts storhub.RESTOptions) (http.Handler, error) {
		if opts.Auth == nil || opts.Auth.Realm != "demo" || len(opts.Auth.Users) != 1 || opts.Auth.Users[0].Username != "admin" {
			t.Fatalf("unexpected auth opts: %+v", opts.Auth)
		}
		return http.NewServeMux(), nil
	}
	app.seams.listenServe = func(server *http.Server) error {
		if server.Addr != "127.0.0.1:9090" || server.Handler == nil {
			return fmt.Errorf("unexpected serve args: addr=%q handler=%v", server.Addr, server.Handler)
		}
		// ReadHeaderTimeout guards slow-loris; Read/WriteTimeout stay 0 so
		// large uploads and slow multi-gigabyte downloads are never amputated
		// mid-transfer (size bounds live in the content handlers).
		if server.ReadHeaderTimeout <= 0 || server.ReadTimeout != 0 || server.WriteTimeout != 0 || server.IdleTimeout <= 0 {
			return fmt.Errorf("expected REST server timeouts (ReadHeader>0 Read=0 Write=0 Idle>0), got ReadHeader=%v Read=%v Write=%v Idle=%v", server.ReadHeaderTimeout, server.ReadTimeout, server.WriteTimeout, server.IdleTimeout)
		}
		return errors.New("stop")
	}
	err := app.Run([]string{"rest", "--token", "x", "--listen", "127.0.0.1:9090", "--authfile", authFile})
	if err == nil || err.Error() != "stop" {
		t.Fatalf("expected stop error, got %v", err)
	}
	if !strings.Contains(stderr(), "with auth") {
		t.Fatalf("unexpected stderr: %q", stderr())
	}
}

func TestNormalizeCLIChunkSizeFloorsSmallValues(t *testing.T) {
	t.Parallel()
	if got := normalizeCLIChunkSize(0); got != 0 {
		t.Fatalf("expected zero chunk size to remain unset, got %d", got)
	}
	if got := normalizeCLIChunkSize(-1); got != -1 {
		t.Fatalf("expected negative chunk size to pass through for usage-error rejection, got %d", got)
	}
	if got := normalizeCLIChunkSize(1024); got != minCLIChunkSize {
		t.Fatalf("expected small chunk size to clamp to %d, got %d", minCLIChunkSize, got)
	}
	if got := normalizeCLIChunkSize(64 << 20); got != 64<<20 {
		t.Fatalf("expected larger chunk size to remain unchanged, got %d", got)
	}
}

// TestNormalizeCLIChunkSizeCeilingClamp pins the ceiling contract: a --chunksize
// above the GitHub release-asset ceiling must clamp DOWN, so the chunker's
// plan and the uploader's windows agree instead of failing mid-upload.
func TestNormalizeCLIChunkSizeCeilingClamp(t *testing.T) {
	t.Parallel()
	if got := normalizeCLIChunkSize(9999999999); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp to %d, got %d", chunking.MaxReleaseAssetSize, got)
	}
	if got := normalizeCLIChunkSize(chunking.MaxReleaseAssetSize); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("ceiling value must pass through, got %d", got)
	}
	if got := normalizeCLIChunkSize(chunking.MaxReleaseAssetSize + 1); got != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp to %d, got %d", chunking.MaxReleaseAssetSize, got)
	}
}

// TestHubConfigClampWarnsThroughSeam pins that the clamp warning goes
// through the warnOutput seam (capturable), not straight to os.Stderr.
func TestHubConfigClampWarnsThroughSeam(t *testing.T) {
	var buf bytes.Buffer
	restore := setWarnOutput(&buf)
	t.Cleanup(restore)
	cfg := newHubConfig("", 1024, false, logSettings{}, false)
	if cfg.ChunkSize != minCLIChunkSize {
		t.Fatalf("expected floor clamp, got %d", cfg.ChunkSize)
	}
	if !strings.Contains(buf.String(), "warning") || !strings.Contains(buf.String(), "--chunksize") {
		t.Fatalf("expected clamp warning on warnOutput, got %q", buf.String())
	}
	buf.Reset()
	cfg = newHubConfig("", 9999999999, false, logSettings{}, false)
	if cfg.ChunkSize != chunking.MaxReleaseAssetSize {
		t.Fatalf("expected ceiling clamp, got %d", cfg.ChunkSize)
	}
	if !strings.Contains(buf.String(), "--chunksize") {
		t.Fatalf("expected ceiling clamp warning, got %q", buf.String())
	}
}

// TestHubConfigRatePolicyWiring pins the documented contract: one-shot
// = fail-fast (negative max wait), long-running (mount, rest,
// serve) = pause up to the reset (positive max wait).
func TestHubConfigRatePolicyWiring(t *testing.T) {
	if got := newHubConfig("", 0, false, logSettings{}, false).RateMaxWait; got >= 0 {
		t.Fatalf("one-shot RateMaxWait = %v, want negative (fail-fast)", got)
	}
	if got := newHubConfig("", 0, false, logSettings{}, true).RateMaxWait; got <= 0 {
		t.Fatalf("long-running RateMaxWait = %v, want positive pause", got)
	}
}

// TestRatePolicyCommandRouting pins which commands reach which constructor:
// download is one-shot (standard hub), mount is long-running (mount hub).
func TestRatePolicyCommandRouting(t *testing.T) {
	var oneShot, longRunning int
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		oneShot++
		return &fakeHub{t: t}, nil
	}
	app.seams.newMountHub = func(_ context.Context, _, _ string, _ logSettings) (hubClient, error) {
		longRunning++
		return &fakeHub{t: t}, nil
	}
	downloadFile := filepath.Join(t.TempDir(), "out.bin")
	if err := app.Run([]string{"download", "--token", "x", "demo", "f", downloadFile}); err != nil {
		t.Fatalf("download: %v", err)
	}
	if oneShot != 1 || longRunning != 0 {
		t.Fatalf("download must use the one-shot hub (oneShot=%d longRunning=%d)", oneShot, longRunning)
	}
	if err := app.Run([]string{"mount", "--token", "x", "demo", t.TempDir()}); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if oneShot != 1 || longRunning != 1 {
		t.Fatalf("mount must use the long-running hub (oneShot=%d longRunning=%d)", oneShot, longRunning)
	}
}

// TestHubConfigAppliesLogSettingsAndJournal pins that every CLI hub
// (including the one-shot download path) must carry the --log-* flags and
// a journal directory beneath the cache base.
func TestHubConfigAppliesLogSettingsAndJournal(t *testing.T) {
	log := logSettings{level: "error", format: "text", color: false}
	for _, longRunning := range []bool{false, true} {
		cfg := newHubConfig("", 0, false, log, longRunning)
		if cfg.LogLevel != "error" || cfg.LogFormat != "text" || cfg.LogColor {
			t.Fatalf("log settings not applied (longRunning=%v): %+v", longRunning, cfg)
		}
		want := filepath.Join(storcfg.CacheBase(), "journal")
		if cfg.JournalDir != want {
			t.Fatalf("JournalDir = %q, want %q", cfg.JournalDir, want)
		}
	}
}

// TestNegativeChunkSizeIsUsageError pins that --chunksize -1 must exit as
// a usage error (class 2), not silently run with defaults.
func TestNegativeChunkSizeIsUsageError(t *testing.T) {
	app, _, _ := newTestApp(t)
	localFile := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(localFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	err := app.Run([]string{"upload", "--token", "x", "--chunksize", "-1", "demo", "f", localFile})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--chunksize") {
		t.Fatalf("negative --chunksize must be a usage error naming the flag, got %v", err)
	}
}

// TestPruneScopeAndKeepAreUsageErrors pins that bad scope and keep < 1 must
// be rejected by the CLI as usage errors (exit 2) before any hub exists.
func TestPruneScopeAndKeepAreUsageErrors(t *testing.T) {
	var created int
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		created++
		return &fakeHub{t: t}, nil
	}
	err := app.Run([]string{"project", "prune", "--token", "x", "demo", "bogus"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "prune scope") {
		t.Fatalf("bad scope must be a usage error, got %v", err)
	}
	err = app.Run([]string{"project", "prune", "--token", "x", "demo", "objects", "--keep", "0"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--keep") {
		t.Fatalf("--keep 0 must be a usage error, got %v", err)
	}
	err = app.Run([]string{"project", "prune", "--token", "x", "demo", "all", "--keep", "-3"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--keep") {
		t.Fatalf("negative --keep must be a usage error, got %v", err)
	}
	if created != 0 {
		t.Fatalf("usage errors must not build a hub, created=%d", created)
	}
	if err := app.Run([]string{"project", "prune", "--token", "x", "demo", "history", "--keep", "2"}); err != nil {
		t.Fatalf("valid prune must still run: %v", err)
	}
}

// TestWritePatchNegativeArgsAreUsageErrors pins that negative offsets and
// deletesizes are command-line mistakes (exit 2), not storage failures.
func TestWritePatchNegativeArgsAreUsageErrors(t *testing.T) {
	app, _, _ := newTestApp(t)
	cases := [][]string{
		{"write", "--token", "x", "demo", "f", "-1", "data"},
		{"patch", "--token", "x", "demo", "f", "-1", "0", "data"},
		{"patch", "--token", "x", "demo", "f", "0", "-5", "data"},
	}
	for _, args := range cases {
		err := app.Run(args)
		if err == nil || !IsUsageError(err) {
			t.Fatalf("%v must be a usage error, got %v", args[0], err)
		}
	}
	// Parse failures are usage errors too (exit 2, not 1).
	err := app.Run([]string{"write", "--token", "x", "demo", "f", "nope", "data"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "invalid offset") {
		t.Fatalf("unparseable offset must classify as usage error, got %v", err)
	}
}

// TestFlexDurationRejectsNonFinite pins the NaN/Inf nit: JSON's extended
// number grammar parses NaN/Infinity cleanly but they are nonsense TTLs.
func TestFlexDurationRejectsNonFinite(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"NaN", "Infinity", "-Infinity", "-30", `"1h"`} {
		var d flexDuration
		err := d.UnmarshalJSON([]byte(raw))
		if raw == `"1h"` {
			if err != nil || d.Duration() != time.Hour {
				t.Fatalf("valid duration string rejected: %v %v", err, d)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%s must be rejected as a TTL, got %v", raw, d)
		}
	}
}

// TestServeRejectsAnonymousWithAuthFile pins the contradictory-flags nit:
// --allowanonymous alongside an auth file must fail as a usage error
// instead of being silently ignored.
func TestServeRejectsAnonymousWithAuthFile(t *testing.T) {
	app, _, _ := newTestApp(t)
	app.seams.newRESTHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (*storhub.StorHub, error) {
		return &storhub.StorHub{}, nil
	}
	authFile := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"token_signing_key":"k"}`), 0o600); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	err := app.Run([]string{"rest", "--token", "x", "--authfile", authFile, "--allowanonymous"})
	if err == nil || !IsUsageError(err) || !strings.Contains(err.Error(), "--allowanonymous") {
		t.Fatalf("contradictory auth flags must be a usage error, got %v", err)
	}
}

// TestAuthFileRejectsUnknownFields pins the typo nit: a misspelled key in
// the auth JSON (e.g. token_ttl) must fail loudly, not silently default.
func TestAuthFileRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte(`{"token_signing_key":"k","token_ttl":"1h","users_typo":[]}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadRESTAuthOptions(path); err == nil || !strings.Contains(err.Error(), "users_typo") {
		t.Fatalf("unknown field must fail decoding, got %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"token_signing_key":"k","token_ttl":3600}`), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	auth, err := loadRESTAuthOptions(path)
	if err != nil || auth.TokenTTL != time.Hour {
		t.Fatalf("valid numeric TTL must decode to one hour, got %v %v", auth, err)
	}
}

// TestMountUmaskFlagMasksCreatedModes pins the mount --umask wiring:
// the flag exists with a 022 default, values parse as octal masks,
// misuse is a usage error, and the wired FUSEOptions mask created modes
// through the same ApplyCreateMode the server applies at create time.
func TestMountUmaskFlagMasksCreatedModes(t *testing.T) {
	app := New()
	mount, _, err := app.rootCmd.Find([]string{"mount"})
	if err != nil || mount == nil {
		t.Fatalf("find mount command: %v", err)
	}
	def, err := mount.Flags().GetString("umask")
	if err != nil {
		t.Fatalf("mount --umask flag missing: %v", err)
	}
	if def != "022" {
		t.Fatalf("mount --umask default: want %q, got %q", "022", def)
	}
	for _, tc := range []struct {
		raw      string
		wantMask uint32
		wantMode uint32
	}{
		{raw: "022", wantMask: 0o022, wantMode: 0o644},
		{raw: "027", wantMask: 0o027, wantMode: 0o640},
		{raw: "077", wantMask: 0o077, wantMode: 0o600},
		{raw: "0", wantMask: 0, wantMode: 0o666},
	} {
		if err := mount.Flags().Set("umask", tc.raw); err != nil {
			t.Fatalf("set --umask %q: %v", tc.raw, err)
		}
		raw, _ := mount.Flags().GetString("umask")
		mask, err := parseMountUmask(raw)
		if err != nil {
			t.Fatalf("parse --umask %q: %v", tc.raw, err)
		}
		if mask != tc.wantMask {
			t.Fatalf("parse --umask %q: want %#o, got %#o", tc.raw, tc.wantMask, mask)
		}
		opts := storhub.DefaultFUSEOptions()
		opts.Umask = mask
		opts.UmaskSet = true
		if got := opts.EffectiveUmask(); got != tc.wantMask {
			t.Fatalf("--umask %q: effective: want %#o, got %#o", tc.raw, tc.wantMask, got)
		}
		ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1000, GID: 1000, Umask: opts.EffectiveUmask()})
		ctx = shfs.WithCreateMode(ctx, 0o666)
		if got := shfs.ApplyCreateMode(ctx, 0o666); got != tc.wantMode {
			t.Fatalf("--umask %q: created mode: want %#o, got %#o", tc.raw, tc.wantMode, got)
		}
	}
	if got := storhub.DefaultFUSEOptions().EffectiveUmask(); got != 0o022 {
		t.Fatalf("default effective umask: want %#o, got %#o", 0o022, got)
	}
	for _, bad := range []string{"", "abc", "888", "1000", "-1"} {
		if _, err := parseMountUmask(bad); err == nil || !IsUsageError(err) {
			t.Fatalf("parse --umask %q: want usage error, got %v", bad, err)
		}
	}
}
