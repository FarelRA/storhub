package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storage "github.com/FarelRA/storhub/internal/storage"
	"github.com/spf13/cobra"
)

// Below-floor --chunksize must fail loud (usage error), not clamp.
func TestChunkSizeBelowFloorIsUsageError(t *testing.T) {
	if _, err := normalizeCLIChunkSize(1024); err == nil || !IsUsageError(err) {
		t.Fatalf("below-floor chunksize must be a usage error, got %v", err)
	}
}

// Pins: invalid STORHUB_RATE_RESERVE must fail hub construction loudly.
func TestInvalidRateEnvFailsLoud(t *testing.T) {
	t.Setenv("STORHUB_RATE_RESERVE", "notanumber")
	if _, err := newHubConfig("", 0, false, logSettings{}, false); err == nil {
		t.Fatal("invalid STORHUB_RATE_RESERVE must fail hub construction")
	}
}

// Pins: missing --config file must fail loud, not warn-and-continue.
func TestMissingConfigFileIsError(t *testing.T) {
	app, _, _ := newTestApp(t)
	app.configFile = "/nonexistent/storhub-config.json"
	if _, _, _, _, err := app.withFileConfig("", 0, false, false); err == nil {
		t.Fatal("missing --config file must be an error")
	}
}

// Pins: --dryrun (unhyphenated) is gone; --dry-run is the single spelling.
func TestPruneDryrunUnhyphenatedRejected(t *testing.T) {
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	err := app.Run([]string{"project", "prune", "--dryrun", "demo"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("prune --dryrun must be rejected as unknown flag, got %v", err)
	}
}

// Pins: --reflink=never is gone.
func TestReflinkNeverRejected(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("hello world")}}
	err := runCpWithFake(t, fake, []string{"cp", "--reflink=never", "demo", "a.txt", "b.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("--reflink=never must be a usage error, got %v", err)
	}
	if !strings.Contains(err.Error(), "auto") {
		t.Fatalf("rejection must suggest auto, got %v", err)
	}
}

// Pins: file create_public_repo=false must win when the flag is unset.
func TestFilePublicFalseWinsWhenFlagUnset(t *testing.T) {
	app, _, _ := newTestApp(t)
	path := filepath.Join(t.TempDir(), "storhub.json")
	if err := os.WriteFile(path, []byte(`{"create_public_repo": false}`), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	app.configFile = path
	_, _, public, _, err := app.withFileConfig("", 0, false, false)
	if err != nil {
		t.Fatalf("withFileConfig: %v", err)
	}
	if public {
		t.Fatal("file false with unset flag must stay false")
	}
}

// Pins: legacy STORHUB_REST_SIGNING_KEY still resolves, with migration path.
func TestLegacyShareSigningKeyFallback(t *testing.T) {
	t.Setenv("STORHUB_SHARE_SIGNING_KEY", "")
	t.Setenv("STORHUB_REST_SIGNING_KEY", "legacy-secret")
	cmd := &cobra.Command{}
	cmd.Flags().String("sharekey", "", "")
	if got := shareSigningKey(cmd); got != "legacy-secret" {
		t.Fatalf("legacy signing key must resolve, got %q", got)
	}
}

// Session mutators accept --sync and drain the handle's project after
// success, mirroring close --sync and REST ?sync=1.
func TestSessionWriteSyncDrains(t *testing.T) {
	fake := &fakeHub{t: t}
	opened := runSessionCLI(t, fake, []string{"session", "open", "--token", "x", "demo", "--mode", "w"})
	handle := strings.TrimSpace(opened)
	runSessionCLI(t, fake, []string{"session", "write", "--token", "x", "--handle", handle, "--sync", "0", "hi"})
	if len(fake.drainCalls) != 1 || fake.drainCalls[0] != "demo" {
		t.Fatalf("session write --sync must drain demo, got %v", fake.drainCalls)
	}
}

// Session open rejects a bad --ttl with the single TTL message shared
// with the REST open path.
func TestSessionOpenBadTTLMessage(t *testing.T) {
	_, err := parseSessionTTL("nope")
	if err == nil || !IsUsageError(err) {
		t.Fatalf("bad ttl must be a usage error, got %v", err)
	}
	if !strings.Contains(err.Error(), "Go duration string") {
		t.Fatalf("ttl message must name the Go duration contract, got %v", err)
	}
}

// The CLI help TTLs match the session manager's single source of truth.
func TestSessionTTLHelpMatchesManagerConsts(t *testing.T) {
	if storage.DefaultSessionIdleTTL != 10*time.Minute {
		t.Fatalf("DefaultSessionIdleTTL = %v, CLI help says 10m", storage.DefaultSessionIdleTTL)
	}
	if storage.DefaultSessionMaxTTL != time.Hour {
		t.Fatalf("DefaultSessionMaxTTL = %v, CLI help says 1h", storage.DefaultSessionMaxTTL)
	}
}

// A typo'd STORHUB_LOG_COLOR fails loud at startup, like every other
// strict STORHUB_* var.
func TestInvalidLogColorEnvFailsLoud(t *testing.T) {
	t.Setenv("STORHUB_LOG_COLOR", "maybe")
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return &fakeHub{t: t}, nil
	}
	err := app.Run([]string{"stat", "--token", "x", "demo", "a.txt"})
	if err == nil || !IsUsageError(err) {
		t.Fatalf("bad STORHUB_LOG_COLOR must be a usage error, got %v", err)
	}
}

// UmaskSet tracks explicit choice: default 022 applies without
// masquerading as an explicit --umask.
func TestFuseOptsUmaskSetTracksFlagChanged(t *testing.T) {
	app, _, _ := newTestApp(t)
	var mountCmd, serveCmd *cobra.Command
	for _, c := range app.rootCmd.Commands() {
		switch c.Name() {
		case "mount":
			mountCmd = c
		case "serve":
			serveCmd = c
		}
	}
	if mountCmd == nil || serveCmd == nil {
		t.Fatal("mount and serve must exist")
	}
	opts, err := fuseOptsFromFlags(mountCmd)
	if err != nil {
		t.Fatalf("fuseOpts: %v", err)
	}
	if opts.UmaskSet {
		t.Fatal("unset --umask must leave UmaskSet false")
	}
	if err := serveCmd.Flags().Set("umask", "027"); err != nil {
		t.Fatalf("set umask: %v", err)
	}
	opts, err = fuseOptsFromFlags(serveCmd)
	if err != nil {
		t.Fatalf("fuseOpts: %v", err)
	}
	if !opts.UmaskSet || opts.Umask != 0o027 {
		t.Fatalf("explicit --umask must set UmaskSet with the mask, got %+v", opts)
	}
}

// File create_public_repo assigns when the flag is unset; an explicit
// flag wins over the file either way.
func TestFilePublicMergeLastWins(t *testing.T) {
	newConfiguredApp := func(t *testing.T, content string) *App {
		t.Helper()
		app, _, _ := newTestApp(t)
		path := filepath.Join(t.TempDir(), "storhub.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write temp config: %v", err)
		}
		app.configFile = path
		return app
	}
	app := newConfiguredApp(t, `{"create_public_repo": true}`)
	if _, _, public, _, err := app.withFileConfig("", 0, false, false); err != nil || !public {
		t.Fatalf("file true with unset flag must apply, got %v err %v", public, err)
	}
	app = newConfiguredApp(t, `{"create_public_repo": true}`)
	if _, _, public, _, err := app.withFileConfig("", 0, false, true); err != nil || public {
		t.Fatalf("explicit unset flag must win over file true, got %v err %v", public, err)
	}
}
