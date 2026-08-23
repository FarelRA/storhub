package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/FarelRA/storhub/internal/posix"
)

func TestHelperUtilities(t *testing.T) {
	if got, err := shfs.NormalizePath(" docs/guide.txt "); err != nil || got != " docs/guide.txt " {
		t.Fatalf("whitespace must be preserved in fs paths: %q %v", got, err)
	}
	if got, err := shfs.NormalizePath("."); err != nil || got != "" {
		t.Fatalf("expected root path normalization, got %q %v", got, err)
	}
	if got, err := shfs.NormalizePath("/docs/guide.txt"); err != nil || got != "docs/guide.txt" {
		t.Fatalf("expected absolute fs path normalization, got %q %v", got, err)
	}
	if _, err := shfs.NormalizePath("../escape"); err == nil {
		t.Fatal("expected path escape error")
	}
	if "docs/guide.txt" != "docs/guide.txt" || shfs.ParentPath("docs/guide.txt") != "docs" {
		t.Fatal("unexpected stored path helpers")
	}
	if !shfs.IsParentOrSame("docs", "docs/guide.txt") || shfs.IsParentOrSame("images", "docs/guide.txt") {
		t.Fatal("unexpected parent/same result")
	}
	if shortSHA("1234567890123456") != "123456789012" || shortSHA("short") != "short" {
		t.Fatal("unexpected short sha")
	}
	nameRe := regexp.MustCompile(`^[a-z]+(?:[-_]?[a-z]+){1,3}\.[a-z]+$`)
	seen := make(map[string]struct{})
	namer := newAssetNamer()
	for i := 0; i < 32; i++ {
		name, err := namer.Next()
		if err != nil {
			t.Fatalf("generate asset name: %v", err)
		}
		if !nameRe.MatchString(name) {
			t.Fatalf("unexpected asset name format: %q", name)
		}
		if strings.Contains(name, "file") || strings.Contains(name, "txt") || strings.Contains(name, filepath.Base("docs/file.txt")) {
			t.Fatalf("asset name should not derive from source file name: %q", name)
		}
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate asset name generated: %q", name)
		}
		seen[name] = struct{}{}
	}
	if defaultFileMode(NodeKindFile) != 0o644 || defaultFileMode(NodeKindSymlink) != 0o777 || defaultDirMode() != 0o755 {
		t.Fatal("unexpected mode defaults")
	}
	if posix.CloneStringMap(nil) != nil {
		t.Fatal("expected nil clone")
	}
	if posix.ChooseNonZeroTime(0, 1) == 0 {
		t.Fatal("expected chosen non-zero time")
	}
	if err := storcfg.SleepWithContext(context.Background(), 0); err != nil {
		t.Fatalf("expected no-op sleep, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := storcfg.SleepWithContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled sleep, got %v", err)
	}
}

func TestNewStorHubAndFUSEDefaults(t *testing.T) {
	if _, err := NewStorHub(""); err == nil {
		t.Fatal("expected empty token error")
	}
	hub, err := NewStorHub("token")
	if err != nil {
		t.Fatalf("new hub: %v", err)
	}
	if hub.Owner() != "" {
		t.Fatalf("expected lazy owner resolution, got %q", hub.Owner())
	}
	defaults := fusefs.DefaultOptions()
	if defaults.PageSize == 0 {
		t.Fatalf("unexpected fuse defaults: %+v", defaults)
	}
	fs, err := hub.NewFUSE("valid-project", fusefs.Options{})
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	opts := fs.Options()
	if opts.PageSize != defaults.PageSize || opts.EntryTimeout != defaults.EntryTimeout {
		t.Fatalf("expected fuse defaults applied: %+v", opts)
	}
	if _, err := hub.NewFUSE("bad/name", fusefs.Options{}); err == nil {
		t.Fatal("expected invalid project error")
	}
}

func requireEnvFlag(t *testing.T, name string) {
	t.Helper()
	if os.Getenv(name) != "1" {
		t.Skipf("set %s=1 to run this smoke test", name)
	}
}

func requireEnvValue(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for this smoke test", name)
	}
	return value
}
