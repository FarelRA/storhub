package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/FarelRA/storhub/internal/posix"
)

func TestNormalizePathPreservesWhitespace(t *testing.T) {
	t.Parallel()
	if got, err := shfs.NormalizePath(" docs/guide.txt "); err != nil || got != " docs/guide.txt " {
		t.Fatalf("whitespace must be preserved in fs paths: %q %v", got, err)
	}
}

func TestNormalizePathRootAndAbsolute(t *testing.T) {
	t.Parallel()
	if got, err := shfs.NormalizePath("."); err != nil || got != "" {
		t.Fatalf("expected root path normalization, got %q %v", got, err)
	}
	if got, err := shfs.NormalizePath("/docs/guide.txt"); err != nil || got != "docs/guide.txt" {
		t.Fatalf("expected absolute fs path normalization, got %q %v", got, err)
	}
	if _, err := shfs.NormalizePath("../escape"); err == nil {
		t.Fatal("expected path escape error")
	}
}

func TestStoredPathParentHelpers(t *testing.T) {
	t.Parallel()
	if shfs.ParentPath("docs/guide.txt") != "docs" {
		t.Fatal("unexpected stored path helpers")
	}
	if !shfs.IsParentOrSame("docs", "docs/guide.txt") || shfs.IsParentOrSame("images", "docs/guide.txt") {
		t.Fatal("unexpected parent/same result")
	}
}

func TestShortSHA(t *testing.T) {
	t.Parallel()
	if shortSHA("1234567890123456") != "123456789012" || shortSHA("short") != "short" {
		t.Fatal("unexpected short sha")
	}
}

func TestAssetNamerShape(t *testing.T) {
	t.Parallel()
	assertNamerShape(t, newAssetNamer(), 32)
}

func TestDefaultModes(t *testing.T) {
	t.Parallel()
	if defaultFileMode(NodeKindFile) != 0o644 || defaultFileMode(NodeKindSymlink) != 0o777 || defaultDirMode() != 0o755 {
		t.Fatal("unexpected mode defaults")
	}
}

func TestCloneStringMapAndChooseNonZeroTime(t *testing.T) {
	t.Parallel()
	if posix.CloneStringMap(nil) != nil {
		t.Fatal("expected nil clone")
	}
	if posix.ChooseNonZeroTime(0, 1) == 0 {
		t.Fatal("expected chosen non-zero time")
	}
}

func TestSleepWithContext(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	if _, err := NewStorHubWithContext(context.Background(), "", DefaultConfig()); err == nil {
		t.Fatal("expected empty token error")
	}
	hub, err := NewStorHubWithContext(context.Background(), "token", DefaultConfig())
	if err != nil {
		t.Fatalf("new hub: %v", err)
	}
	if hub.Owner() != "" {
		t.Fatalf("expected lazy owner resolution, got %q", hub.Owner())
	}
	defaults := fusefs.DefaultOptions()
	if defaults.OverlayBufferSize == 0 {
		t.Fatalf("unexpected fuse defaults: %+v", defaults)
	}
	fs, err := hub.NewFUSE("valid-project", fusefs.Options{})
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	opts := fs.Options()
	if opts.OverlayBufferSize != defaults.OverlayBufferSize || opts.EntryTimeout != defaults.EntryTimeout {
		t.Fatalf("expected fuse defaults applied: %+v", opts)
	}
	if _, err := hub.NewFUSE("bad/name", fusefs.Options{}); err == nil {
		t.Fatal("expected invalid project error")
	}
}

func TestAssetNamingDictionaryRegression(t *testing.T) {
	t.Parallel()
	// Shape (words/extensions/derivation/uniqueness) is pinned once by
	// assertNamerShape; this regression adds only the diversity assertion.
	names := assertNamerShape(t, newAssetNamer(), 200)
	seenExts := make(map[string]int)
	for _, name := range names {
		parts := strings.Split(name, ".")
		if len(parts) < 2 || len(parts) > 6 {
			t.Fatalf("unexpected dot parts %q: want 1-5 words + 1-5 exts", name)
		}
		for _, ext := range parts[1:] {
			if ext == "" {
				t.Fatalf("empty extension in %q", name)
			}
			seenExts[ext]++
		}
		if parts[0] == "" {
			t.Fatalf("empty words part %q", name)
		}
	}
	if len(seenExts) < 50 {
		t.Fatalf("expected dictionary-based diversity, got %d unique exts <50 (likely old wordlist)", len(seenExts))
	}
}
