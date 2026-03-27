package storage

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/FarelRA/storhub/internal/posix"
)

func TestHelperUtilities(t *testing.T) {
	if got, err := shfs.NormalizePath(" docs/guide.txt "); err != nil || got != "docs/guide.txt" {
		t.Fatalf("unexpected normalized fs path: %q %v", got, err)
	}
	if got, err := shfs.NormalizePath("."); err != nil || got != "" {
		t.Fatalf("expected root path normalization, got %q %v", got, err)
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
	if metadataCommitMessage("file.txt", false) != "storhub: add file.txt" || metadataCommitMessage("file.txt", true) != "storhub: replace file.txt" {
		t.Fatal("unexpected metadata commit message")
	}
	if shortSHA("1234567890123456") != "123456789012" || shortSHA("short") != "short" {
		t.Fatal("unexpected short sha")
	}
	if key := uploadAssetKey(strings.Repeat("a", 20), time.Unix(1, 0)); !strings.HasPrefix(key, "aaaaaaaaaaaa-") {
		t.Fatalf("unexpected asset key: %q", key)
	}
	if sumCRC32C([]byte("abc")) != formatCRC32C(910901175) {
		t.Fatalf("unexpected crc32c: %s", sumCRC32C([]byte("abc")))
	}
	if defaultFileMode(NodeKindFile) != 0o644 || defaultFileMode(NodeKindSymlink) != 0o777 || defaultDirMode() != 0o755 {
		t.Fatal("unexpected mode defaults")
	}
	if posix.CloneStringMap(nil) != nil {
		t.Fatal("expected nil clone")
	}
	if posix.ChooseNonZeroTime(time.Time{}, time.Unix(1, 0)).IsZero() {
		t.Fatal("expected chosen non-zero time")
	}
	if err := sleepWithContext(context.Background(), 0); err != nil {
		t.Fatalf("expected no-op sleep, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepWithContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
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
	if defaults.PageSize == 0 || defaults.MaxCachedPages == 0 {
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
