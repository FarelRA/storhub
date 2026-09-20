package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

func TestShortSHAAndPrintHelpers(t *testing.T) {
	if shortSHA("123456789") != "12345678" || shortSHA("short") != "short" {
		t.Fatal("unexpected short sha")
	}
	output := captureStdout(t, func() {
		printFile("file", nil)
		printFile("file", &storhub.FileMetadata{Size: 3, Inode: 1, Mode: 0o644})
		printDir("docs", []storhub.DirEntry{{Path: "docs/a.txt", Inode: 1, Size: 3, Mode: 0o644}, {Path: "docs/link", IsSymlink: true, Inode: 2, Mode: 0o777}})
		printStat("stat", nil)
		printStat("stat", &storhub.EntryInfo{Path: "docs/a.txt", Inode: 1, Size: 3, Mode: 0o644, UID: 1, GID: 2, NLink: 1, Kind: storhub.NodeKindFile})
	})
	for _, want := range []string{"file: <nil>", "docs/a.txt", "docs/link", "stat: <nil>", "uid/gid: 1/2"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected %q in output %q", want, output)
		}
	}
}

func TestMainFailurePath(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess smoke test: pays full test-binary init per spawn, skipped in short mode")
	}
	helper := os.Args[0]
	cmd := exec.Command(helper, "-test.run=TestShowcaseHelperProcess", "--", "main")
	cmd.Env = append(os.Environ(), "GO_WANT_SHOWCASE_HELPER=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("expected showcase main to fail without token")
	}
	if !strings.Contains(string(out), "GITHUB_TOKEN environment variable not set") {
		t.Fatalf("unexpected main output: %q", out)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	original := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = original }()
	readDone := make(chan string, 1)
	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, r)
		readDone <- buf.String()
	}()
	fn()
	_ = w.Close()
	return <-readDone
}

func TestPrintStatFormatsEntry(t *testing.T) {
	output := captureStdout(t, func() {
		printStat("stat", &storhub.EntryInfo{Path: "x", Inode: 9, Size: 2, Mode: 0o644, UID: 1, GID: 2, NLink: 1, IsDir: true, Kind: storhub.NodeKindFile, ModifiedAt: 1})
	})
	if !strings.Contains(output, fmt.Sprintf("- inode: %d", 9)) {
		t.Fatalf("unexpected stat output: %q", output)
	}
}

func TestRunShowcaseWorkflow(t *testing.T) {
	hub := &fakeShowcaseHub{}
	workspace := t.TempDir()
	output := captureStdout(t, func() {
		if err := runShowcase(context.Background(), hub, workspace, "demo-project"); err != nil {
			t.Fatalf("run showcase: %v", err)
		}
	})
	for _, want := range []string{"- owner: demo-owner", "ok: upload file", "chunk count", "xattr user.demo", "Tracked Files", "Releases", "Filesystem Stats", "Recent Metadata Revisions", "rollback removed marker as expected"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected %q in output %q", want, output)
		}
	}
	downloaded, err := os.ReadFile(filepath.Join(workspace, "downloaded-guide.txt"))
	if err != nil || !strings.Contains(string(downloaded), "StorHub showcase") {
		t.Fatalf("downloaded content: %q %v", downloaded, err)
	}
}

type fakeShowcaseHub struct{}

func (f *fakeShowcaseHub) Owner() string { return "demo-owner" }
func (f *fakeShowcaseHub) MkdirContext(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeShowcaseHub) CreateFileContext(_ context.Context, _, filePath string) (*storhub.FileMetadata, error) {
	return fileMeta(filePath, []byte{}), nil
}
func (f *fakeShowcaseHub) WriteFileAtContext(_ context.Context, _, filePath string, offset int64, data []byte, _ ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	return fileMeta(filePath, append(make([]byte, offset), data...)), nil
}
func (f *fakeShowcaseHub) AppendFileContext(_ context.Context, _, filePath string, data []byte, _ ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	return fileMeta(filePath, data), nil
}
func (f *fakeShowcaseHub) ReadFileAtContext(_ context.Context, _, _ string, _, _ int64) ([]byte, error) {
	return []byte("alpha\nbeta\n"), nil
}
func (f *fakeShowcaseHub) RenameContext(_ context.Context, _, _, _ string, _ ...shfs.MutateOption) error {
	return nil
}
func (f *fakeShowcaseHub) TruncateFileContext(_ context.Context, _, filePath string, _ int64, _ ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	return fileMeta(filePath, []byte("alpha")), nil
}
func (f *fakeShowcaseHub) UploadFileContext(_ context.Context, _, remotePath, _ string) (*storhub.FileMetadata, error) {
	return fileMeta(remotePath, []byte("StorHub guide\n")), nil
}
func (f *fakeShowcaseHub) ReplaceFileContext(_ context.Context, _, remotePath, _ string, _ ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	return fileMeta(remotePath, []byte("StorHub guide v2\n")), nil
}
func (f *fakeShowcaseHub) PatchFileContext(_ context.Context, _, filePath string, _, _ int64, _ []byte, _ ...shfs.MutateOption) (*storhub.FileMetadata, error) {
	return fileMeta(filePath, []byte("StorHub showcase guide\n")), nil
}
func (f *fakeShowcaseHub) DownloadFileContext(_ context.Context, _, _, localPath string) error {
	return os.WriteFile(localPath, []byte("StorHub showcase guide\n"), 0o644)
}
func (f *fakeShowcaseHub) ListFilesContext(_ context.Context, _ string) ([]storhub.FileMetadata, error) {
	return []storhub.FileMetadata{*fileMeta("docs/specs/guide.txt", []byte("StorHub showcase guide\n"))}, nil
}
func (f *fakeShowcaseHub) ListReleasesContext(_ context.Context, _ string) ([]storhub.ReleaseMetadata, error) {
	return []storhub.ReleaseMetadata{{AssetCount: 1}}, nil
}
func (f *fakeShowcaseHub) ListMetadataRevisionsContext(_ context.Context, _ string) ([]storhub.MetadataRevision, error) {
	return []storhub.MetadataRevision{{CommitSHA: "deadbeefcafebabe", Message: "new", CommittedAt: 2}, {CommitSHA: "facefeedcafebabe", Message: "old", CommittedAt: 1}}, nil
}
func (f *fakeShowcaseHub) RollbackMetadataContext(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeShowcaseHub) ChmodContext(_ context.Context, _, _ string, _ uint32) error {
	return nil
}
func (f *fakeShowcaseHub) ChownContext(_ context.Context, _, _ string, _, _ uint32) error {
	return nil
}
func (f *fakeShowcaseHub) ChtimesContext(_ context.Context, _, _ string, _, _ int64) error {
	return nil
}
func (f *fakeShowcaseHub) SetXAttrContext(_ context.Context, _, _, _ string, _ []byte, _ ...shfs.XAttrMode) error {
	return nil
}
func (f *fakeShowcaseHub) GetXAttrContext(_ context.Context, _, _, _ string) ([]byte, error) {
	return []byte("enabled"), nil
}
func (f *fakeShowcaseHub) ListXAttrContext(_ context.Context, _, _ string) ([]string, error) {
	return []string{"user.demo"}, nil
}
func (f *fakeShowcaseHub) RemoveXAttrContext(_ context.Context, _, _, _ string) error {
	return nil
}
func (f *fakeShowcaseHub) SymlinkContext(_ context.Context, _, target, linkPath string) (*storhub.FileMetadata, error) {
	meta := fileMeta(linkPath, []byte(target))
	meta.Symlink = target
	meta.Mode = 0o777
	return meta, nil
}
func (f *fakeShowcaseHub) ReadlinkContext(_ context.Context, _, _ string) (string, error) {
	return "docs/specs/guide.txt", nil
}
func (f *fakeShowcaseHub) LinkContext(_ context.Context, _, _, newPath string) (*storhub.FileMetadata, error) {
	return fileMeta(newPath, []byte("StorHub showcase guide\n")), nil
}
func (f *fakeShowcaseHub) ReadDirContext(_ context.Context, _, _ string) ([]storhub.DirEntry, error) {
	return []storhub.DirEntry{{Path: "docs/specs/guide.txt", Name: "guide.txt", Size: 23, Inode: 1, Mode: 0o640}, {Path: "docs/specs/guide.link", Name: "guide.link", IsSymlink: true, Inode: 2, Mode: 0o777}}, nil
}
func (f *fakeShowcaseHub) StatPathContext(_ context.Context, _, targetPath string) (*storhub.EntryInfo, error) {
	if targetPath == "scratch/rollback-marker.txt" {
		return nil, fmt.Errorf("file not found")
	}
	return &storhub.EntryInfo{Path: targetPath, Inode: 1, Size: 23, Mode: 0o640, UID: 1, GID: 2, NLink: 1, Kind: storhub.NodeKindFile}, nil
}
func (f *fakeShowcaseHub) StatFSContext(_ context.Context, _ string) (*storhub.FSStats, error) {
	return &storhub.FSStats{Files: 2, Directories: 3, Inodes: 5, Bytes: 42, Releases: 1, Assets: 2}, nil
}
func (f *fakeShowcaseHub) DeleteFileContext(_ context.Context, _, _ string, _ ...shfs.MutateOption) error {
	return nil
}
func (f *fakeShowcaseHub) RmdirContext(_ context.Context, _, _ string, _ ...shfs.MutateOption) error {
	return nil
}
func (f *fakeShowcaseHub) PruneContext(_ context.Context, _, _ string, _ int, _ bool) (*storhub.PruneResult, error) {
	return &storhub.PruneResult{DeletedAssets: 1, DeletedReleases: 0}, nil
}
func (f *fakeShowcaseHub) CleanupProjectContext(_ context.Context, _ string) error {
	return nil
}
func (f *fakeShowcaseHub) DeleteReleaseContext(_ context.Context, _, _ string) error {
	return nil
}
func (f *fakeShowcaseHub) DeleteProjectContext(_ context.Context, _ string) error { return nil }

func fileMeta(_ string, data []byte) *storhub.FileMetadata {
	return &storhub.FileMetadata{
		Size:   int64(len(data)),
		Inode:  1,
		Mode:   0o644,
		Chunks: []int64{1},
	}
}

func TestShowcaseHelperProcess(_ *testing.T) {
	if os.Getenv("GO_WANT_SHOWCASE_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" && i+1 < len(args) {
			switch args[i+1] {
			case "main":
				main()
			}
			return
		}
	}
	os.Exit(2)
}
