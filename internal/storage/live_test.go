package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FarelRA/storhub/internal/test"
)

func TestLiveGitHubSmoke(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_LIVE")
	token := test.RequireValue(t, "GITHUB_TOKEN")

	hub := newLiveHub(t, token, liveSmokeConfig())

	repoName := fmt.Sprintf("storhub-live-smoke-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	payload := []byte("live smoke test payload for storhub")
	payloadV2 := []byte("live smoke test payload for storhub version two")
	inputPath := filepath.Join(t.TempDir(), "live.txt")
	inputPathV2 := filepath.Join(t.TempDir(), "live-v2.txt")
	outputPath := filepath.Join(t.TempDir(), "downloaded.txt")
	rollbackPath := filepath.Join(t.TempDir(), "rolled-back.txt")
	if err := os.WriteFile(inputPath, payload, 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	if err := os.WriteFile(inputPathV2, payloadV2, 0o644); err != nil {
		t.Fatalf("write second input file: %v", err)
	}

	fileMeta, err := hub.UploadFileContext(context.Background(), repoName, "live.txt", inputPath)
	if err != nil {
		t.Fatalf("upload file: %v", err)
	}
	if fileMeta.Size != int64(len(payload)) {
		t.Fatalf("unexpected uploaded metadata: %+v", fileMeta)
	}

	files, err := hub.ListFilesContext(context.Background(), repoName)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("unexpected listed files: %+v", files)
	}

	if _, err := hub.ReplaceFileContext(context.Background(), repoName, "live.txt", inputPathV2); err != nil {
		t.Fatalf("replace file: %v", err)
	}
	for i := 0; i < 10; i++ {
		files, err = hub.ListFilesContext(context.Background(), repoName)
		if err != nil {
			t.Fatalf("list files after replace: %v", err)
		}
		if len(files) == 1 && files[0].Size == int64(len(payloadV2)) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(files) != 1 || files[0].Size != int64(len(payloadV2)) {
		t.Fatalf("replace metadata not visible yet: %+v", files)
	}
	patchedMeta, err := hub.PatchFileContext(context.Background(), repoName, "live.txt", 5, 5, []byte("PATCH"))
	if err != nil {
		t.Fatalf("patch file: %v", err)
	}
	if patchedMeta.Size != int64(len(payloadV2)) {
		t.Fatalf("unexpected patched size: %+v", patchedMeta)
	}
	var revisions []MetadataRevision
	for i := 0; i < 10; i++ {
		revisions, err = hub.ListMetadataRevisionsContext(context.Background(), repoName)
		if err != nil {
			t.Fatalf("list metadata revisions: %v", err)
		}
		if len(revisions) >= 3 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(revisions) < 3 {
		t.Fatalf("expected metadata history, got %+v", revisions)
	}
	var initialRevision string
	for _, revision := range revisions {
		if revision.Message == "storhub: add live.txt" {
			initialRevision = revision.CommitSHA
			break
		}
	}
	if initialRevision == "" {
		t.Fatalf("initial metadata revision not found: %+v", revisions)
	}
	if err := hub.RollbackMetadataContext(context.Background(), repoName, initialRevision); err != nil {
		t.Fatalf("rollback metadata: %v", err)
	}

	if err := hub.DownloadFileContext(context.Background(), repoName, "live.txt", outputPath); err != nil {
		t.Fatalf("download file: %v", err)
	}

	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("downloaded content mismatch")
	}
	if err := hub.DownloadFileContext(context.Background(), repoName, "live.txt", rollbackPath); err != nil {
		t.Fatalf("download rolled back file: %v", err)
	}
	rolledBackData, err := os.ReadFile(rollbackPath)
	if err != nil {
		t.Fatalf("read rolled back file: %v", err)
	}
	if !bytes.Equal(rolledBackData, payload) {
		t.Fatalf("rolled back content mismatch")
	}
}

func TestLiveGitHubFilesystemOps(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_LIVE")

	hub := newLiveHub(t, test.RequireValue(t, "GITHUB_TOKEN"), liveSmokeConfig())
	repoName := fmt.Sprintf("storhub-live-fs-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	if err := hub.MkdirContext(context.Background(), repoName, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.MkdirContext(context.Background(), repoName, "docs/specs"); err != nil {
		t.Fatalf("mkdir docs/specs: %v", err)
	}
	created, err := hub.CreateFileContext(context.Background(), repoName, "docs/specs/notes.txt")
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if created.Size != 0 {
		t.Fatalf("expected empty created file, got %+v", created)
	}
	if _, err := hub.WriteFileAtContext(context.Background(), repoName, "docs/specs/notes.txt", 0, []byte("hello")); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if _, err := hub.WriteFileAtContext(context.Background(), repoName, "docs/specs/notes.txt", 7, []byte("world")); err != nil {
		t.Fatalf("sparse write file: %v", err)
	}
	if _, err := hub.AppendFileContext(context.Background(), repoName, "docs/specs/notes.txt", []byte("!")); err != nil {
		t.Fatalf("append file: %v", err)
	}
	partial, err := hub.ReadFileAtContext(context.Background(), repoName, "docs/specs/notes.txt", 0, 13)
	if err != nil {
		t.Fatalf("read file at: %v", err)
	}
	if !bytes.Equal(partial, []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 'w', 'o', 'r', 'l', 'd', '!'}) {
		t.Fatalf("unexpected partial bytes: %v", partial)
	}
	if _, err := hub.TruncateFileContext(context.Background(), repoName, "docs/specs/notes.txt", 5); err != nil {
		t.Fatalf("truncate file: %v", err)
	}
	if err := hub.RenameContext(context.Background(), repoName, "docs", "archive"); err != nil {
		t.Fatalf("rename directory: %v", err)
	}

	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		info, err := hub.StatPathContext(context.Background(), repoName, "archive/specs/notes.txt")
		if err != nil {
			return false, nil
		}
		return !info.IsDir && info.Size == 5, nil
	}); err != nil {
		t.Fatalf("wait for renamed file metadata: %v", err)
	}

	entries, err := hub.ReadDirContext(context.Background(), repoName, "archive")
	if err != nil {
		t.Fatalf("readdir archive: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "specs" || !entries[0].IsDir {
		t.Fatalf("unexpected archive entries: %+v", entries)
	}
	stats, err := hub.StatFSContext(context.Background(), repoName)
	if err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if stats.Files != 1 || stats.Directories < 2 || stats.Bytes != 5 {
		t.Fatalf("unexpected statfs: %+v", stats)
	}

	outputPath := filepath.Join(t.TempDir(), "live-fs.txt")
	if err := hub.DownloadFileContext(context.Background(), repoName, "archive/specs/notes.txt", outputPath); err != nil {
		t.Fatalf("download file: %v", err)
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(data, []byte("hello")) {
		t.Fatalf("unexpected downloaded content: %q", data)
	}
	meta, err := hub.StatPathContext(context.Background(), repoName, "archive/specs/notes.txt")
	if err != nil {
		t.Fatalf("stat file after download: %v", err)
	}
	if meta.Size != 5 {
		t.Fatalf("unexpected file size after truncate: %+v", meta)
	}
	if err := hub.DeleteFileContext(context.Background(), repoName, "archive/specs/notes.txt"); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		files, err := hub.ListFilesContext(context.Background(), repoName)
		if err != nil {
			return false, err
		}
		return len(files) == 0, nil
	}); err != nil {
		t.Fatalf("wait for delete visibility: %v", err)
	}
}

func TestLiveGitHubPOSIXOps(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_LIVE")
	token := test.RequireValue(t, "GITHUB_TOKEN")

	hub := newLiveHub(t, token, liveSmokeConfig())
	repoName := fmt.Sprintf("storhub-live-posix-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	if err := hub.MkdirContext(context.Background(), repoName, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	input := filepath.Join(t.TempDir(), "base.txt")
	if err := os.WriteFile(input, []byte("hello live posix"), 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}
	base, err := hub.UploadFileContext(context.Background(), repoName, "docs/base.txt", input)
	if err != nil {
		t.Fatalf("upload base: %v", err)
	}
	alias, err := hub.LinkContext(context.Background(), repoName, "docs/base.txt", "docs/alias.txt")
	if err != nil {
		t.Fatalf("create hard link: %v", err)
	}
	if alias.Inode != base.Inode {
		t.Fatalf("expected hardlink inode reuse, got %d want %d", alias.Inode, base.Inode)
	}
	if err := hub.ChmodContext(context.Background(), repoName, "docs/base.txt", 0o600); err != nil {
		t.Fatalf("chmod base: %v", err)
	}
	if err := hub.ChownContext(context.Background(), repoName, "docs/base.txt", 1001, 1002); err != nil {
		t.Fatalf("chown base: %v", err)
	}
	if err := hub.ChtimesContext(context.Background(), repoName, "docs/base.txt", 123, 123); err != nil {
		t.Fatalf("chtimes base: %v", err)
	}
	if err := hub.SetXAttrContext(context.Background(), repoName, "docs/base.txt", "user.note", []byte("present")); err != nil {
		t.Fatalf("setxattr base: %v", err)
	}
	symlink, err := hub.SymlinkContext(context.Background(), repoName, "docs/alias.txt", "docs/link.txt")
	if err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if symlink.Symlink == "" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}
	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		info, err := hub.StatPathContext(context.Background(), repoName, "docs/alias.txt")
		if err != nil {
			return false, nil
		}
		return info.Inode == base.Inode && info.NLink == 2 && info.Mode == 0o600 && info.UID == 1001 && info.GID == 1002, nil
	}); err != nil {
		t.Fatalf("wait for hardlink metadata: %v", err)
	}
	aliasInfo, err := hub.StatPathContext(context.Background(), repoName, "docs/alias.txt")
	if err != nil {
		t.Fatalf("stat alias: %v", err)
	}
	if aliasInfo.ModifiedAt != 123 {
		t.Fatalf("unexpected alias modified time: %v", aliasInfo.ModifiedAt)
	}
	attrs, err := hub.ListXAttrContext(context.Background(), repoName, "docs/alias.txt")
	if err != nil {
		t.Fatalf("listxattr alias: %v", err)
	}
	if len(attrs) != 1 || attrs[0] != "user.note" {
		t.Fatalf("unexpected alias xattrs: %v", attrs)
	}
	value, err := hub.GetXAttrContext(context.Background(), repoName, "docs/alias.txt", "user.note")
	if err != nil {
		t.Fatalf("getxattr alias: %v", err)
	}
	if string(value) != "present" {
		t.Fatalf("unexpected xattr value: %q", value)
	}
	target, err := hub.ReadlinkContext(context.Background(), repoName, "docs/link.txt")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "docs/alias.txt" {
		t.Fatalf("unexpected symlink target: %q", target)
	}
	if _, err := hub.ReadFileAtContext(context.Background(), repoName, "docs/link.txt", 0, 4); err == nil {
		t.Fatal("expected symlink readfileat to fail")
	}
	output := filepath.Join(t.TempDir(), "alias.txt")
	if err := hub.DownloadFileContext(context.Background(), repoName, "docs/alias.txt", output); err != nil {
		t.Fatalf("download alias: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read alias output: %v", err)
	}
	if !bytes.Equal(data, []byte("hello live posix")) {
		t.Fatalf("unexpected alias content: %q", data)
	}
	if err := hub.UnlinkContext(context.Background(), repoName, "docs/base.txt"); err != nil {
		t.Fatalf("unlink base: %v", err)
	}
	if err := waitForLiveCondition(t, 30*time.Second, 2*time.Second, func() (bool, error) {
		info, err := hub.StatPathContext(context.Background(), repoName, "docs/alias.txt")
		if err != nil {
			return false, err
		}
		return info.NLink == 1, nil
	}); err != nil {
		t.Fatalf("wait for link count drop: %v", err)
	}
}

func TestLiveGitHubSmoke2GB(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_LIVE_LARGE")

	token := test.RequireValue(t, "GITHUB_TOKEN")

	hub := newLiveHub(t, token, liveLargeSmokeConfig(newProgressHTTPClient(t)))

	repoName := fmt.Sprintf("storhub-live-2gb-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		if cleanupErr := hub.DeleteProject(repoName); cleanupErr != nil {
			t.Logf("cleanup warning: %v", cleanupErr)
		}
	})

	const fileSize = int64(2) << 30
	inputPath := filepath.Join(t.TempDir(), "live2gb.bin")
	outputPath := filepath.Join(t.TempDir(), "live-2gb.out")
	if err := createSparseZeroFile(inputPath, fileSize); err != nil {
		t.Fatalf("create sparse input: %v", err)
	}

	t.Log("uploading 2GB sparse file")
	fileMeta, err := hub.UploadFileContext(context.Background(), repoName, "live2gb.bin", inputPath)
	if err != nil {
		t.Fatalf("upload 2GB file: %v", err)
	}
	if fileMeta.Size != fileSize {
		t.Fatalf("unexpected uploaded size: got %d want %d", fileMeta.Size, fileSize)
	}
	expectedChunks := expectedChunkCount(fileSize, hub.config.ChunkSize)
	if len(fileMeta.Chunks) != expectedChunks {
		t.Fatalf("unexpected chunk count: got %d want %d", len(fileMeta.Chunks), expectedChunks)
	}

	files, err := hub.ListFilesContext(context.Background(), repoName)
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	if len(files) != 1 || files[0].Size != fileSize {
		t.Fatalf("unexpected listed files: %+v", files)
	}

	t.Log("downloading 2GB file; integrity verification runs inside DownloadFile")
	if err := hub.DownloadFileContext(context.Background(), repoName, "live2gb.bin", outputPath); err != nil {
		t.Fatalf("download 2GB file: %v", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatalf("stat output: %v", err)
	}
	if info.Size() != fileSize {
		t.Fatalf("unexpected downloaded size: got %d want %d", info.Size(), fileSize)
	}

	t.Log("purging untracked 2GB file data")
	if err := hub.DeleteFileContext(context.Background(), repoName, "live2gb.bin"); err != nil {
		t.Fatalf("delete metadata entry: %v", err)
	}
	purge, err := hub.PruneProject(repoName, "assets", 0, false)
	if err != nil {
		t.Fatalf("purge untracked 2GB file: %v", err)
	}
	if purge.DeletedReleases != 1 || purge.DeletedAssets != 0 {
		t.Fatalf("unexpected purge result: %+v", purge)
	}
}

func newProgressHTTPClient(t *testing.T) *http.Client {
	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{
		Timeout:   5 * time.Minute,
		Transport: progressTransport{base: baseTransport, t: t},
	}
}

type progressTransport struct {
	base http.RoundTripper
	t    *testing.T
}

func (p progressTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && strings.Contains(req.URL.Host, "uploads.github.com") {
		p.t.Logf("starting upload %s", req.URL.String())
		req.Body = newProgressReadCloser(req.Body, 128<<20, func(written int64) {
			p.t.Logf("uploaded %d MB", written>>20)
		})
	}
	resp, err := p.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.Body != nil && strings.Contains(req.URL.Path, "/releases/assets/") {
		p.t.Logf("starting download %s", req.URL.String())
		resp.Body = newProgressReadCloser(resp.Body, 128<<20, func(read int64) {
			p.t.Logf("downloaded %d MB", read>>20)
		})
	}
	return resp, nil
}

type progressReadCloser struct {
	base      io.ReadCloser
	step      int64
	total     int64
	nextLog   int64
	onAdvance func(int64)
}

func newProgressReadCloser(base io.ReadCloser, step int64, onAdvance func(int64)) *progressReadCloser {
	return &progressReadCloser{base: base, step: step, nextLog: step, onAdvance: onAdvance}
}

func (p *progressReadCloser) Read(buf []byte) (int, error) {
	n, err := p.base.Read(buf)
	if n > 0 {
		total := atomic.AddInt64(&p.total, int64(n))
		for total >= p.nextLog {
			if p.onAdvance != nil {
				p.onAdvance(p.nextLog)
			}
			p.nextLog += p.step
		}
	}
	return n, err
}

func (p *progressReadCloser) Close() error {
	return p.base.Close()
}

func createSparseZeroFile(path string, size int64) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Truncate(size)
}

func waitForLiveCondition(t *testing.T, timeout, interval time.Duration, check func() (bool, error)) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %v", timeout)
		}
		time.Sleep(interval)
	}
}
