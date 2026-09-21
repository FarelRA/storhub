package storage

import (
	"bytes"
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

func TestDirectoryOperationsAndPathSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.MkdirContext(context.Background(), "projecttree", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.MkdirContext(context.Background(), "projecttree", "docs/specs"); err != nil {
		t.Fatalf("mkdir docs/specs: %v", err)
	}
	entries, err := hub.ReadDirContext(context.Background(), "projecttree", "")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "docs" || !entries[0].IsDir {
		t.Fatalf("unexpected root entries: %+v", entries)
	}
	info, err := hub.StatPathContext(context.Background(), "projecttree", "docs/specs")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir {
		t.Fatalf("expected directory info, got %+v", info)
	}
	if err := hub.RmdirContext(context.Background(), "projecttree", "docs"); err == nil {
		t.Fatal("expected non-empty rmdir to fail")
	}
}

func TestCreateRenameReadWriteAndTruncateFileOperations(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 4, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	if err := hub.MkdirContext(context.Background(), "projectfsops", "notes"); err != nil {
		t.Fatalf("mkdir notes: %v", err)
	}
	created, err := hub.CreateFileContext(context.Background(), "projectfsops", "notes/todo.txt")
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	if created.Size != 0 {
		t.Fatalf("expected empty file, got %+v", created)
	}
	if _, err := hub.WriteFileAtContext(context.Background(), "projectfsops", "notes/todo.txt", 0, []byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	if _, err := hub.WriteFileAtContext(context.Background(), "projectfsops", "notes/todo.txt", 7, []byte("world")); err != nil {
		t.Fatalf("write beyond eof: %v", err)
	}
	data, err := hub.ReadFileAtContext(context.Background(), "projectfsops", "notes/todo.txt", 0, 12)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(data, []byte{'h', 'e', 'l', 'l', 'o', 0, 0, 'w', 'o', 'r', 'l', 'd'}) {
		t.Fatalf("unexpected file data: %v", data)
	}
	if _, err := hub.AppendFileContext(context.Background(), "projectfsops", "notes/todo.txt", []byte("!")); err != nil {
		t.Fatalf("append file: %v", err)
	}
	if _, err := hub.TruncateFileContext(context.Background(), "projectfsops", "notes/todo.txt", 5); err != nil {
		t.Fatalf("truncate shrink: %v", err)
	}
	if err := hub.RenameContext(context.Background(), "projectfsops", "notes/todo.txt", "notes/done.txt"); err != nil {
		t.Fatalf("rename file: %v", err)
	}
	output := filepath.Join(t.TempDir(), "done.txt")
	if err := hub.DownloadFileContext(context.Background(), "projectfsops", "notes/done.txt", output); err != nil {
		t.Fatalf("download renamed file: %v", err)
	}
	assertFileContent(t, output, []byte("hello"))
	entries, err := hub.ReadDirContext(context.Background(), "projectfsops", "notes")
	if err != nil {
		t.Fatalf("readdir notes: %v", err)
	}
	if len(entries) != 1 || entries[0].Path != "notes/done.txt" {
		t.Fatalf("unexpected notes entries: %+v", entries)
	}
	stats, err := hub.StatFSContext(context.Background(), "projectfsops")
	if err != nil {
		t.Fatalf("statfs: %v", err)
	}
	if stats.Files != 1 || stats.Directories != 1 || stats.Bytes != 5 {
		t.Fatalf("unexpected fs stats: %+v", stats)
	}
}

func TestCreateFileStoresEmptyMetadataWithoutAssetUpload(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.MkdirContext(context.Background(), "projectemptyupload", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	meta, err := hub.CreateFileContext(context.Background(), "projectemptyupload", "docs/empty.txt")
	if err != nil {
		t.Fatalf("create empty file: %v", err)
	}
	if meta.Size != 0 {
		t.Fatalf("expected zero-sized file metadata, got %+v", meta)
	}
	if len(meta.Chunks) != 0 {
		t.Fatalf("expected empty file to have no chunks, got %+v", meta.Chunks)
	}

	if uploadCalls.Load() != 0 {
		t.Fatalf("expected no asset uploads for empty file, got %d", uploadCalls.Load())
	}
}

func TestFilesystemEdgeCasesAndRootSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())

	// POSIX: mkdir on the always-existing root reports EEXIST.
	if err := hub.MkdirContext(context.Background(), "projectfsedge", "."); err == nil {
		t.Fatal("expected mkdir on root to fail")
	}
	if err := hub.RmdirContext(context.Background(), "projectfsedge", ""); err == nil {
		t.Fatal("expected rmdir root to fail")
	}
	if _, err := hub.CreateFileContext(context.Background(), "projectfsedge", ""); err == nil {
		t.Fatal("expected empty create path to fail")
	}
	if err := hub.MkdirContext(context.Background(), "projectfsedge", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.MkdirContext(context.Background(), "projectfsedge", "docs"); err == nil {
		t.Fatal("expected duplicate mkdir to fail")
	}
	if err := hub.MkdirContext(context.Background(), "projectfsedge", "docs/nested"); err != nil {
		t.Fatalf("mkdir docs/nested: %v", err)
	}
	if _, err := hub.CreateFileContext(context.Background(), "projectfsedge", "docs/nested/file.txt"); err != nil {
		t.Fatalf("create nested file: %v", err)
	}
	if err := hub.UnlinkContext(context.Background(), "projectfsedge", "docs"); err == nil {
		t.Fatal("expected unlink directory path to fail")
	}
	if err := hub.RenameContext(context.Background(), "projectfsedge", "docs", "docs/nested/docs"); err == nil {
		t.Fatal("expected renaming directory into itself to fail")
	}
	rootInfo, err := hub.StatPathContext(context.Background(), "projectfsedge", "")
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if !rootInfo.IsDir || rootInfo.Path != "" {
		t.Fatalf("unexpected root stat: %+v", rootInfo)
	}
	entries, err := hub.ReadDirContext(context.Background(), "projectfsedge", "")
	if err != nil {
		t.Fatalf("readdir root: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "docs" || !entries[0].IsDir {
		t.Fatalf("unexpected root entries: %+v", entries)
	}
}

func TestRenameDirectoryMovesTree(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	if err := hub.MkdirContext(context.Background(), "projectdirrename", "a"); err != nil {
		t.Fatalf("mkdir a: %v", err)
	}
	if err := hub.MkdirContext(context.Background(), "projectdirrename", "a/b"); err != nil {
		t.Fatalf("mkdir a/b: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "nested.txt", []byte("payload"))
	if _, err := hub.UploadFileContext(context.Background(), "projectdirrename", "a/b/file.txt", input); err != nil {
		t.Fatalf("upload nested file: %v", err)
	}
	if err := hub.RenameContext(context.Background(), "projectdirrename", "a", "renamed"); err != nil {
		t.Fatalf("rename dir: %v", err)
	}
	if _, err := hub.StatPathContext(context.Background(), "projectdirrename", "renamed/b/file.txt"); err != nil {
		t.Fatalf("stat moved file: %v", err)
	}
	if _, err := hub.StatPathContext(context.Background(), "projectdirrename", "a/b/file.txt"); err == nil {
		t.Fatal("expected old path lookup to fail")
	}
}

func TestPOSIXMetadataOpsHardlinksSymlinksAndXAttrs(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	// Ownership and chown operations require an explicitly identified
	// privileged caller; absent identities fail closed to the process user.
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})

	if err := hub.MkdirContext(ctx, "projectposix", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "base.txt", []byte("hello world"))
	base, err := hub.UploadFileContext(ctx, "projectposix", "docs/base.txt", input)
	if err != nil {
		t.Fatalf("upload base file: %v", err)
	}
	linked, err := hub.LinkContext(ctx, "projectposix", "docs/base.txt", "docs/alias.txt")
	if err != nil {
		t.Fatalf("create hard link: %v", err)
	}
	if linked.Inode != base.Inode {
		t.Fatalf("expected hard link to reuse inode, base=%d alias=%d", base.Inode, linked.Inode)
	}
	baseInfo, err := hub.StatPathContext(ctx, "projectposix", "docs/base.txt")
	if err != nil {
		t.Fatalf("stat base: %v", err)
	}
	aliasInfo, err := hub.StatPathContext(ctx, "projectposix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("stat alias: %v", err)
	}
	if baseInfo.Inode != aliasInfo.Inode || baseInfo.NLink != 2 || aliasInfo.NLink != 2 {
		t.Fatalf("unexpected hard link stats: base=%+v alias=%+v", baseInfo, aliasInfo)
	}
	if err := hub.ChmodContext(ctx, "projectposix", "docs/base.txt", 0o600); err != nil {
		t.Fatalf("chmod hardlink family: %v", err)
	}
	if err := hub.ChownContext(ctx, "projectposix", "docs/alias.txt", 123, 456); err != nil {
		t.Fatalf("chown hardlink family: %v", err)
	}
	if err := hub.ChtimesContext(ctx, "projectposix", "docs/base.txt", 10, 20); err != nil {
		t.Fatalf("chtimes hardlink family: %v", err)
	}
	if err := hub.SetXAttrContext(ctx, "projectposix", "docs/alias.txt", "user.note", []byte("linked")); err != nil {
		t.Fatalf("setxattr hardlink family: %v", err)
	}
	attrs, err := hub.ListXAttrContext(ctx, "projectposix", "docs/base.txt")
	if err != nil {
		t.Fatalf("listxattr base: %v", err)
	}
	if len(attrs) != 1 || attrs[0] != "user.note" {
		t.Fatalf("unexpected xattrs: %v", attrs)
	}
	value, err := hub.GetXAttrContext(ctx, "projectposix", "docs/base.txt", "user.note")
	if err != nil {
		t.Fatalf("getxattr base: %v", err)
	}
	if string(value) != "linked" {
		t.Fatalf("unexpected xattr value: %q", value)
	}
	updated, err := hub.WriteFileAtContext(ctx, "projectposix", "docs/base.txt", 6, []byte("storhub"))
	if err != nil {
		t.Fatalf("write through hardlink family: %v", err)
	}
	if updated.Inode != base.Inode {
		t.Fatalf("expected inode preservation after write, got %d want %d", updated.Inode, base.Inode)
	}
	aliasDownload := filepath.Join(t.TempDir(), "alias.txt")
	if err := hub.DownloadFileContext(ctx, "projectposix", "docs/alias.txt", aliasDownload); err != nil {
		t.Fatalf("download alias: %v", err)
	}
	assertFileContent(t, aliasDownload, []byte("hello storhub"))
	aliasInfo, err = hub.StatPathContext(ctx, "projectposix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("restat alias: %v", err)
	}
	if aliasInfo.Mode != 0o600 || aliasInfo.UID != 123 || aliasInfo.GID != 456 {
		t.Fatalf("hardlink family metadata did not propagate: %+v", aliasInfo)
	}
	if aliasInfo.ModifiedAt <= time.Unix(20, 0).Unix() {
		t.Fatalf("expected modified time to advance after write, got %v", aliasInfo.ModifiedAt)
	}
	if err := hub.RemoveXAttrContext(ctx, "projectposix", "docs/base.txt", "user.note"); err != nil {
		t.Fatalf("removexattr family: %v", err)
	}
	attrs, err = hub.ListXAttrContext(ctx, "projectposix", "docs/base.txt")
	if err != nil {
		t.Fatalf("listxattr after remove: %v", err)
	}
	if len(attrs) != 0 {
		t.Fatalf("expected xattrs to be removed, got %v", attrs)
	}
	if err := hub.UnlinkContext(ctx, "projectposix", "docs/base.txt"); err != nil {
		t.Fatalf("unlink one hardlink: %v", err)
	}
	aliasInfo, err = hub.StatPathContext(ctx, "projectposix", "docs/alias.txt")
	if err != nil {
		t.Fatalf("stat alias after unlink: %v", err)
	}
	if aliasInfo.NLink != 1 {
		t.Fatalf("expected remaining hardlink count 1, got %+v", aliasInfo)
	}
	symlink, err := hub.SymlinkContext(ctx, "projectposix", "alias.txt", "docs/alias-link")
	if err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if symlink.Symlink != "alias.txt" {
		t.Fatalf("unexpected symlink metadata: %+v", symlink)
	}
	target, err := hub.ReadlinkContext(ctx, "projectposix", "docs/alias-link")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "alias.txt" {
		t.Fatalf("unexpected symlink target: %q", target)
	}
	linkInfo, err := hub.StatPathContext(ctx, "projectposix", "docs/alias-link")
	if err != nil {
		t.Fatalf("stat symlink: %v", err)
	}
	if !linkInfo.IsSymlink || linkInfo.SymlinkTarget != "alias.txt" {
		t.Fatalf("unexpected symlink stat: %+v", linkInfo)
	}
	// Read has open() semantics and follows the final symlink.
	if data, err := hub.ReadFileAtContext(ctx, "projectposix", "docs/alias-link", 0, 4); err != nil || string(data) != "hell" {
		t.Fatalf("read through symlink must return the target bytes, got %q err=%v", data, err)
	}
	if err := hub.SetXAttrContext(ctx, "projectposix", "", "user.root", []byte("rooted")); err != nil {
		t.Fatalf("set root xattr: %v", err)
	}
	rootAttrs, err := hub.ListXAttrContext(ctx, "projectposix", "")
	if err != nil {
		t.Fatalf("list root xattrs: %v", err)
	}
	if len(rootAttrs) != 1 || rootAttrs[0] != "user.root" {
		t.Fatalf("unexpected root xattrs: %v", rootAttrs)
	}
}
