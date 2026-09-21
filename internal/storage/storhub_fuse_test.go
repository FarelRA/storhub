package storage

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	"github.com/FarelRA/storhub/internal/test"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestFUSEAdapterCallbacksAndHandles(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()

	if err := hub.MkdirContext(ctx, "projectfuse", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "fuse.txt", []byte("hello world"))
	if _, err := hub.UploadFileContext(ctx, "projectfuse", "docs/file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfuse", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()

	var rootOut fuse.EntryOut
	_, errno := fsys.RootNode().Lookup(ctx, "docs", &rootOut)
	if errno != 0 {
		t.Fatalf("lookup docs failed: %v", errno)
	}
	docsEntry, err := hub.StatPathContext(ctx, "projectfuse", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if docsNode == nil {
		t.Fatal("expected docs node")
	}
	dirStream, errno := docsNode.Readdir(ctx)
	if errno != 0 {
		t.Fatalf("readdir docs failed: %v", errno)
	}
	// Skip "." and ".." entries to find "file.txt"
	var entry fuse.DirEntry
	for {
		entry, errno = dirStream.Next()
		if errno != 0 {
			t.Fatalf("readdir next failed: %v", errno)
		}
		if entry.Name != "." && entry.Name != ".." {
			break
		}
	}
	if entry.Name != "file.txt" {
		t.Fatalf("unexpected directory entry: %+v", entry)
	}
	var fileOut fuse.EntryOut
	_, errno = docsNode.Lookup(ctx, "file.txt", &fileOut)
	if errno != 0 {
		t.Fatalf("lookup file failed: %v", errno)
	}
	fileEntry, err := hub.StatPathContext(ctx, "projectfuse", "docs/file.txt")
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	fileNode := fsys.EnsureNodeForTest(ctx, fileEntry)
	if fileNode == nil {
		t.Fatal("expected file node")
	}
	handleAny, _, errno := fileNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open file failed: %v", errno)
	}
	handle, ok := handleAny.(*fusefs.TestHandle)
	if !ok {
		t.Fatalf("unexpected handle type: %T", handleAny)
	}
	if written, errno := handle.Write(ctx, []byte("FUSE"), 6); errno != 0 || written != 4 {
		t.Fatalf("write failed: written=%d errno=%v", written, errno)
	}
	if errno := handle.Flush(ctx); errno != 0 {
		t.Fatalf("flush failed: %v", errno)
	}
	// With writeback caching, close(2)-time Flush may be the only
	// durability signal the kernel sends, so Flush commits the dirty
	// overlay. The remote file is therefore already updated here; Fsync
	// below is an idempotent second commit.
	postFlush := filepath.Join(t.TempDir(), "fuse-postflush.out")
	if err := hub.DownloadFileContext(ctx, "projectfuse", "docs/file.txt", postFlush); err != nil {
		t.Fatalf("download after flush: %v", err)
	}
	assertFileContent(t, postFlush, []byte("hello FUSEd"))
	if errno := handle.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync failed: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "fuse.out")
	if err := hub.DownloadFileContext(ctx, "projectfuse", "docs/file.txt", output); err != nil {
		t.Fatalf("download after fuse write: %v", err)
	}
	assertFileContent(t, output, []byte("hello FUSEd"))
	if errno := fileNode.Setxattr(ctx, "user.cache", []byte("warm"), 0); errno != 0 {
		t.Fatalf("setxattr via node failed: %v", errno)
	}
	size, errno := fileNode.Listxattr(ctx, nil)
	if errno != 0 {
		t.Fatalf("listxattr size failed: %v", errno)
	}
	buf := make([]byte, size)
	if _, errno := fileNode.Listxattr(ctx, buf); errno != 0 {
		t.Fatalf("listxattr payload failed: %v", errno)
	}
	if string(bytes.TrimRight(buf, "\x00")) != "user.cache" {
		t.Fatalf("unexpected xattr list payload: %q", buf)
	}
	getBuf := make([]byte, 16)
	n, errno := fileNode.Getxattr(ctx, "user.cache", getBuf)
	if errno != 0 {
		t.Fatalf("getxattr failed: %v", errno)
	}
	if string(getBuf[:n]) != "warm" {
		t.Fatalf("unexpected xattr data: %q", getBuf[:n])
	}
	lock := &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}
	if errno := handle.Setlk(ctx, 1, lock, 0); errno != 0 {
		t.Fatalf("setlk failed: %v", errno)
	}
	var outLock fuse.FileLock
	if errno := handle.Getlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0, &outLock); errno != 0 {
		t.Fatalf("getlk failed: %v", errno)
	}
	if outLock.Typ != syscall.F_WRLCK {
		t.Fatalf("expected active write lock, got %+v", outLock)
	}
	if errno := handle.Setlk(ctx, 1, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_UNLCK}, 0); errno != 0 {
		t.Fatalf("unlock failed: %v", errno)
	}
	if errno := fileNode.Removexattr(ctx, "user.cache"); errno != 0 {
		t.Fatalf("removexattr failed: %v", errno)
	}
	var createOut fuse.EntryOut
	createdInode, createHandleAny, _, errno := docsNode.Create(ctx, "created.txt", syscall.O_RDWR, 0o640, &createOut)
	if errno != 0 {
		t.Fatalf("create failed: %v", errno)
	}
	_ = createdInode
	createdHandle := createHandleAny.(*fusefs.TestHandle)
	if written, errno := createdHandle.Write(ctx, []byte("created"), 0); errno != 0 || written != 7 {
		t.Fatalf("write created file failed: written=%d errno=%v", written, errno)
	}
	if errno := createdHandle.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync created file failed: %v", errno)
	}
	if errno := createdHandle.Release(ctx); errno != 0 {
		t.Fatalf("release created file failed: %v", errno)
	}
	if _, errno := docsNode.Symlink(ctx, "docs/file.txt", "link.txt", &createOut); errno != 0 {
		t.Fatalf("node symlink failed: %v", errno)
	}
	_, errno = docsNode.Lookup(ctx, "link.txt", &createOut)
	if errno != 0 {
		t.Fatalf("lookup symlink failed: %v", errno)
	}
	linkEntry, err := hub.StatPathContext(ctx, "projectfuse", "docs/link.txt")
	if err != nil {
		t.Fatalf("stat link: %v", err)
	}
	linkNode := fsys.EnsureNodeForTest(ctx, linkEntry)
	target, errno := linkNode.Readlink(ctx)
	if errno != 0 || string(target) != "docs/file.txt" {
		t.Fatalf("unexpected readlink result: target=%q errno=%v", target, errno)
	}
	if _, errno := docsNode.Link(ctx, fileNode, "hard.txt", &createOut); errno != 0 {
		t.Fatalf("node hard link failed: %v", errno)
	}
	if errno := docsNode.Rename(ctx, "created.txt", docsNode, "file.txt", 0); errno != 0 {
		t.Fatalf("rename with replace failed: %v", errno)
	}
	replaced := filepath.Join(t.TempDir(), "replaced.txt")
	if err := hub.DownloadFileContext(ctx, "projectfuse", "docs/file.txt", replaced); err != nil {
		t.Fatalf("download replaced file: %v", err)
	}
	assertFileContent(t, replaced, []byte("created"))
	if errno := docsNode.Unlink(ctx, "hard.txt"); errno != 0 {
		t.Fatalf("unlink hard link failed: %v", errno)
	}
	var attrOut fuse.AttrOut
	if errno := docsNode.Getattr(ctx, nil, &attrOut); errno != 0 {
		t.Fatalf("getattr docs failed: %v", errno)
	}
	if attrOut.Ino == 0 || attrOut.Mode&syscall.S_IFDIR == 0 {
		t.Fatalf("unexpected directory attr: %+v", attrOut.Attr)
	}
	var statfs fuse.StatfsOut
	if errno := fsys.RootNode().Statfs(ctx, &statfs); errno != 0 {
		t.Fatalf("statfs failed: %v", errno)
	}
	if statfs.Files == 0 || statfs.Bsize == 0 {
		t.Fatalf("unexpected statfs result: %+v", statfs)
	}
	if _, errno := docsNode.Mkdir(ctx, "subdir", 0o755, &createOut); errno != 0 {
		t.Fatalf("mkdir via node failed: %v", errno)
	}
	if errno := docsNode.Rmdir(ctx, "subdir"); errno != 0 {
		t.Fatalf("rmdir via node failed: %v", errno)
	}
	if errno := handle.Release(ctx); errno != 0 {
		t.Fatalf("release handle failed: %v", errno)
	}
}

func TestFUSEOptionalMountLifecycle(t *testing.T) {
	test.RequireFlag(t, "STORHUB_RUN_FUSE")
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skip("/dev/fuse unavailable")
	}
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	input := writeTempFile(t, t.TempDir(), "mount.txt", []byte("mounted"))
	if _, err := hub.UploadFileContext(context.Background(), "projectfusemount", "mount.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfusemount", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	mountPoint := filepath.Join(t.TempDir(), "mnt")
	if err := os.MkdirAll(mountPoint, 0o755); err != nil {
		t.Fatalf("mkdir mountpoint: %v", err)
	}
	if err := fsys.Mount(mountPoint); err != nil {
		t.Fatalf("mount fuse fs: %v", err)
	}
	mountedPath := filepath.Join(mountPoint, "mount.txt")
	data, err := os.ReadFile(mountedPath)
	if err != nil {
		t.Fatalf("read mounted file: %v", err)
	}
	if string(data) != "mounted" {
		t.Fatalf("unexpected mounted file content: %q", data)
	}
	newFile := filepath.Join(mountPoint, "created.txt")
	createdHandle, err := os.OpenFile(newFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o640)
	if err != nil {
		t.Fatalf("open mounted file for write: %v", err)
	}
	if _, err := createdHandle.Write([]byte("created via mount")); err != nil {
		t.Fatalf("write mounted file: %v", err)
	}
	if err := createdHandle.Sync(); err != nil {
		t.Fatalf("sync mounted file: %v", err)
	}
	if err := createdHandle.Close(); err != nil {
		t.Fatalf("close mounted file: %v", err)
	}
	if got, err := os.ReadFile(newFile); err != nil || string(got) != "created via mount" {
		t.Fatalf("read created mounted file: got=%q err=%v", got, err)
	}
	hardPath := filepath.Join(mountPoint, "hard.txt")
	if err := os.Link(newFile, hardPath); err != nil {
		t.Fatalf("create hardlink on mount: %v", err)
	}
	linkPath := filepath.Join(mountPoint, "sym.txt")
	if err := os.Symlink("created.txt", linkPath); err != nil {
		t.Fatalf("create symlink on mount: %v", err)
	}
	if target, err := os.Readlink(linkPath); err != nil || target != "created.txt" {
		t.Fatalf("read mounted symlink: target=%q err=%v", target, err)
	}
	renamedPath := filepath.Join(mountPoint, "renamed.txt")
	if err := os.Rename(newFile, renamedPath); err != nil {
		t.Fatalf("rename mounted file: %v", err)
	}
	if err := os.Chmod(renamedPath, 0o600); err != nil {
		t.Fatalf("chmod mounted file: %v", err)
	}
	if err := os.WriteFile(hardPath, []byte("hardlink update"), 0o600); err != nil {
		t.Fatalf("write through mounted hardlink: %v", err)
	}
	if got, err := os.ReadFile(renamedPath); err != nil || string(got) != "hardlink update" {
		t.Fatalf("expected hardlink content reflection: got=%q err=%v", got, err)
	}
	if err := os.Chmod(renamedPath, 0o000); err != nil {
		t.Fatalf("chmod mounted file to 000: %v", err)
	}
	if _, err := os.ReadFile(renamedPath); err == nil {
		t.Fatal("expected mounted permission denial after chmod 000")
	}
	if err := os.Chmod(renamedPath, 0o600); err != nil {
		t.Fatalf("restore chmod mounted file: %v", err)
	}
	info, err := os.Lstat(renamedPath)
	if err != nil {
		t.Fatalf("lstat renamed file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("unexpected mounted mode: %o", info.Mode().Perm())
	}
	if err := testSetUserXattr(renamedPath, "user.mount", []byte("warm")); err == nil {
		got, err := testGetUserXattr(renamedPath, "user.mount")
		if err != nil {
			t.Fatalf("get mounted xattr: %v", err)
		}
		if string(got) != "warm" {
			t.Fatalf("unexpected mounted xattr: %q", got)
		}
	}
	if err := fsys.Unmount(); err != nil {
		t.Fatalf("unmount fuse fs: %v", err)
	}
}

func TestFUSECloseIdempotent(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	fsys, err := hub.NewFUSE("projectfuseclose", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := fsys.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestFUSEHandleRenameAndUnlinkSemantics(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "projectfusesemantics", "dir"); err != nil {
		t.Fatalf("mkdir dir: %v", err)
	}
	if err := hub.MkdirContext(ctx, "projectfusesemantics", "dir/sub"); err != nil {
		t.Fatalf("mkdir dir/sub: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "semantics.txt", []byte("payload"))
	if _, err := hub.UploadFileContext(ctx, "projectfusesemantics", "dir/sub/file.txt", input); err != nil {
		t.Fatalf("upload file: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfusesemantics", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	dirEntry, err := hub.StatPathContext(ctx, "projectfusesemantics", "dir")
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	dirNode := fsys.EnsureNodeForTest(ctx, dirEntry)
	var out fuse.EntryOut
	_, errno := dirNode.Lookup(ctx, "sub", &out)
	if errno != 0 {
		t.Fatalf("lookup sub: %v", errno)
	}
	fileEntry, err := hub.StatPathContext(ctx, "projectfusesemantics", "dir/sub/file.txt")
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	fileNode := fsys.EnsureNodeForTest(ctx, fileEntry)
	hAny, _, errno := fileNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open file: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	if errno := fsys.RootNode().Rename(ctx, "dir", fsys.RootNode(), "renamed", 0); errno != 0 {
		t.Fatalf("rename dir: %v", errno)
	}
	if written, errno := h.Write(ctx, []byte("R"), 0); errno != 0 || written != 1 {
		t.Fatalf("write after rename: written=%d errno=%v", written, errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync after rename: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "renamed-semantic.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusesemantics", "renamed/sub/file.txt", output); err != nil {
		t.Fatalf("download renamed path: %v", err)
	}
	assertFileContent(t, output, []byte("Rayload"))
	if _, err := hub.StatPathContext(ctx, "projectfusesemantics", "dir/sub/file.txt"); err == nil {
		t.Fatal("expected old path to be gone after rename")
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release renamed handle: %v", errno)
	}
	unlinkEntry, err := hub.StatPathContext(ctx, "projectfusesemantics", "renamed/sub/file.txt")
	if err != nil {
		t.Fatalf("stat unlink file: %v", err)
	}
	unlinkNode := fsys.EnsureNodeForTest(ctx, unlinkEntry)
	hAny, _, errno = unlinkNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open unlink file: %v", errno)
	}
	h = hAny.(*fusefs.TestHandle)
	parentEntry, err := hub.StatPathContext(ctx, "projectfusesemantics", "renamed/sub")
	if err != nil {
		t.Fatalf("stat parent dir: %v", err)
	}
	parentNode := fsys.EnsureNodeForTest(ctx, parentEntry)
	if errno := parentNode.Unlink(ctx, "file.txt"); errno != 0 {
		t.Fatalf("unlink open file: %v", errno)
	}
	if written, errno := h.Write(ctx, []byte("gone"), 0); errno != 0 || written != 4 {
		t.Fatalf("write after unlink: written=%d errno=%v", written, errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync after unlink: %v", errno)
	}
	readBuf := make([]byte, 4)
	res, errno := h.Read(ctx, readBuf, 0)
	if errno != 0 {
		t.Fatalf("read after unlink: %v", errno)
	}
	readData, status := res.Bytes(readBuf)
	if status != 0 {
		t.Fatalf("read result bytes: %v", status)
	}
	if string(readData) != "gone" {
		t.Fatalf("unexpected read after unlink: %q", readData)
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release unlinked handle: %v", errno)
	}
	if _, err := hub.StatPathContext(ctx, "projectfusesemantics", "renamed/sub/file.txt"); err == nil {
		t.Fatal("expected unlinked file to remain absent after release")
	}
}

func TestFUSEReadOnlyHandleSurvivesPathLoss(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "projectfusereadonlyloss", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	oldPath := writeTempFile(t, t.TempDir(), "old.txt", []byte("old-data"))
	newPath := writeTempFile(t, t.TempDir(), "new.txt", []byte("new-data"))
	if _, err := hub.UploadFileContext(ctx, "projectfusereadonlyloss", "docs/victim.txt", oldPath); err != nil {
		t.Fatalf("upload victim: %v", err)
	}
	if _, err := hub.UploadFileContext(ctx, "projectfusereadonlyloss", "docs/replacement.txt", newPath); err != nil {
		t.Fatalf("upload replacement: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfusereadonlyloss", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	victimEntry, err := hub.StatPathContext(ctx, "projectfusereadonlyloss", "docs/victim.txt")
	if err != nil {
		t.Fatalf("stat victim: %v", err)
	}
	victimNode := fsys.EnsureNodeForTest(ctx, victimEntry)
	roAny, _, errno := victimNode.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("open readonly victim: %v", errno)
	}
	ro := roAny.(*fusefs.TestHandle)
	docsEntry, err := hub.StatPathContext(ctx, "projectfusereadonlyloss", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if errno := docsNode.Unlink(ctx, "victim.txt"); errno != 0 {
		t.Fatalf("unlink victim: %v", errno)
	}
	buf := make([]byte, 16)
	res, errno := ro.Read(ctx, buf, 0)
	if errno != 0 {
		t.Fatalf("read readonly unlinked handle: %v", errno)
	}
	got, status := res.Bytes(buf)
	if status != 0 {
		t.Fatalf("bytes readonly unlinked handle: %v", status)
	}
	if string(got) != "old-data" {
		t.Fatalf("unexpected readonly unlinked data: %q", got)
	}
	if errno := ro.Release(ctx); errno != 0 {
		t.Fatalf("release readonly unlinked handle: %v", errno)
	}

	if _, err := hub.UploadFileContext(ctx, "projectfusereadonlyloss", "docs/victim.txt", oldPath); err != nil {
		t.Fatalf("re-upload victim: %v", err)
	}
	victimEntry, err = hub.StatPathContext(ctx, "projectfusereadonlyloss", "docs/victim.txt")
	if err != nil {
		t.Fatalf("restat victim: %v", err)
	}
	// The re-upload minted a fresh inode for the recreated file; drop the
	// mount's cached node for the old identity the same way a kernel
	// re-Lookup would after invalidation.
	fsys.ResetNodeForTest("docs/victim.txt")
	victimNode = fsys.EnsureNodeForTest(ctx, victimEntry)
	roAny, _, errno = victimNode.Open(ctx, syscall.O_RDONLY)
	if errno != 0 {
		t.Fatalf("reopen readonly victim: %v", errno)
	}
	ro = roAny.(*fusefs.TestHandle)
	if errno := docsNode.Rename(ctx, "replacement.txt", docsNode, "victim.txt", 0); errno != 0 {
		t.Fatalf("rename replacement over victim: %v", errno)
	}
	res, errno = ro.Read(ctx, buf, 0)
	// Pinned reads are deterministic: the handle captured its metadata
	// snapshot at open, so a rename over the path cannot change what this
	// returns. No settling window exists anymore.
	if errno != 0 {
		t.Fatalf("read readonly replaced handle: %v", errno)
	}
	got = mustBytes(t, res, buf)
	if string(got) != "old-data" {
		t.Fatalf("unexpected readonly replaced data: %q", got)
	}
	if errno := ro.Release(ctx); errno != 0 {
		t.Fatalf("release readonly replaced handle: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "replaced-readonly.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusereadonlyloss", "docs/victim.txt", output); err != nil {
		t.Fatalf("download replaced victim: %v", err)
	}
	assertFileContent(t, output, []byte("new-data"))
}

func TestFUSEConcurrentWritableHandlesShareState(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "shared.txt", []byte("abcdefghij"))
	if _, err := hub.UploadFileContext(ctx, "projectfusesharedwrites", "shared.txt", input); err != nil {
		t.Fatalf("upload shared file: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfusesharedwrites", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusesharedwrites", "shared.txt")
	if err != nil {
		t.Fatalf("stat shared file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	h1Any, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle1: %v", errno)
	}
	h2Any, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle2: %v", errno)
	}
	h1 := h1Any.(*fusefs.TestHandle)
	h2 := h2Any.(*fusefs.TestHandle)
	if written, errno := h1.Write(ctx, []byte("HELLO"), 0); errno != 0 || written != 5 {
		t.Fatalf("write handle1: written=%d errno=%v", written, errno)
	}
	if written, errno := h2.Write(ctx, []byte("WORLD"), 5); errno != 0 || written != 5 {
		t.Fatalf("write handle2: written=%d errno=%v", written, errno)
	}
	if errno := h1.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync handle1: %v", errno)
	}
	output := filepath.Join(t.TempDir(), "shared-writes.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusesharedwrites", "shared.txt", output); err != nil {
		t.Fatalf("download shared file: %v", err)
	}
	assertFileContent(t, output, []byte("HELLOWORLD"))
	if errno := h1.Release(ctx); errno != 0 {
		t.Fatalf("release handle1: %v", errno)
	}
	if errno := h2.Release(ctx); errno != 0 {
		t.Fatalf("release handle2: %v", errno)
	}
}

func TestFUSEPartialWritebackAvoidsFullMaterializeAndReupload(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "large.txt", []byte("abcdefghijklmnopqrstuvwx"))
	if _, err := hub.UploadFileContext(ctx, "projectfusepartialwriteback", "large.txt", input); err != nil {
		t.Fatalf("upload large file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfusepartialwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusepartialwriteback", "large.txt")
	if err != nil {
		t.Fatalf("stat large file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	if written, errno := h.Write(ctx, []byte("Z"), 10); errno != 0 || written != 1 {
		t.Fatalf("single-byte overwrite: written=%d errno=%v", written, errno)
	}
	if got := assetDownloadCalls.Load(); got != 0 {
		t.Fatalf("expected no asset download during open/write, got %d", got)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync partial overwrite: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 1 {
		t.Fatalf("expected one uploaded patch chunk, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "partial-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusepartialwriteback", "large.txt", output); err != nil {
		t.Fatalf("download partially updated file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghijZlmnopqrstuvwx"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release handle: %v", errno)
	}
}

func TestFUSEAppendWritebackUsesPatchPath(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "append.txt", []byte("abcdefgh"))
	if _, err := hub.UploadFileContext(ctx, "projectfuseappendwriteback", "append.txt", input); err != nil {
		t.Fatalf("upload append file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfuseappendwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfuseappendwriteback", "append.txt")
	if err != nil {
		t.Fatalf("stat append file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR|syscall.O_APPEND)
	if errno != 0 {
		t.Fatalf("open append handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	// The kernel VFS positions every O_APPEND write at EOF before the
	// FUSE_WRITE is issued (pwrite offsets are ignored for O_APPEND
	// fds), so the handler receives the end offset, not 0. Forcing a
	// sub-EOF offset to EOF here would corrupt merged writeback replays
	// (kernel 7 vs server 11 divergence); sub-EOF offsets are honored
	// so retransmits stay idempotent.
	if written, errno := h.Write(ctx, []byte("XYZ"), 8); errno != 0 || written != 3 {
		t.Fatalf("append write: written=%d errno=%v", written, errno)
	}
	if got := assetDownloadCalls.Load(); got != 0 {
		t.Fatalf("expected no asset download during append write, got %d", got)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync append: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 1 {
		t.Fatalf("expected one uploaded append chunk, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "append-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "projectfuseappendwriteback", "append.txt", output); err != nil {
		t.Fatalf("download appended file: %v", err)
	}
	assertFileContent(t, output, []byte("abcdefghXYZ"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release append handle: %v", errno)
	}
}

func TestFUSETruncateWritebackAvoidsUploads(t *testing.T) {
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
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "truncate.txt", []byte("abcdefghijklmnop"))
	if _, err := hub.UploadFileContext(ctx, "projectfusetruncatewriteback", "truncate.txt", input); err != nil {
		t.Fatalf("upload truncate file: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	fsys, err := hub.NewFUSE("projectfusetruncatewriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusetruncatewriteback", "truncate.txt")
	if err != nil {
		t.Fatalf("stat truncate file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open truncate handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	var attr fuse.SetAttrIn
	attr.Valid = fuse.FATTR_SIZE
	attr.Size = 5
	var out fuse.AttrOut
	if errno := node.Setattr(ctx, h, &attr, &out); errno != 0 {
		t.Fatalf("setattr truncate: %v", errno)
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync truncate: %v", errno)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 0 {
		t.Fatalf("expected truncate to avoid uploads, got %d", delta)
	}
	output := filepath.Join(t.TempDir(), "truncate-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusetruncatewriteback", "truncate.txt", output); err != nil {
		t.Fatalf("download truncated file: %v", err)
	}
	assertFileContent(t, output, []byte("abcde"))
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release truncate handle: %v", errno)
	}
}

func TestFUSERepeatedEditorStyleSaveCycles(t *testing.T) {
	t.Parallel()
	testFUSEEditorSaveCycle(t, "full-rewrite", func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSERepeatedEditorStyleSaveCyclesWithoutSetattrHandle(t *testing.T) {
	t.Parallel()
	testFUSEEditorSaveCycleWithSetattrHandle(t, "full-rewrite-no-setattr-handle", false, func(original []byte) ([]byte, []byte) {
		first := append(append([]byte(nil), original...), 'X')
		second := append([]byte(nil), original...)
		return first, second
	})
}

func TestFUSEPartialEditorRewriteSaveCycles(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mutator func([]byte) ([]byte, []byte)
	}{
		{
			name: "rewrite-99-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				first := append([]byte(nil), original...)
				first[len(first)-1] = 'Z'
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-80-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				first := append([]byte(nil), original[:len(original)/5]...)
				first = append(first, bytes.Repeat([]byte("Q"), len(original)-len(original)/5)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-50-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				half := len(original) / 2
				first := append([]byte(nil), original[:half]...)
				first = append(first, bytes.Repeat([]byte("R"), len(original)-half)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
		{
			name: "rewrite-40-percent",
			mutator: func(original []byte) ([]byte, []byte) {
				prefix := (len(original) * 3) / 5
				first := append([]byte(nil), original[:prefix]...)
				first = append(first, bytes.Repeat([]byte("S"), len(original)-prefix)...)
				second := append([]byte(nil), original...)
				return first, second
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFUSEEditorSaveCycle(t, tt.name, tt.mutator)
		})
	}
}

func testFUSEEditorSaveCycle(t *testing.T, projectSuffix string, mutator func([]byte) ([]byte, []byte)) {
	testFUSEEditorSaveCycleWithSetattrHandle(t, projectSuffix, true, mutator)
}

func testFUSEEditorSaveCycleWithSetattrHandle(t *testing.T, projectSuffix string, passHandleToSetattr bool, mutator func([]byte) ([]byte, []byte)) {
	t.Helper()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, Config{ChunkSize: 4096, BufferSize: testSingleBufferSize, MaxRetries: 0, DisableGitBackend: true})
	ctx := context.Background()
	original := append(bytes.Repeat([]byte("A"), 4096), bytes.Repeat([]byte("B"), 4096)...)
	original = append(original, bytes.Repeat([]byte("C"), 4096)...)
	original = append(original, []byte("tail")...)
	input := writeTempFile(t, t.TempDir(), "editor.txt", original)
	project := "project-fuse-editor-cycles-" + projectSuffix
	if _, err := hub.UploadFileContext(ctx, project, "editor.txt", input); err != nil {
		t.Fatalf("upload editor file: %v", err)
	}
	fsys, err := hub.NewFUSE(project, fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, project, "editor.txt")
	if err != nil {
		t.Fatalf("stat editor file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	save := func(content []byte) {
		hAny, _, errno := node.Open(ctx, syscall.O_WRONLY)
		if errno != 0 {
			t.Fatalf("open editor handle: %v", errno)
		}
		h := hAny.(*fusefs.TestHandle)
		var attr fuse.SetAttrIn
		attr.Valid = fuse.FATTR_SIZE
		attr.Size = 0
		var out fuse.AttrOut
		var setattrHandle gofusefs.FileHandle
		if passHandleToSetattr {
			setattrHandle = h
		}
		if errno := node.Setattr(ctx, setattrHandle, &attr, &out); errno != 0 {
			t.Fatalf("truncate editor handle: %v", errno)
		}
		for offset := 0; offset < len(content); offset += 4096 {
			end := offset + 4096
			if end > len(content) {
				end = len(content)
			}
			part := content[offset:end]
			if written, errno := h.Write(ctx, part, int64(offset)); errno != 0 || written != uint32(len(part)) {
				t.Fatalf("write editor handle: written=%d errno=%v", written, errno)
			}
		}
		if errno := h.Fsync(ctx, 0); errno != 0 {
			t.Fatalf("fsync editor handle: %v", errno)
		}
		if errno := h.Release(ctx); errno != 0 {
			t.Fatalf("release editor handle: %v", errno)
		}
	}
	first, second := mutator(original)
	save(first)
	save(second)
	output := filepath.Join(t.TempDir(), projectSuffix+"-editor-cycles.txt")
	if err := hub.DownloadFileContext(ctx, project, "editor.txt", output); err != nil {
		t.Fatalf("download saved file: %v", err)
	}
	assertFileContent(t, output, second)
}

func TestFUSEFragmentedWritebackUploadsTouchedChunks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	var uploadCalls atomic.Int32
	var metadataWrites atomic.Int32
	var assetDownloadCalls atomic.Int32
	backend.intercept.Store(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/") {
			uploadCalls.Add(1)
		}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/contents/.storhub/index.json") {
			metadataWrites.Add(1)
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/releases/assets/") {
			assetDownloadCalls.Add(1)
		}
		return false
	})
	hub := backend.newClient(t, Config{
		ChunkSize:         8,
		BufferSize:        testSingleBufferSize,
		MaxRetries:        0,
		DisableGitBackend: true,
	})
	ctx := context.Background()
	input := writeTempFile(t, t.TempDir(), "fragmented.txt", []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"))
	if _, err := hub.UploadFileContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt", input); err != nil {
		t.Fatalf("upload fragmented file: %v", err)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	baselineUploads := uploadCalls.Load()
	baselineMetadataWrites := metadataWrites.Load()
	fsys, err := hub.NewFUSE("projectfusefragmentedwriteback", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	entry, err := hub.StatPathContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt")
	if err != nil {
		t.Fatalf("stat fragmented file: %v", err)
	}
	node := fsys.EnsureNodeForTest(ctx, entry)
	hAny, _, errno := node.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open fragmented handle: %v", errno)
	}
	h := hAny.(*fusefs.TestHandle)
	for i, off := range []int64{0, 8, 16, 24, 32, 40} {
		if written, errno := h.Write(ctx, []byte{byte('0' + i)}, off); errno != 0 || written != 1 {
			t.Fatalf("fragmented write %d: written=%d errno=%v", i, written, errno)
		}
	}
	if errno := h.Fsync(ctx, 0); errno != 0 {
		t.Fatalf("fsync fragmented writes: %v", errno)
	}
	if err := hub.FlushMetadata(context.Background()); err != nil {
		t.Fatalf("flush metadata: %v", err)
	}
	if delta := uploadCalls.Load() - baselineUploads; delta != 6 {
		t.Fatalf("expected six uploaded touched chunks, got %d", delta)
	}
	if delta := metadataWrites.Load() - baselineMetadataWrites; delta < 1 || delta > 6 {
		t.Fatalf("expected 1-6 metadata writes for 6 fragmented dirty ranges, got %d", delta)
	}
	if got := assetDownloadCalls.Load(); got > 8 {
		t.Fatalf("expected bounded base reads during fragmented write commit, got %d", got)
	}
	output := filepath.Join(t.TempDir(), "fragmented-writeback.txt")
	if err := hub.DownloadFileContext(ctx, "projectfusefragmentedwriteback", "fragmented.txt", output); err != nil {
		t.Fatalf("download fragmented file: %v", err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read fragmented output: %v", err)
	}
	for i, off := range []int64{0, 8, 16, 24, 32, 40} {
		if data[off] != byte('0'+i) {
			t.Fatalf("unexpected byte at %d: %q", off, data[off])
		}
	}
	if errno := h.Release(ctx); errno != 0 {
		t.Fatalf("release fragmented handle: %v", errno)
	}
}

func TestFUSEFlagsAndLocks(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	ctx := context.Background()
	if err := hub.MkdirContext(ctx, "projectfuseflags", "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	first := writeTempFile(t, t.TempDir(), "first.txt", []byte("first"))
	second := writeTempFile(t, t.TempDir(), "second.txt", []byte("second"))
	if _, err := hub.UploadFileContext(ctx, "projectfuseflags", "docs/a.txt", first); err != nil {
		t.Fatalf("upload a: %v", err)
	}
	if _, err := hub.UploadFileContext(ctx, "projectfuseflags", "docs/b.txt", second); err != nil {
		t.Fatalf("upload b: %v", err)
	}
	fsys, err := hub.NewFUSE("projectfuseflags", fusefs.DefaultOptions())
	if err != nil {
		t.Fatalf("new fuse fs: %v", err)
	}
	defer func() { _ = fsys.Close() }()
	docsEntry, err := hub.StatPathContext(ctx, "projectfuseflags", "docs")
	if err != nil {
		t.Fatalf("stat docs: %v", err)
	}
	docsNode := fsys.EnsureNodeForTest(ctx, docsEntry)
	if errno := docsNode.Rename(ctx, "a.txt", docsNode, "b.txt", 0x1); errno != syscall.EEXIST {
		t.Fatalf("expected rename noreplace to fail with EEXIST, got %v", errno)
	}
	if errno := docsNode.Rename(ctx, "a.txt", docsNode, "b.txt", 0x2); errno != syscall.EINVAL {
		t.Fatalf("expected rename exchange to fail with EINVAL, got %v", errno)
	}
	aEntry, err := hub.StatPathContext(ctx, "projectfuseflags", "docs/a.txt")
	if err != nil {
		t.Fatalf("stat a: %v", err)
	}
	aNode := fsys.EnsureNodeForTest(ctx, aEntry)
	if errno := aNode.Setxattr(ctx, "user.flag", []byte("one"), 0x1); errno != 0 {
		t.Fatalf("setxattr create: %v", errno)
	}
	if errno := aNode.Setxattr(ctx, "user.flag", []byte("two"), 0x1); errno != syscall.EEXIST {
		t.Fatalf("expected xattr create conflict, got %v", errno)
	}
	if errno := aNode.Setxattr(ctx, "user.other", []byte("two"), 0x2); errno != syscall.ENODATA {
		t.Fatalf("expected xattr replace miss, got %v", errno)
	}
	if errno := aNode.Removexattr(ctx, "user.missing"); errno != syscall.ENODATA {
		t.Fatalf("expected removexattr miss, got %v", errno)
	}
	h1Any, _, errno := aNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle1: %v", errno)
	}
	h2Any, _, errno := aNode.Open(ctx, syscall.O_RDWR)
	if errno != 0 {
		t.Fatalf("open handle2: %v", errno)
	}
	h1 := h1Any.(*fusefs.TestHandle)
	h2 := h2Any.(*fusefs.TestHandle)
	if errno := h1.Setlk(ctx, 1, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_RDLCK}, 0); errno != 0 {
		t.Fatalf("set read lock owner1: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_RDLCK}, 0); errno != 0 {
		t.Fatalf("set read lock owner2: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0); errno != syscall.EAGAIN {
		t.Fatalf("expected write lock conflict, got %v", errno)
	}
	if errno := h1.Release(ctx); errno != 0 {
		t.Fatalf("release handle1: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 4, Typ: syscall.F_WRLCK}, 0); errno != 0 {
		t.Fatalf("upgrade own lock after release: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 0, End: 9, Typ: syscall.F_WRLCK}, 0); errno != 0 {
		t.Fatalf("set broad write lock: %v", errno)
	}
	if errno := h2.Setlk(ctx, 2, &fuse.FileLock{Start: 3, End: 6, Typ: syscall.F_UNLCK}, 0); errno != 0 {
		t.Fatalf("partial unlock: %v", errno)
	}
	var conflict fuse.FileLock
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 4, End: 4, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk unlocked middle: %v", errno)
	}
	if conflict.Typ != syscall.F_UNLCK {
		t.Fatalf("expected middle range to be unlocked, got %+v", conflict)
	}
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 2, End: 2, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk locked prefix: %v", errno)
	}
	if conflict.Typ != syscall.F_WRLCK {
		t.Fatalf("expected prefix range to stay locked, got %+v", conflict)
	}
	if errno := h2.Getlk(ctx, 3, &fuse.FileLock{Start: 8, End: 8, Typ: syscall.F_WRLCK}, 0, &conflict); errno != 0 {
		t.Fatalf("getlk locked suffix: %v", errno)
	}
	if conflict.Typ != syscall.F_WRLCK {
		t.Fatalf("expected suffix range to stay locked, got %+v", conflict)
	}
	if errno := h2.Release(ctx); errno != 0 {
		t.Fatalf("release handle2: %v", errno)
	}
}

func mustBytes(t *testing.T, res fuse.ReadResult, buf []byte) []byte {
	t.Helper()
	got, status := res.Bytes(buf)
	if status != 0 {
		t.Fatalf("read result bytes: %v", status)
	}
	return got
}
