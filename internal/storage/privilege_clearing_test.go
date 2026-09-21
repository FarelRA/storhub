package storage

import (
	"bytes"
	"context"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	fusefs "github.com/FarelRA/storhub/internal/fusefs"
)

func privAdminCtx() context.Context {
	return shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})
}

func privUserCtx() context.Context {
	return shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1001, GID: 1002, Groups: []uint32{2000}})
}

func setupPrivProject(t *testing.T, project, dir, file string, content []byte, mode uint32) (*StorHub, context.Context, context.Context, string) {
	t.Helper()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	adminCtx := privAdminCtx()
	userCtx := privUserCtx()
	if err := hub.MkdirContext(adminCtx, project, dir); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	if err := hub.ChmodContext(adminCtx, project, dir, 0o777); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "seed.txt", content)
	if _, err := hub.UploadFileContext(adminCtx, project, dir+"/"+file, input); err != nil {
		t.Fatalf("upload seed: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, dir+"/"+file, mode); err != nil {
		t.Fatalf("chmod seed to %o: %v", mode, err)
	}
	info, err := hub.StatPathContext(adminCtx, project, dir+"/"+file)
	if err != nil {
		t.Fatalf("stat seed: %v", err)
	}
	if info.Mode != mode {
		t.Fatalf("seed mode = %o, want %o", info.Mode, mode)
	}
	return hub, adminCtx, userCtx, dir + "/" + file
}

func statMode(ctx context.Context, t *testing.T, hub *StorHub, project, path string) uint32 {
	t.Helper()
	info, err := hub.StatPathContext(ctx, project, path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode
}

func TestUnprivilegedPutClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectputclear", "docs", "f.txt", []byte("hello world"), 0o6777)
	input := writeTempFile(t, t.TempDir(), "new.txt", []byte("replaced!!"))
	if _, err := hub.ReplaceFileContext(userCtx, "projectputclear", target, input); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectputclear", target); got != 0o777 {
		t.Fatalf("put mode = %o, want %o", got, uint32(0o777))
	}
}

func TestAdminPutKeepsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, _, target := setupPrivProject(t, "projectputkeep", "docs", "f.txt", []byte("hello world"), 0o6777)
	input := writeTempFile(t, t.TempDir(), "new.txt", []byte("replaced!!"))
	if _, err := hub.ReplaceFileContext(adminCtx, "projectputkeep", target, input); err != nil {
		t.Fatalf("admin replace: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectputkeep", target); got != 0o6777 {
		t.Fatalf("admin put mode = %o, want %o", got, uint32(0o6777))
	}
}

func TestUnprivilegedPatchClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectpatchclear", "docs", "f.txt", []byte("hello world"), 0o6777)
	if _, err := hub.PatchFileContext(userCtx, "projectpatchclear", target, 0, 1, []byte("X")); err != nil {
		t.Fatalf("patch: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectpatchclear", target); got != 0o777 {
		t.Fatalf("patch mode = %o, want %o", got, uint32(0o777))
	}
}

func TestAdminPatchKeepsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, _, target := setupPrivProject(t, "projectpatchkeep", "docs", "f.txt", []byte("hello world"), 0o6777)
	if _, err := hub.PatchFileContext(adminCtx, "projectpatchkeep", target, 0, 1, []byte("X")); err != nil {
		t.Fatalf("admin patch: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectpatchkeep", target); got != 0o6777 {
		t.Fatalf("admin patch mode = %o, want %o", got, uint32(0o6777))
	}
}

func TestUnprivilegedPatchRangesClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectrangesclear", "docs", "f.txt", []byte("hello world"), 0o6777)
	edits := []shfs.RangeEdit{{Start: 0, DeleteSize: 1, Data: []byte("X")}}
	if _, err := hub.PatchFileRangesContext(userCtx, "projectrangesclear", target, edits); err != nil {
		t.Fatalf("patch ranges: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectrangesclear", target); got != 0o777 {
		t.Fatalf("patch ranges mode = %o, want %o", got, uint32(0o777))
	}
}

func TestUnprivilegedReplaceReaderClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectreplacereader", "docs", "f.txt", []byte("hello world"), 0o6777)
	payload := []byte("new content here")
	if _, err := hub.ReplaceFileFromReaderContext(userCtx, "projectreplacereader", target, bytes.NewReader(payload), shfs.WithSize(int64(len(payload)))); err != nil {
		t.Fatalf("replace from reader: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectreplacereader", target); got != 0o777 {
		t.Fatalf("replace reader mode = %o, want %o", got, uint32(0o777))
	}
}

func TestUnprivilegedRewriteRangesClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectrewriteclear", "docs", "f.txt", []byte("hello world"), 0o6777)
	snapshot := writeTempFile(t, t.TempDir(), "snap.txt", []byte("HELLO world"))
	repo, _, err := hub.LoadRepoMetadataContext(userCtx, "projectrewriteclear")
	if err != nil {
		t.Fatalf("load meta: %v", err)
	}
	file := repo.FindFile(target)
	if file == nil {
		t.Fatalf("seed file missing")
	}
	ranges := []fusefs.ByteRange{{Start: 0, End: 5}}
	if _, err := hub.RewriteFileRangesWithMetadataContext(userCtx, "projectrewriteclear", target, snapshot, repo, file, int64(len("HELLO world")), ranges); err != nil {
		t.Fatalf("rewrite ranges: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectrewriteclear", target); got != 0o777 {
		t.Fatalf("rewrite mode = %o, want %o", got, uint32(0o777))
	}
}

func TestUnprivilegedTruncateShrinkClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projecttruncshrink", "docs", "f.txt", []byte("hello world"), 0o6666)
	if _, err := hub.TruncateFileContext(userCtx, "projecttruncshrink", target, 5); err != nil {
		t.Fatalf("truncate shrink: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projecttruncshrink", target); got != 0o666 {
		t.Fatalf("truncate shrink mode = %o, want %o", got, uint32(0o666))
	}
}

func TestUnprivilegedTruncateNoopClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projecttruncnoop", "docs", "f.txt", []byte("hello world"), 0o6666)
	info, err := hub.StatPathContext(adminCtx, "projecttruncnoop", target)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if _, err := hub.TruncateFileContext(userCtx, "projecttruncnoop", target, info.Size); err != nil {
		t.Fatalf("truncate noop: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projecttruncnoop", target); got != 0o666 {
		t.Fatalf("truncate noop mode = %o, want %o", got, uint32(0o666))
	}
}

func TestAdminTruncateNoopKeepsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, _, target := setupPrivProject(t, "projecttruncnoopkeep", "docs", "f.txt", []byte("hello world"), 0o6666)
	info, err := hub.StatPathContext(adminCtx, "projecttruncnoopkeep", target)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if _, err := hub.TruncateFileContext(adminCtx, "projecttruncnoopkeep", target, info.Size); err != nil {
		t.Fatalf("admin truncate noop: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projecttruncnoopkeep", target); got != 0o6666 {
		t.Fatalf("admin truncate noop mode = %o, want %o", got, uint32(0o6666))
	}
}

func TestUnprivilegedWriteAtClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectwriteatclear", "docs", "f.txt", []byte("hello world"), 0o6666)
	if _, err := hub.WriteFileAtContext(userCtx, "projectwriteatclear", target, 0, []byte("XX")); err != nil {
		t.Fatalf("write at: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectwriteatclear", target); got != 0o666 {
		t.Fatalf("writeat mode = %o, want %o", got, uint32(0o666))
	}
}

func TestUnprivilegedAppendClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projectappendclear", "docs", "f.txt", []byte("hello world"), 0o6666)
	if _, err := hub.AppendFileContext(userCtx, "projectappendclear", target, []byte("tail")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectappendclear", target); got != 0o666 {
		t.Fatalf("append mode = %o, want %o", got, uint32(0o666))
	}
}

func TestUnprivilegedChownClearsFilePrivilegeBits(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	adminCtx := privAdminCtx()
	userCtx := privUserCtx()
	project := "projectchownfile"
	if err := hub.MkdirContext(adminCtx, project, "docs"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs", 0o777); err != nil {
		t.Fatalf("chmod dir: %v", err)
	}
	input := writeTempFile(t, t.TempDir(), "owned.txt", []byte("owned"))
	if _, err := hub.UploadFileContext(userCtx, project, "docs/owned.txt", input); err != nil {
		t.Fatalf("user upload: %v", err)
	}
	if err := hub.ChmodContext(userCtx, project, "docs/owned.txt", 0o6777); err != nil {
		t.Fatalf("user chmod setuid: %v", err)
	}
	const keepOwner = ^uint32(0)
	if err := hub.ChownContext(userCtx, project, "docs/owned.txt", keepOwner, 2000); err != nil {
		t.Fatalf("user chown: %v", err)
	}
	if got := statMode(adminCtx, t, hub, project, "docs/owned.txt"); got != 0o777 {
		t.Fatalf("chown file mode = %o, want %o", got, uint32(0o777))
	}
}

func TestUnprivilegedChownClearsDirPrivilegeBits(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	adminCtx := privAdminCtx()
	userCtx := privUserCtx()
	project := "projectchowndir"
	if err := hub.MkdirContext(adminCtx, project, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs", 0o777); err != nil {
		t.Fatalf("chmod docs: %v", err)
	}
	if err := hub.MkdirContext(userCtx, project, "docs/sub"); err != nil {
		t.Fatalf("user mkdir sub: %v", err)
	}
	if err := hub.ChmodContext(userCtx, project, "docs/sub", 0o6755); err != nil {
		t.Fatalf("user chmod sub: %v", err)
	}
	const keepOwner = ^uint32(0)
	if err := hub.ChownContext(userCtx, project, "docs/sub", keepOwner, 2000); err != nil {
		t.Fatalf("user chown dir: %v", err)
	}
	if got := statMode(adminCtx, t, hub, project, "docs/sub"); got != 0o755 {
		t.Fatalf("chown dir mode = %o, want %o", got, uint32(0o755))
	}
}

func TestAdminChownKeepsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, _, target := setupPrivProject(t, "projectchownkeep", "docs", "f.txt", []byte("hello world"), 0o6777)
	if err := hub.ChownContext(adminCtx, "projectchownkeep", target, 1001, 1002); err != nil {
		t.Fatalf("admin chown file: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projectchownkeep", target); got != 0o6777 {
		t.Fatalf("admin chown file mode = %o, want %o", got, uint32(0o6777))
	}
	if err := hub.ChownContext(adminCtx, "projectchownkeep", "docs", 1001, 1002); err != nil {
		t.Fatalf("admin chown dir: %v", err)
	}
	dirMode := statMode(adminCtx, t, hub, "projectchownkeep", "docs")
	_ = dirMode
}

func TestAdminChownKeepsDirPrivilegeBits(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	adminCtx := privAdminCtx()
	project := "projectchowndirkeep"
	if err := hub.MkdirContext(adminCtx, project, "docs"); err != nil {
		t.Fatalf("mkdir docs: %v", err)
	}
	if err := hub.ChmodContext(adminCtx, project, "docs", 0o6775); err != nil {
		t.Fatalf("chmod docs setuid: %v", err)
	}
	if err := hub.ChownContext(adminCtx, project, "docs", 1001, 1002); err != nil {
		t.Fatalf("admin chown dir: %v", err)
	}
	if got := statMode(adminCtx, t, hub, project, "docs"); got != 0o6775 {
		t.Fatalf("admin chown dir mode = %o, want %o", got, uint32(0o6775))
	}
}

func TestTruncateGrowClearsPrivilegeBits(t *testing.T) {
	t.Parallel()
	hub, adminCtx, userCtx, target := setupPrivProject(t, "projecttruncgrow", "docs", "f.txt", []byte("hi"), 0o6666)
	if _, err := hub.TruncateFileContext(userCtx, "projecttruncgrow", target, 10); err != nil {
		t.Fatalf("truncate grow: %v", err)
	}
	if got := statMode(adminCtx, t, hub, "projecttruncgrow", target); got != 0o666 {
		t.Fatalf("truncate grow mode = %o, want %o", got, uint32(0o666))
	}
}
