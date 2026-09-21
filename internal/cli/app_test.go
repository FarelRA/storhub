package cli

import (
	"context"
	"os"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

type fakeHub struct {
	t                 *testing.T
	assertReadDirPath string
	assertStatPath    string
	readDirErr        error
	shutdowns         int
	shutdownErr       error
	drainCalls        []string
	drainErr          error
	sess              *cliSessionStore
}

// DrainProjectContext records the call so sync tests can assert draining
// happened (or did not); a configured drainErr simulates a failed commit.
func (h *fakeHub) DrainProjectContext(_ context.Context, project string) error {
	h.drainCalls = append(h.drainCalls, project)
	return h.drainErr
}

// Shutdown records every drain so tests can prove App.Run closed what a
// command opened - the regression guard for the silent-data-loss bug. A
// configured shutdownErr simulates a failed commit point.
func (h *fakeHub) Shutdown(_ context.Context) error {
	h.shutdowns++
	return h.shutdownErr
}

func (h *fakeHub) DeleteProject(_ string) error { return nil }

func (h *fakeHub) UploadFileContext(_ context.Context, _, _, _ string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 11, Inode: 1, Mode: 0o644}, nil
}
func (h *fakeHub) ReplaceFileContext(ctx context.Context, project, remotePath, localPath string, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return h.UploadFileContext(ctx, project, remotePath, localPath)
}
func (h *fakeHub) DownloadFileContext(_ context.Context, _, _, localPath string) error {
	return os.WriteFile(localPath, []byte("downloaded"), 0o644)
}
func (h *fakeHub) ReadDirContext(_ context.Context, _, dir string) ([]storhub.DirEntry, error) {
	if h.readDirErr != nil {
		return nil, h.readDirErr
	}
	if h.assertReadDirPath != "" {
		got, err := shfs.NormalizePath(dir)
		if err != nil {
			h.t.Fatalf("normalize read dir path: %v", err)
		}
		if got != h.assertReadDirPath {
			h.t.Fatalf("unexpected read dir path: %q", dir)
		}
	}
	return []storhub.DirEntry{{Name: "readme.txt", Path: "docs/readme.txt", Size: 11, Mode: 0o644}}, nil
}
func (h *fakeHub) StatPathContext(_ context.Context, _, targetPath string) (*storhub.EntryInfo, error) {
	if h.assertStatPath != "" {
		got, err := shfs.NormalizePath(targetPath)
		if err != nil {
			h.t.Fatalf("normalize stat path: %v", err)
		}
		if got != h.assertStatPath {
			h.t.Fatalf("unexpected stat path: %q", targetPath)
		}
	}
	return &storhub.EntryInfo{Path: targetPath, Size: 11, Mode: 0o644, Inode: 1, UID: 1, GID: 2, NLink: 1, ModifiedAt: 1, AccessedAt: 2, ChangedAt: 3}, nil
}
func (h *fakeHub) ReadFileAtContext(_ context.Context, _, _ string, _, _ int64) ([]byte, error) {
	return []byte("hello world"), nil
}
func (h *fakeHub) MkdirContext(_ context.Context, _, _ string) error { return nil }
func (h *fakeHub) DeleteFileContext(_ context.Context, _, _ string, _ ...storhub.MutateOption) error {
	return nil
}
func (h *fakeHub) RmdirContext(_ context.Context, _, _ string, _ ...storhub.MutateOption) error {
	return nil
}
func (h *fakeHub) RenameContext(_ context.Context, _, _, _ string, _ ...storhub.MutateOption) error {
	return nil
}
func (h *fakeHub) AppendFileContext(_ context.Context, _, _ string, data []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: int64(len(data)), Inode: 2, Mode: 0o644}, nil
}
func (h *fakeHub) WriteFileAtContext(_ context.Context, _, _ string, offset int64, data []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: offset + int64(len(data)), Inode: 3, Mode: 0o644}, nil
}
func (h *fakeHub) PatchFileContext(_ context.Context, _, _ string, _, _ int64, _ []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 9, Inode: 4, Mode: 0o644}, nil
}
func (h *fakeHub) CreateFileContext(_ context.Context, _, _ string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 0, Inode: 5, Mode: 0o644}, nil
}
func (h *fakeHub) TruncateFileContext(_ context.Context, _, _ string, size int64, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: size, Inode: 6, Mode: 0o644}, nil
}
func (h *fakeHub) ChmodContext(_ context.Context, _, _ string, _ uint32) error {
	return nil
}
func (h *fakeHub) ChownContext(_ context.Context, _, _ string, _, _ uint32) error {
	return nil
}
func (h *fakeHub) ChtimesContext(_ context.Context, _, _ string, _, _ int64) error {
	return nil
}
func (h *fakeHub) SymlinkContext(_ context.Context, _, target, _ string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: int64(len(target)), Inode: 7, Mode: 0o777}, nil
}
func (h *fakeHub) ReadlinkContext(_ context.Context, _, _ string) (string, error) {
	return "target", nil
}
func (h *fakeHub) LinkContext(_ context.Context, _, _, _ string) (*storhub.FileMetadata, error) {
	return &storhub.FileMetadata{Size: 11, Inode: 1, Mode: 0o644}, nil
}
func (h *fakeHub) ListMetadataRevisionsContext(_ context.Context, _ string) ([]storhub.MetadataRevision, error) {
	return []storhub.MetadataRevision{{CommitSHA: "deadbeefcafebabe", Message: "demo", CommittedAt: 1}}, nil
}
func (h *fakeHub) RollbackMetadataContext(_ context.Context, _, _ string) error {
	return nil
}
func (h *fakeHub) PruneContext(_ context.Context, _, scope string, _ int, dryRun bool) (*storhub.PruneResult, error) {
	return &storhub.PruneResult{Scope: storhub.PruneScope(scope), DryRun: dryRun}, nil
}
func (h *fakeHub) DegradedProjects() []string {
	return nil
}
func (h *fakeHub) ReEnableProject(_ string) error { return nil }
func (h *fakeHub) PressureSnapshot() storhub.PressureSnapshot {
	return storhub.PressureSnapshot{}
}
func (h *fakeHub) PressureFailureStreak(_ string) uint64 { return 0 }
func (h *fakeHub) PressurePendingDepth(_ string) int     { return 0 }
func (h *fakeHub) NewFUSE(_ string, _ storhub.FUSEOptions) (fuseMount, error) {
	return fakeMount{}, nil
}

type fakeMount struct{}

func (fakeMount) Mount(string) error { return nil }
func (fakeMount) Unmount() error     { return nil }
func (fakeMount) Wait()              {}
func (fakeMount) Close() error       { return nil }

func newTestApp(t *testing.T) (*App, func() string, func() string) {
	t.Helper()
	app := New()
	stdoutFile, stdout := tempCaptureFile(t)
	stderrFile, stderr := tempCaptureFile(t)
	app.stdout = stdoutFile
	app.stderr = stderrFile
	// Route App warnings through the same capture so tests can assert on
	// them instead of polluting real stderr. Per-App field, no globals.
	app.warnOut = stderrFile
	return app, stdout, stderr
}

func tempCaptureFile(t *testing.T) (*os.File, func() string) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "storhub-cli-*.txt")
	if err != nil {
		t.Fatalf("create temp capture file: %v", err)
	}
	return file, func() string {
		if err := file.Sync(); err != nil {
			t.Fatalf("sync capture file: %v", err)
		}
		data, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatalf("read capture file: %v", err)
		}
		return string(data)
	}
}
