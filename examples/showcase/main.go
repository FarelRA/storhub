package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	storfuse "github.com/FarelRA/storhub/fuse"
	"github.com/FarelRA/storhub/storhub"
)

type showcaseHub interface {
	Owner() string
	Mkdir(project, dirPath string) error
	CreateFile(project, filePath string) (*storhub.FileMetadata, error)
	WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error)
	AppendFile(project, filePath string, data []byte) (*storhub.FileMetadata, error)
	ReadFileAt(project, filePath string, offset, length int64) ([]byte, error)
	Rename(project, oldPath, newPath string) error
	TruncateFile(project, filePath string, size int64) (*storhub.FileMetadata, error)
	UploadFile(project, remotePath, localPath string) (*storhub.FileMetadata, error)
	ReplaceFile(project, remotePath, localPath string) (*storhub.FileMetadata, error)
	PatchFile(project, filePath string, offset, deleteSize int64, edit []byte) (*storhub.FileMetadata, error)
	DownloadFile(project, remotePath, localPath string) error
	ListFiles(project string) ([]storhub.FileMetadata, error)
	ListReleases(project string) ([]storhub.ReleaseMetadata, error)
	ListMetadataRevisions(project string) ([]storhub.MetadataRevision, error)
	RollbackMetadata(project, commitSHA string) error
	StatPath(project, targetPath string) (*storhub.EntryInfo, error)
	ReadDir(project, dirPath string) ([]storhub.DirEntry, error)
	StatFS(project string) (*storhub.FSStats, error)
	Chmod(project, targetPath string, mode uint32) error
	Chown(project, targetPath string, uid, gid uint32) error
	Chtimes(project, targetPath string, atime, mtime int64) error
	SetXAttr(project, targetPath, attr string, data []byte) error
	GetXAttr(project, targetPath, attr string) ([]byte, error)
	ListXAttr(project, targetPath string) ([]string, error)
	RemoveXAttr(project, targetPath, attr string) error
	Symlink(project, target, linkPath string) (*storhub.FileMetadata, error)
	Readlink(project, linkPath string) (string, error)
	Link(project, existingPath, newPath string) (*storhub.FileMetadata, error)
	DeleteFile(project, filePath string) error
	Rmdir(project, dirPath string) error
	PurgeUntracked(project string) (*storhub.PurgeResult, error)
	CleanupProject(project string) error
	DeleteRelease(project, tag string) error
	DeleteProject(project string) error
}

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}

	printTitle("StorHub Showcase")
	printKV("default chunk size", "%d", storhub.DefaultChunkSize)
	printKV("default buffer size", "%d", storhub.DefaultBufferSize)
	printKV("max release asset", "%d", storhub.MaxReleaseAssetSize)
	printKV("node kinds", "file=%s symlink=%s", storhub.NodeKindFile, storhub.NodeKindSymlink)
	fmt.Println()

	defaultHub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatalf("initialize default StorHub: %v", err)
	}
	_ = defaultHub

	cfg := storhub.DefaultConfig()
	cfg.ChunkSize = 8 << 20

	configuredHub, err := storhub.NewStorHubWithConfig(token, cfg)
	if err != nil {
		log.Fatalf("initialize configured StorHub: %v", err)
	}
	_ = configuredHub

	hub, err := storhub.NewStorHubWithContext(context.Background(), token, cfg)
	if err != nil {
		log.Fatalf("initialize contextual StorHub: %v", err)
	}

	workspace, err := os.MkdirTemp("", "storhub-showcase-")
	if err != nil {
		log.Fatalf("create workspace: %v", err)
	}
	defer os.RemoveAll(workspace)

	project := fmt.Sprintf("storhub-showcase-%d", os.Getpid())
	if err := runShowcase(hub, workspace, project); err != nil {
		log.Fatal(err)
	}
	if err := previewFUSE(hub, project); err != nil {
		log.Fatal(err)
	}
	if err := runMaintenance(hub, project); err != nil {
		log.Fatal(err)
	}

	fmt.Println("showcase completed")
}

func runShowcase(hub showcaseHub, workspace, project string) error {
	printSection("Session")
	printKV("owner", "%s", hub.Owner())
	printKV("project", "%s", project)
	printKV("workspace", "%s", workspace)
	fmt.Println()

	if err := runStep("prepare directories", func() error {
		for _, dir := range []string{"docs", "docs/specs", "artifacts", "scratch"} {
			if err := hub.Mkdir(project, dir); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	guideV1 := filepath.Join(workspace, "guide-v1.txt")
	guideV2 := filepath.Join(workspace, "guide-v2.txt")
	seedV1 := strings.Join([]string{
		"StorHub guide",
		"- immutable chunked storage",
		"- metadata catalog in .storhub/metadata.json",
		"- optional FUSE mounting",
	}, "\n") + "\n"
	seedV2 := strings.Replace(seedV1, "optional FUSE mounting", "adaptive FUSE writeback", 1)
	if err := runStep("create local source files", func() error {
		if err := os.WriteFile(guideV1, []byte(seedV1), 0o644); err != nil {
			return err
		}
		return os.WriteFile(guideV2, []byte(seedV2), 0o644)
	}); err != nil {
		return err
	}

	if err := runStep("create and edit managed file", func() error {
		if _, err := hub.CreateFile(project, "scratch/notes.txt"); err != nil {
			return err
		}
		if _, err := hub.WriteFileAt(project, "scratch/notes.txt", 0, []byte("alpha\n")); err != nil {
			return err
		}
		if _, err := hub.AppendFile(project, "scratch/notes.txt", []byte("beta\n")); err != nil {
			return err
		}
		preview, err := hub.ReadFileAt(project, "scratch/notes.txt", 0, 64)
		if err != nil {
			return err
		}
		printKV("notes preview", "%q", string(preview))
		if _, err := hub.TruncateFile(project, "scratch/notes.txt", 5); err != nil {
			return err
		}
		return hub.Rename(project, "scratch/notes.txt", "scratch/notes-short.txt")
	}); err != nil {
		return err
	}

	guidePath := "docs/specs/guide.txt"
	uploaded, err := stepFile("upload file", func() (*storhub.FileMetadata, error) {
		return hub.UploadFile(project, guidePath, guideV1)
	})
	if err != nil {
		return err
	}
	printFile("uploaded file", uploaded)

	replaced, err := stepFile("replace file", func() (*storhub.FileMetadata, error) {
		return hub.ReplaceFile(project, guidePath, guideV2)
	})
	if err != nil {
		return err
	}
	printFile("replaced file", replaced)

	patched, err := stepFile("patch file", func() (*storhub.FileMetadata, error) {
		return hub.PatchFile(project, guidePath, 0, int64(len("StorHub")), []byte("StorHub showcase"))
	})
	if err != nil {
		return err
	}
	printFile("patched file", patched)

	printKV("chunk count", "%d", len(patched.Chunks))
	fmt.Println()

	if err := runStep("apply POSIX metadata", func() error {
		now := int64(1_700_000_000)
		if err := hub.Chmod(project, guidePath, 0o640); err != nil {
			return err
		}
		if err := hub.Chown(project, guidePath, 1000, 1000); err != nil {
			return err
		}
		if err := hub.Chtimes(project, guidePath, now, now+7200); err != nil { // +2 hours
			return err
		}
		if err := hub.SetXAttr(project, guidePath, "user.demo", []byte("enabled")); err != nil {
			return err
		}
		value, err := hub.GetXAttr(project, guidePath, "user.demo")
		if err != nil {
			return err
		}
		printKV("xattr user.demo", "%q", string(value))
		attrs, err := hub.ListXAttr(project, guidePath)
		if err != nil {
			return err
		}
		printKV("xattrs", "%v", attrs)
		if err := hub.RemoveXAttr(project, guidePath, "user.demo"); err != nil {
			return err
		}
		return hub.SetXAttr(project, guidePath, "user.demo", []byte("enabled"))
	}); err != nil {
		return err
	}

	if err := runStep("create links", func() error {
		if _, err := hub.Symlink(project, "docs/specs/guide.txt", "docs/specs/guide.link"); err != nil {
			return err
		}
		target, err := hub.Readlink(project, "docs/specs/guide.link")
		if err != nil {
			return err
		}
		printKV("symlink target", "%s", target)
		_, err = hub.Link(project, "docs/specs/guide.txt", "artifacts/guide-copy.txt")
		return err
	}); err != nil {
		return err
	}

	if err := runStep("write sparse tail through hardlink", func() error {
		_, err := hub.WriteFileAt(project, "artifacts/guide-copy.txt", patched.Size+16, []byte("tail marker"))
		return err
	}); err != nil {
		return err
	}

	downloadPath := filepath.Join(workspace, "downloaded-guide.txt")
	if err := runStep("download final file", func() error {
		return hub.DownloadFile(project, guidePath, downloadPath)
	}); err != nil {
		return err
	}
	downloaded, err := os.ReadFile(downloadPath)
	if err != nil {
		return fmt.Errorf("read downloaded file: %w", err)
	}
	printSection("Downloaded Content")
	fmt.Println(string(downloaded))

	files, err := hub.ListFiles(project)
	if err != nil {
		return fmt.Errorf("list files: %w", err)
	}
	printFiles(files)

	releases, err := hub.ListReleases(project)
	if err != nil {
		return fmt.Errorf("list releases: %w", err)
	}
	printReleases(releases)

	entries, err := hub.ReadDir(project, "docs/specs")
	if err != nil {
		return fmt.Errorf("read directory: %w", err)
	}
	printDir("docs/specs", entries)

	stat, err := hub.StatPath(project, guidePath)
	if err != nil {
		return fmt.Errorf("stat path: %w", err)
	}
	printStat("guide.txt stat", stat)

	stats, err := hub.StatFS(project)
	if err != nil {
		return fmt.Errorf("statfs: %w", err)
	}
	printSection("Filesystem Stats")
	printKV("files", "%d", stats.Files)
	printKV("directories", "%d", stats.Directories)
	printKV("inodes", "%d", stats.Inodes)
	printKV("bytes", "%d", stats.Bytes)
	printKV("releases", "%d", stats.Releases)
	printKV("assets", "%d", stats.Assets)
	fmt.Println()

	rollbackMarker, err := stepFile("create rollback marker", func() (*storhub.FileMetadata, error) {
		return hub.CreateFile(project, "scratch/rollback-marker.txt")
	})
	if err != nil {
		return err
	}
	if rollbackMarker == nil {
		return errors.New("rollback marker metadata missing")
	}
	if _, err := hub.WriteFileAt(project, "scratch/rollback-marker.txt", 0, []byte("rollback me")); err != nil {
		return fmt.Errorf("write rollback marker: %w", err)
	}

	revisions, err := hub.ListMetadataRevisions(project)
	if err != nil {
		return fmt.Errorf("list revisions: %w", err)
	}
	printRevisions(revisions)
	if len(revisions) > 1 {
		if err := runStep("rollback metadata one revision", func() error {
			return hub.RollbackMetadata(project, revisions[1].CommitSHA)
		}); err != nil {
			return err
		}
	}

	if _, err := hub.StatPath(project, "scratch/rollback-marker.txt"); err != nil {
		printKV("rollback removed marker as expected", "%v", err)
	}

	if err := runStep("delete scratch file and directory", func() error {
		if err := hub.DeleteFile(project, "scratch/notes-short.txt"); err != nil {
			return err
		}
		return hub.Rmdir(project, "scratch")
	}); err != nil {
		return err
	}

	return nil
}

func previewFUSE(hub *storhub.StorHub, project string) error {
	storhubOpts := storhub.DefaultFUSEOptions()
	fuseOpts := storfuse.DefaultOptions()
	if storhubOpts.PageSize != fuseOpts.PageSize {
		return fmt.Errorf("fuse default mismatch: storhub=%d fuse=%d", storhubOpts.PageSize, fuseOpts.PageSize)
	}
	fsys, err := storfuse.New(hub, project, fuseOpts)
	if err != nil {
		return fmt.Errorf("create FUSE filesystem: %w", err)
	}
	defer fsys.Close()
	printSection("FUSE Preview")
	printKV("page size", "%d", fsys.Options().PageSize)
	printKV("cache dir", "%s", fsys.Options().CacheDir)
	fmt.Println()
	return nil
}

func runMaintenance(hub showcaseHub, project string) error {
	printSection("Maintenance")
	purge, err := hub.PurgeUntracked(project)
	if err != nil {
		return fmt.Errorf("purge untracked: %w", err)
	}
	printKV("purged assets", "%d", purge.DeletedAssets)
	printKV("purged releases", "%d", purge.DeletedReleases)
	if err := hub.CleanupProject(project); err != nil {
		return fmt.Errorf("cleanup project: %w", err)
	}
	printKV("cleanup", "%s", "done")
	if tag := strings.TrimSpace(os.Getenv("STORHUB_DELETE_RELEASE_TAG")); tag != "" {
		if err := hub.DeleteRelease(project, tag); err != nil {
			return fmt.Errorf("delete release %s: %w", tag, err)
		}
		printKV("deleted release tag", "%s", tag)
	}
	if os.Getenv("STORHUB_DELETE_PROJECT") == "1" {
		if err := hub.DeleteProject(project); err != nil {
			return fmt.Errorf("delete project: %w", err)
		}
		printKV("project deletion", "%s", "deleted at end of showcase")
	} else {
		printKV("project deletion", "%s", "skipped; set STORHUB_DELETE_PROJECT=1 to remove it automatically")
	}
	fmt.Println()
	return nil
}

func runStep(label string, fn func() error) error {
	if err := fn(); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	fmt.Printf("ok: %s\n", label)
	return nil
}

func stepFile(label string, fn func() (*storhub.FileMetadata, error)) (*storhub.FileMetadata, error) {
	meta, err := fn()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}
	fmt.Printf("ok: %s\n", label)
	return meta, nil
}

func runOrDie(label string, fn func() error) {
	if err := fn(); err != nil {
		log.Fatalf("%s: %v", label, err)
	}
	fmt.Printf("ok: %s\n", label)
}

func printFile(label string, meta *storhub.FileMetadata) {
	if meta == nil {
		fmt.Printf("%s: <nil>\n\n", label)
		return
	}
	fmt.Printf("%s:\n", label)
	fmt.Printf("- size: %d\n", meta.Size)
	fmt.Printf("- inode: %d\n", meta.Inode)
	fmt.Printf("- mode: %#o\n", meta.Mode)
	fmt.Printf("- chunks: %d\n", len(meta.Chunks))
	if meta.Symlink != "" {
		fmt.Printf("- kind: symlink\n\n")
	} else {
		fmt.Printf("- kind: file\n\n")
	}
}

func printFiles(files []storhub.FileMetadata) {
	printSection("Tracked Files")
	for _, file := range files {
		fmt.Printf("- size=%d inode=%d chunks=%d\n", file.Size, file.Inode, len(file.Chunks))
	}
	fmt.Println()
}

func printReleases(releases []storhub.ReleaseMetadata) {
	printSection("Releases")
	for _, release := range releases {
		fmt.Printf("- assets=%d\n", release.AssetCount)
	}
	fmt.Println()
}

func printDir(label string, entries []storhub.DirEntry) {
	printSection("Directory Listing")
	printKV("path", "%s", label)
	for _, entry := range entries {
		kind := "file"
		if entry.IsDir {
			kind = "dir"
		} else if entry.IsSymlink {
			kind = "symlink"
		}
		fmt.Printf("- %s (%s, inode=%d, size=%d, mode=%#o)\n", entry.Path, kind, entry.Inode, entry.Size, entry.Mode)
	}
	fmt.Println()
}

func printStat(label string, entry *storhub.EntryInfo) {
	if entry == nil {
		fmt.Printf("%s: <nil>\n\n", label)
		return
	}
	printSection(label)
	fmt.Printf("- path: %s\n", entry.Path)
	fmt.Printf("- inode: %d\n", entry.Inode)
	fmt.Printf("- size: %d\n", entry.Size)
	fmt.Printf("- mode: %#o\n", entry.Mode)
	fmt.Printf("- uid/gid: %d/%d\n", entry.UID, entry.GID)
	fmt.Printf("- nlink: %d\n", entry.NLink)
	fmt.Printf("- kind: dir=%v symlink=%v logical=%s\n\n", entry.IsDir, entry.IsSymlink, entry.Kind)
}

func printRevisions(revisions []storhub.MetadataRevision) {
	printSection("Recent Metadata Revisions")
	for i, rev := range revisions {
		if i >= 5 {
			break
		}
		fmt.Printf("- %s  %s  %s\n", shortSHA(rev.CommitSHA), time.Unix(rev.CommittedAt, 0).Format("2006-01-02 15:04:05"), rev.Message)
	}
	fmt.Println()
}

func shortSHA(sha string) string {
	if len(sha) <= 8 {
		return sha
	}
	return sha[:8]
}

func printTitle(title string) {
	line := strings.Repeat("=", len(title)+8)
	fmt.Println(line)
	fmt.Printf("=== %s ===\n", title)
	fmt.Println(line)
}

func printSection(title string) {
	fmt.Println(title)
	fmt.Println(strings.Repeat("-", len(title)))
}

func printKV(label, format string, args ...any) {
	fmt.Printf("- %s: ", label)
	fmt.Printf(format, args...)
	fmt.Println()
}
