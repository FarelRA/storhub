package storage

// FUSE editor-style save cycles.
import (
	"bytes"
	"context"
	"path/filepath"
	"syscall"
	"testing"

	fusefs "github.com/FarelRA/storhub/internal/fusefs"
	gofusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

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
