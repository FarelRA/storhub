package cli

import (
	"context"
	"strings"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/storhub"
)

// cpFakeHub is an in-memory hub behind the cp seam with call counters: the
// reflink-mode tests prove which path executed by counting clone versus
// streaming calls, not by reading log lines.
type cpFakeHub struct {
	hubClient
	files                  map[string][]byte
	cloneErr               error
	cloneCalls             int
	readCalls              int
	writeCalls             int
	createCalls            int
	lastSrcOff, lastDstOff int64
	lastLength             int64
}

func (h *cpFakeHub) StatPath(project, targetPath string) (*storhub.EntryInfo, error) {
	if data, ok := h.files[targetPath]; ok {
		return &storhub.EntryInfo{Path: targetPath, Size: int64(len(data)), Mode: 0o644, Inode: 1, NLink: 1}, nil
	}
	return nil, shfs.NotFound(targetPath)
}

func (h *cpFakeHub) CreateFile(project, filePath string) (*storhub.FileMetadata, error) {
	h.createCalls++
	if _, ok := h.files[filePath]; ok {
		return nil, shfs.AlreadyExists(filePath)
	}
	h.files[filePath] = []byte{}
	return &storhub.FileMetadata{Size: 0, Inode: 1, Mode: 0o644}, nil
}

func (h *cpFakeHub) ReadFileAt(project, filePath string, offset, length int64) ([]byte, error) {
	h.readCalls++
	data, ok := h.files[filePath]
	if !ok {
		return nil, shfs.NotFound(filePath)
	}
	if offset > int64(len(data)) {
		return []byte{}, nil
	}
	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return append([]byte(nil), data[offset:end]...), nil
}

func (h *cpFakeHub) WriteFileAt(project, filePath string, offset int64, data []byte) (*storhub.FileMetadata, error) {
	h.writeCalls++
	content, ok := h.files[filePath]
	if !ok {
		return nil, shfs.NotFound(filePath)
	}
	content = append([]byte(nil), content...)
	if offset > int64(len(content)) {
		content = append(content, make([]byte, offset-int64(len(content)))...)
	}
	if need := offset + int64(len(data)); int64(len(content)) < need {
		content = append(content, make([]byte, need-int64(len(content)))...)
	}
	copy(content[offset:], data)
	h.files[filePath] = content
	return &storhub.FileMetadata{Size: int64(len(content)), Inode: 1, Mode: 0o644}, nil
}

func (h *cpFakeHub) CloneRange(ctx context.Context, project, src string, srcOff int64, dst string, dstOff int64, length int64, opts ...storhub.MutateOption) (*storhub.FileMetadata, error) {
	h.cloneCalls++
	h.lastSrcOff, h.lastDstOff, h.lastLength = srcOff, dstOff, length
	if h.cloneErr != nil {
		return nil, h.cloneErr
	}
	snap, ok := h.files[src]
	if !ok {
		return nil, shfs.NotFound(src)
	}
	seg := append([]byte(nil), snap[srcOff:srcOff+length]...)
	d := append([]byte(nil), h.files[dst]...)
	if need := dstOff + length; int64(len(d)) < need {
		d = append(d, make([]byte, need-int64(len(d)))...)
	}
	copy(d[dstOff:], seg)
	h.files[dst] = d
	return &storhub.FileMetadata{Size: int64(len(d)), Inode: 1, Mode: 0o644}, nil
}

func (h *cpFakeHub) DrainProjectContext(ctx context.Context, project string) error { return nil }
func (h *cpFakeHub) Shutdown(ctx context.Context) error                            { return nil }

func runCpWithFake(t *testing.T, fake *cpFakeHub, args []string) error {
	t.Helper()
	oldFactory := newHubFromFlagsFn
	t.Cleanup(func() { newHubFromFlagsFn = oldFactory })
	newHubFromFlagsFn = func(token, apiBase string, chunkSize int64, public bool, log logSettings) (hubClient, error) {
		return fake, nil
	}
	app, _, _ := newTestApp(t)
	return app.Run(args)
}

func TestCpReflinkAlwaysClonesWithoutStreaming(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("hello world")}}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=always", "demo", "a.txt", "b.txt"}); err != nil {
		t.Fatalf("cp always: %v", err)
	}
	if string(fake.files["b.txt"]) != "hello world" {
		t.Fatalf("clone bytes wrong, got %q", fake.files["b.txt"])
	}
	if fake.cloneCalls != 1 {
		t.Fatalf("always must clone exactly once, got %d", fake.cloneCalls)
	}
	if fake.readCalls+fake.writeCalls+fake.createCalls != 0 {
		t.Fatalf("always must never stream, got reads=%d writes=%d creates=%d", fake.readCalls, fake.writeCalls, fake.createCalls)
	}
}

func TestCpReflinkAlwaysFailsLoud(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("holes")}, cloneErr: context.DeadlineExceeded}
	err := runCpWithFake(t, fake, []string{"cp", "--reflink=always", "demo", "a.txt", "b.txt"})
	if err == nil {
		t.Fatal("always must fail loud when the clone fails")
	}
	if fake.readCalls+fake.writeCalls != 0 {
		t.Fatal("always must not fall back to streaming after a failed clone")
	}
	if _, ok := fake.files["b.txt"]; ok {
		t.Fatal("failed always-clone must not create the destination")
	}
}

func TestCpReflinkAutoFallsBackToStreaming(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("stream me")}, cloneErr: shfs.ErrCorrupted}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=auto", "demo", "a.txt", "b.txt"}); err != nil {
		t.Fatalf("cp auto: %v", err)
	}
	if string(fake.files["b.txt"]) != "stream me" {
		t.Fatalf("fallback bytes wrong, got %q", fake.files["b.txt"])
	}
	if fake.cloneCalls != 1 {
		t.Fatalf("auto must attempt the clone first, got %d clone calls", fake.cloneCalls)
	}
	if fake.writeCalls == 0 || fake.readCalls == 0 {
		t.Fatalf("auto fallback must stream, got reads=%d writes=%d", fake.readCalls, fake.writeCalls)
	}
}

func TestCpReflinkNeverStreamsWithoutCloning(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("0123456789")}}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=never", "demo", "a.txt", "b.txt", "2", "0", "4"}); err != nil {
		t.Fatalf("cp never: %v", err)
	}
	if string(fake.files["b.txt"]) != "2345" {
		t.Fatalf("range stream bytes wrong, got %q", fake.files["b.txt"])
	}
	if fake.cloneCalls != 0 {
		t.Fatalf("never must not call the clone op, got %d", fake.cloneCalls)
	}
	if fake.writeCalls == 0 {
		t.Fatal("never must copy through the streaming path")
	}
}

func TestCpRangeOffsetsReachTheClone(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("0123456789"), "b.txt": []byte("XXXX")}}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=always", "demo", "a.txt", "b.txt", "4", "1", "3"}); err != nil {
		t.Fatalf("cp range: %v", err)
	}
	if string(fake.files["b.txt"]) != "X456" {
		t.Fatalf("range clone bytes wrong, got %q", fake.files["b.txt"])
	}
	if fake.lastSrcOff != 4 || fake.lastDstOff != 1 || fake.lastLength != 3 {
		t.Fatalf("offsets must reach the clone, got %d %d %d", fake.lastSrcOff, fake.lastDstOff, fake.lastLength)
	}
}

func TestCpUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"cp", "--reflink=sometimes", "demo", "a.txt", "b.txt"},
		{"cp", "demo", "a.txt"},
		{"cp", "demo", "a.txt", "b.txt", "0"},
		{"cp", "demo", "a.txt", "b.txt", "0", "0"},
		{"cp", "demo", "a.txt", "b.txt", "-1", "0", "4"},
	} {
		fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("x")}}
		err := runCpWithFake(t, fake, args)
		if err == nil || !IsUsageError(err) {
			t.Fatalf("cp %q must be a usage error (exit 2), got %v", strings.Join(args, " "), err)
		}
	}
}
