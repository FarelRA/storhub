package cli

import (
	"context"
	"errors"
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
	modes                  map[string]uint32
	cloneErr               error
	cloneCalls             int
	readCalls              int
	writeCalls             int
	createCalls            int
	chmodCalls             int
	lastSrcOff, lastDstOff int64
	lastLength             int64
}

func (h *cpFakeHub) modeOf(targetPath string) uint32 {
	if h.modes != nil {
		if mode, ok := h.modes[targetPath]; ok {
			return mode
		}
	}
	return 0o644
}

func (h *cpFakeHub) StatPathContext(_ context.Context, _, targetPath string) (*storhub.EntryInfo, error) {
	if data, ok := h.files[targetPath]; ok {
		return &storhub.EntryInfo{Path: targetPath, Size: int64(len(data)), Mode: h.modeOf(targetPath), Inode: 1, NLink: 1}, nil
	}
	return nil, shfs.NotFound(targetPath)
}

func (h *cpFakeHub) ChmodContext(_ context.Context, _, targetPath string, mode uint32) error {
	h.chmodCalls++
	if _, ok := h.files[targetPath]; !ok {
		return shfs.NotFound(targetPath)
	}
	if h.modes == nil {
		h.modes = map[string]uint32{}
	}
	h.modes[targetPath] = mode
	return nil
}

func (h *cpFakeHub) CreateFileContext(_ context.Context, _, filePath string) (*storhub.FileMetadata, error) {
	h.createCalls++
	if _, ok := h.files[filePath]; ok {
		return nil, shfs.AlreadyExists(filePath)
	}
	h.files[filePath] = []byte{}
	return &storhub.FileMetadata{Size: 0, Inode: 1, Mode: 0o644}, nil
}

func (h *cpFakeHub) ReadFileAtContext(_ context.Context, _, filePath string, offset, length int64) ([]byte, error) {
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

func (h *cpFakeHub) WriteFileAtContext(_ context.Context, _, filePath string, offset int64, data []byte, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
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

func (h *cpFakeHub) CloneRange(_ context.Context, _, src string, srcOff int64, dst string, dstOff int64, length int64, _ ...storhub.MutateOption) (*storhub.FileMetadata, error) {
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

func (h *cpFakeHub) DrainProjectContext(_ context.Context, _ string) error { return nil }
func (h *cpFakeHub) Shutdown(_ context.Context) error                      { return nil }

func runCpWithFake(t *testing.T, fake *cpFakeHub, args []string) error {
	t.Helper()
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
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

func TestCpAutoFallbackStreamsRangesWithoutCloning(t *testing.T) {
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("0123456789")}, cloneErr: errTestCloneDown}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=auto", "demo", "a.txt", "b.txt", "2", "0", "4"}); err != nil {
		t.Fatalf("cp auto fallback: %v", err)
	}
	if string(fake.files["b.txt"]) != "2345" {
		t.Fatalf("range stream bytes wrong, got %q", fake.files["b.txt"])
	}
	if fake.writeCalls == 0 {
		t.Fatal("auto fallback must copy through the streaming path")
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
		{"cp", "--reflink=never", "demo", "a.txt", "b.txt"},
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

// TestCpAutoGuardedCloneFailureReturnsCloneError pins the guarded-cp
// contract: with --expectedrevision set there is no streaming fallback
// (it cannot honor the compare-and-swap), so a failed clone fails the
// command with the clone error instead of exiting 0 unapplied.
func TestCpAutoGuardedCloneFailureReturnsCloneError(t *testing.T) {
	cloneErr := errors.New("clone unavailable: backend down")
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("guarded")}, cloneErr: cloneErr}
	app, _, _ := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	err := app.Run([]string{"cp", "--reflink=auto", "--expectedrevision", "rev1", "demo", "a.txt", "b.txt"})
	if err == nil {
		t.Fatal("guarded clone failure must fail the command, got nil (exit 0 unapplied)")
	}
	if !errors.Is(err, cloneErr) {
		t.Fatalf("guarded clone failure must return the clone error, got %v", err)
	}
	if fake.readCalls+fake.writeCalls+fake.createCalls != 0 {
		t.Fatalf("guarded failure must not fall back to streaming, got reads=%d writes=%d creates=%d",
			fake.readCalls, fake.writeCalls, fake.createCalls)
	}
	if _, ok := fake.files["b.txt"]; ok {
		t.Fatal("guarded failure must not create the destination")
	}
}

// TestCpAutoFallbackWarnsWithCloneError pins the diagnostic half: an
// unguarded clone failure falls back to streaming and warns with the
// real clone error, never a nil one.
func TestCpAutoFallbackWarnsWithCloneError(t *testing.T) {
	cloneErr := errors.New("clone broke: no clone op")
	fake := &cpFakeHub{files: map[string][]byte{"a.txt": []byte("stream me")}, cloneErr: cloneErr}
	app, _, stderr := newTestApp(t)
	app.seams.newHub = func(_ context.Context, _, _ string, _ int64, _ bool, _ logSettings) (hubClient, error) {
		return fake, nil
	}
	if err := app.Run([]string{"cp", "--reflink=auto", "demo", "a.txt", "b.txt"}); err != nil {
		t.Fatalf("unguarded fallback must succeed, got %v", err)
	}
	if string(fake.files["b.txt"]) != "stream me" {
		t.Fatalf("fallback bytes wrong, got %q", fake.files["b.txt"])
	}
	if out := stderr(); !strings.Contains(out, "clone broke: no clone op") {
		t.Fatalf("fallback warning must carry the real clone error, got %q", out)
	}
}

// A streaming copy onto a fresh path takes the source permission bits
// minus setuid/setgid, matching the CopyContext/CloneRange rule so the
// --reflink choice never changes the result mode.
func TestCpStreamingCopyUnifiesDestinationMode(t *testing.T) {
	fake := &cpFakeHub{
		files: map[string][]byte{"locked.txt": []byte("secret"), "setuid.txt": []byte("tool")},
		modes: map[string]uint32{"locked.txt": 0o600, "setuid.txt": 0o4755},
	}
	fake.cloneErr = errTestCloneDown
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=auto", "demo", "locked.txt", "locked-copy.txt"}); err != nil {
		t.Fatalf("cp auto fallback: %v", err)
	}
	if got := fake.modeOf("locked-copy.txt"); got != 0o600 {
		t.Fatalf("streaming copy mode = %o, want source 600", got)
	}
	if err := runCpWithFake(t, fake, []string{"cp", "--reflink=auto", "demo", "setuid.txt", "tool-copy.txt"}); err != nil {
		t.Fatalf("cp auto fallback: %v", err)
	}
	if got := fake.modeOf("tool-copy.txt"); got != 0o755 {
		t.Fatalf("streaming copy mode = %o, want 755 (setuid cleared)", got)
	}
	if fake.chmodCalls != 2 {
		t.Fatalf("streaming copy must chmod each fresh destination, got %d chmod calls", fake.chmodCalls)
	}
}
