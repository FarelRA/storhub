package fusefs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path"
	"strings"
	"syscall"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/metadata"
)

// A 2000-byte dirty span with a 4-byte overlay page must commit as 500
// capped edits, never one 2000-byte allocation. Fails on the old code,
// which sent a single whole-span edit.
func TestCommitPatchStreamsBoundedEdits(t *testing.T) {
	t.Parallel()
	var got []shfs.RangeEdit
	hub := &stubHub{chunkSize: 64}
	hub.patchRanges = func(edits []shfs.RangeEdit) (*metadata.FileMeta, error) {
		got = append([]shfs.RangeEdit{}, edits...)
		return &metadata.FileMeta{}, nil
	}
	fsys := mustMount(t, hub, "", Options{OverlayBufferSize: 4})
	ctx := context.Background()
	const size = 2000
	h, err := fsys.newHandle(ctx, 7, "bounds.bin", syscall.O_WRONLY, &writeBootstrap{baseSize: size})
	if err != nil {
		t.Fatalf("new handle: %v", err)
	}
	want := make([]byte, size)
	for i := range want {
		want[i] = byte(i % 251)
	}
	if n, errno := h.Write(ctx, want, 0); errno != 0 || n != size {
		t.Fatalf("write: n=%d errno=%v", n, errno)
	}
	ws := h.snapshotWriteState()
	ws.mu.Lock()
	planned := ws.plannedRangesLocked()
	ws.mu.Unlock()
	if len(planned) != 1 || planned[0].Start != 0 || planned[0].End != size {
		t.Fatalf("planned ranges must be one [0,%d) span, got %+v", size, planned)
	}
	// Caller must hold ws.mu; a successful return leaves it unlocked.
	ws.mu.Lock()
	var notifies commitNotifies
	if errno := h.commitPatch(ctx, "bounds.bin", size, size, planned, shfs.MetadataPatch{}, &notifies); errno != 0 {
		t.Fatalf("commitPatch: %v", errno)
	}
	if len(got) != size/4 {
		t.Fatalf("bounded commit must split into %d edits, got %d", size/4, len(got))
	}
	reassembled := make([]byte, 0, size)
	for i, edit := range got {
		if edit.Start != int64(i*4) {
			t.Fatalf("edit %d starts at %d, want %d", i, edit.Start, i*4)
		}
		if len(edit.Data) > 4 {
			t.Fatalf("edit %d holds %d bytes, exceeding the page cap", i, len(edit.Data))
		}
		if edit.DeleteSize != int64(len(edit.Data)) {
			t.Fatalf("edit %d delete size %d mismatches data length %d", i, edit.DeleteSize, len(edit.Data))
		}
		reassembled = append(reassembled, edit.Data...)
	}
	if !bytes.Equal(reassembled, want) {
		t.Fatal("reassembled edits differ from the written bytes")
	}
}

// A 2.5 MiB recorded span must redrive as three capped edits (1 MiB cap),
// never one 2.5 MiB allocation. Fails on the old code, which sent a
// single whole-span edit.
func TestRedriveRangesStreamsBoundedEdits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	const size = 5 * mountMaxIOSize / 2
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	fp := &targetFingerprint{Size: size, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	seedRecovery(t, dir, "docs/f.txt", payload,
		quarantineIntent{
			fullImage:   false,
			ranges:      [][2]int64{{0, size}},
			baseSize:    size,
			logicalSize: size,
			fingerprint: fp,
		})
	var got []shfs.RangeEdit
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: size, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		patchRanges: func(edits []shfs.RangeEdit) (*metadata.FileMeta, error) {
			got = append([]shfs.RangeEdit{}, edits...)
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, nil)
	if len(got) != 3 {
		t.Fatalf("bounded redrive must split into 3 edits, got %d", len(got))
	}
	reassembled := make([]byte, 0, size)
	for i, edit := range got {
		if len(edit.Data) > redrivePatchEditCap {
			t.Fatalf("edit %d holds %d bytes, exceeding the redrive cap", i, len(edit.Data))
		}
		if edit.Start != int64(i)*redrivePatchEditCap {
			t.Fatalf("edit %d starts at %d, want %d", i, edit.Start, int64(i)*redrivePatchEditCap)
		}
		reassembled = append(reassembled, edit.Data...)
	}
	if !bytes.Equal(reassembled, payload) {
		t.Fatal("reassembled redrive edits differ from the payload")
	}
}

// A go-fuse panic during attach must log at Error, never Debug: a
// swallowed panic is an operational failure. A detached node (nil bridge)
// panics deterministically in Root, exercising the recover path without
// a mount. Fails on the old code, which logged at Debug (silent here).
func TestAttachChildPanicLogsAtError(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	fsys := mustMount(t, &stubHub{}, "", Options{Logger: logger})
	parent := &storhubNode{fs: fsys, inode: 99, isDir: true}
	child := &storhubNode{fs: fsys, inode: 100}
	if ino := parent.attachChild(context.Background(), child); ino != nil {
		t.Fatalf("detached attach must degrade to nil, got %v", ino)
	}
	if !strings.Contains(buf.String(), "attachChild failed") {
		t.Fatalf("panic must log attachChild failed at Error, got %q", buf.String())
	}
}

// The inventory must order by quarantine time, not by filename: lexical
// SavedPath order only matches age incidentally via the stamp format.
func TestRecoveryInventorySortsByCreatedAt(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	writeEntry := func(name string, created int64) {
		t.Helper()
		if err := os.WriteFile(path.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatalf("seed payload: %v", err)
		}
		raw, err := json.Marshal(RecoveryEntry{SavedPath: path.Join(dir, name), TargetPath: "t", Reason: "x", CreatedAt: created})
		if err != nil {
			t.Fatalf("sidecar: %v", err)
		}
		if err := os.WriteFile(path.Join(dir, name+".json"), append(raw, '\n'), 0o600); err != nil {
			t.Fatalf("sidecar write: %v", err)
		}
	}
	writeEntry("a.dat", 200)
	writeEntry("b.dat", 100)
	inv, err := readRecoveryInventory(dir)
	if err != nil {
		t.Fatalf("inventory: %v", err)
	}
	if len(inv) != 2 || inv[0].CreatedAt != 100 || inv[1].CreatedAt != 200 {
		t.Fatalf("inventory must sort oldest first by CreatedAt, got %+v", inv)
	}
}
