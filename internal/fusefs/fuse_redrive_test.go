package fusefs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path"
	"testing"

	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	"github.com/FarelRA/storhub/internal/metadata"
)

var errTestRedrive = errors.New("redrive replace injected failure")

// seedRecovery quarantines payload under dir with the given intent and
// returns the saved data path, using the real quarantine path (not a
// hand-written sidecar) so the sidecar contract is tested, not assumed.
func seedRecovery(t *testing.T, dir, target string, payload []byte, intent quarantineIntent) string {
	t.Helper()
	src := path.Join(t.TempDir(), "overlay")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatalf("seed payload: %v", err)
	}
	saved := quarantineIntoDirWithIntent(src, dir, target, quarantineReasonCommitFailure, intent, nil)
	if saved == "" {
		t.Fatal("quarantine preserved nothing")
	}
	return saved
}

func readSidecar(t *testing.T, saved string) RecoveryEntry {
	t.Helper()
	raw, err := os.ReadFile(saved + ".json")
	if err != nil {
		t.Fatalf("sidecar missing: %v", err)
	}
	var entry RecoveryEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		t.Fatalf("sidecar corrupt: %v", err)
	}
	return entry
}

func TestQuarantineSidecarRecordsIntent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	intent := quarantineIntent{
		fullImage:   true,
		ranges:      [][2]int64{{0, 5}},
		baseSize:    3,
		logicalSize: 5,
		fingerprint: &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101},
	}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"), intent)
	entry := readSidecar(t, saved)
	if !entry.FullImage || entry.TargetPath != "docs/f.txt" {
		t.Fatalf("sidecar lost identity: %+v", entry)
	}
	if len(entry.Ranges) != 1 || entry.Ranges[0] != [2]int64{0, 5} {
		t.Fatalf("sidecar lost ranges: %+v", entry.Ranges)
	}
	if entry.BaseSize != 3 || entry.LogicalSize != 5 {
		t.Fatalf("sidecar lost sizes: %+v", entry)
	}
	fp := entry.Fingerprint
	if fp == nil || *fp != *intent.fingerprint {
		t.Fatalf("sidecar lost fingerprint: %+v", fp)
	}
}

// TestRedriveCommitsMatchingFullImage pins the C11 contract end to end: a
// full-image temp whose target still matches the fingerprint is re-uploaded
// and only then removed from recovery/.
func TestRedriveCommitsMatchingFullImage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"),
		quarantineIntent{fullImage: true, fingerprint: fp})
	var replaced []byte
	var replacedTarget string
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		replaceFile: func(_ context.Context, _, target, inputPath string) (*metadata.FileMeta, error) {
			replacedTarget = target
			data, err := os.ReadFile(inputPath)
			if err != nil {
				t.Errorf("read redrive input: %v", err)
				return nil, err
			}
			replaced = data
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if replacedTarget != "docs/f.txt" || string(replaced) != "hello" {
		t.Fatalf("redrive must re-upload quarantined bytes to the target, got %q %q", replacedTarget, replaced)
	}
	if _, err := os.Stat(saved); !os.IsNotExist(err) {
		t.Fatal("committed redrive must remove the payload")
	}
	if _, err := os.Stat(saved + ".json"); !os.IsNotExist(err) {
		t.Fatal("committed redrive must remove the sidecar")
	}
}

func TestRedriveRefusesChangedTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"),
		quarantineIntent{fullImage: true, fingerprint: fp})
	replaced := false
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			// Concurrent writer advanced the target after quarantine.
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 6, Inode: 9, ModifiedAt: 102, ChangedAt: 102}, nil
		},
		replaceFile: func(context.Context, string, string, string) (*metadata.FileMeta, error) {
			replaced = true
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if replaced {
		t.Fatal("redrive over a changed target would destroy the concurrent write")
	}
	if _, err := os.Stat(saved); err != nil {
		t.Fatalf("refused redrive must keep quarantine data: %v", err)
	}
	if _, err := os.Stat(saved + ".json"); err != nil {
		t.Fatalf("refused redrive must keep the sidecar: %v", err)
	}
}

func TestRedriveSkipsNonEligible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := map[string]quarantineIntent{
		"range fragment":    {fullImage: false, fingerprint: &targetFingerprint{Size: 5, Inode: 9}},
		"missing print":     {fullImage: true},
		"handle provenance": {},
	}
	for name, intent := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"), intent)
			replaced := false
			hub := &stubHub{
				statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
					return &shfs.EntryInfo{Path: "docs/f.txt", Size: 5, Inode: 9}, nil
				},
				replaceFile: func(context.Context, string, string, string) (*metadata.FileMeta, error) {
					replaced = true
					return &metadata.FileMeta{}, nil
				},
			}
			redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
			if replaced {
				t.Fatalf("%s must never auto-redrive", name)
			}
			if _, err := os.Stat(saved); err != nil {
				t.Fatalf("%s must keep quarantine data: %v", name, err)
			}
		})
	}
}

func TestRedriveKeepsOnReplaceError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"),
		quarantineIntent{fullImage: true, fingerprint: fp})
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		replaceFile: func(context.Context, string, string, string) (*metadata.FileMeta, error) {
			return nil, errTestRedrive
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if _, err := os.Stat(saved); err != nil {
		t.Fatalf("failed redrive must keep quarantine data: %v", err)
	}
}

func TestRedriveIgnoresCorruptSidecar(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	orphan := path.Join(dir, "orphan.tmp")
	if err := os.WriteFile(orphan, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan+".json", []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	replaced := false
	hub := &stubHub{
		replaceFile: func(context.Context, string, string, string) (*metadata.FileMeta, error) {
			replaced = true
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if replaced {
		t.Fatal("corrupt sidecar must never redrive")
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("corrupt entry payload must be preserved: %v", err)
	}
}

func TestRedrivePatchesRecordedRanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 10, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	// Payload holds the full 10 bytes; only spans [2,5) and [7,9) were dirty.
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("0123456789"),
		quarantineIntent{
			fullImage:   false,
			ranges:      [][2]int64{{2, 5}, {7, 9}},
			baseSize:    10,
			logicalSize: 10,
			fingerprint: fp,
		})
	var gotEdits []shfs.RangeEdit
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 10, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		patchRanges: func(edits []shfs.RangeEdit) (*metadata.FileMeta, error) {
			gotEdits = append([]shfs.RangeEdit{}, edits...)
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if len(gotEdits) != 2 {
		t.Fatalf("range redrive must patch exactly the recorded spans, got %+v", gotEdits)
	}
	if gotEdits[0].Start != 2 || string(gotEdits[0].Data) != "234" || gotEdits[0].DeleteSize != 3 {
		t.Fatalf("first span wrong: %+v", gotEdits[0])
	}
	if gotEdits[1].Start != 7 || string(gotEdits[1].Data) != "78" || gotEdits[1].DeleteSize != 2 {
		t.Fatalf("second span wrong: %+v", gotEdits[1])
	}
	if _, err := os.Stat(saved); !os.IsNotExist(err) {
		t.Fatal("patched redrive must remove the payload")
	}
	if _, err := os.Stat(saved + ".json"); !os.IsNotExist(err) {
		t.Fatal("patched redrive must remove the sidecar")
	}
}

func TestRedriveTruncatesBareSizeChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 10, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("0123456789"),
		quarantineIntent{fullImage: false, baseSize: 10, logicalSize: 6, fingerprint: fp})
	var truncatedTo int64 = -1
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 10, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		truncateFile: func(_ context.Context, _, _ string, size int64) (*metadata.FileMeta, error) {
			truncatedTo = size
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if truncatedTo != 6 {
		t.Fatalf("truncate redrive must resize to the recorded logical size, got %d", truncatedTo)
	}
	if _, err := os.Stat(saved); !os.IsNotExist(err) {
		t.Fatal("truncated redrive must remove the payload")
	}
}

func TestRedriveAppliesPendingPatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	intent := quarantineIntent{fullImage: true, fingerprint: fp}
	intent.hasPending = true
	intent.pending = shfs.MetadataPatch{HasMode: true, Mode: 0o640}
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"), intent)
	var patched *shfs.MetadataPatch
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		replaceFile: func(context.Context, string, string, string) (*metadata.FileMeta, error) {
			return &metadata.FileMeta{}, nil
		},
		applyPatch: func(_ context.Context, _, _ string, patch shfs.MetadataPatch) error {
			patched = &patch
			return nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if patched == nil || !patched.HasMode || patched.Mode != 0o640 {
		t.Fatalf("redrive must apply the staged metadata patch, got %+v", patched)
	}
	if _, err := os.Stat(saved); !os.IsNotExist(err) {
		t.Fatal("patched redrive must remove the payload")
	}
}

func TestRedriveRefusesSpanOutsidePayload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	fp := &targetFingerprint{Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}
	// Recorded span [0,99) exceeds the 5-byte payload: a truncated temp
	// must never redrive partial ranges.
	saved := seedRecovery(t, dir, "docs/f.txt", []byte("hello"),
		quarantineIntent{fullImage: false, ranges: [][2]int64{{0, 99}}, fingerprint: fp})
	patched := false
	hub := &stubHub{
		statPath: func(context.Context, string, string) (*shfs.EntryInfo, error) {
			return &shfs.EntryInfo{Path: "docs/f.txt", Size: 5, Inode: 9, ModifiedAt: 100, ChangedAt: 101}, nil
		},
		patchRanges: func([]shfs.RangeEdit) (*metadata.FileMeta, error) {
			patched = true
			return &metadata.FileMeta{}, nil
		},
	}
	redriveRecoveryInventory(ctx, hub, "demo", dir, logging.NewLogger(logging.Options{}))
	if patched {
		t.Fatal("out-of-payload span must refuse the whole entry")
	}
	if _, err := os.Stat(saved); err != nil {
		t.Fatalf("refused redrive must keep quarantine data: %v", err)
	}
}
