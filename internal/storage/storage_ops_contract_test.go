package storage

import (
	"strings"
	"testing"
)

// Commit-message summary classes must follow the op classifier: every
// state-class type except mkdir counts as put, mkdir counts as mkdir,
// every delete-class type counts as del.
func TestOpSummaryPutClassMatchesStateClassifier(t *testing.T) {
	t.Parallel()
	var ops []Op
	for _, typ := range []OpType{OpPutFile, OpTruncate, OpPatch, OpSetattr, OpXattr, OpMkdir, OpDeleteFile, OpRmdir, OpRename, OpRelease, OpChunkPrune} {
		op := Op{Type: typ, Paths: []string{"p"}, Cause: "c", Timestamp: 1}
		if typ == OpRelease {
			tag := "v1"
			op.Tag = tag
		}
		ops = append(ops, op)
	}
	got := opSummaryCounts(ops)
	if !strings.Contains(got, "5 put") {
		t.Fatalf("put class must group the five non-mkdir state types, got %q", got)
	}
	for _, want := range []string{"1 mkdir", "2 del", "1 rename", "1 release", "1 chunkprune"} {
		if !strings.Contains(got, want) {
			t.Fatalf("summary %q must contain %q", got, want)
		}
	}
}

// An xattr op carrying a record asserts its path like any other state op;
// a record-less xattr asserts nothing.
func TestOpAssertPathsCoversRecordCarryingXattr(t *testing.T) {
	t.Parallel()
	file := FileMeta{Inode: 7}
	withRecord := Op{Type: OpXattr, Paths: []string{"a"}, XAttr: "k", File: &file}
	paths := opAssertPaths(withRecord)
	if len(paths) != 1 || paths[0] != "a" {
		t.Fatalf("record-carrying xattr must assert its path, got %v", paths)
	}
	bare := Op{Type: OpXattr, Paths: []string{"a"}, XAttr: "k"}
	if len(opAssertPaths(bare)) != 0 {
		t.Fatalf("record-less xattr must assert nothing, got %v", opAssertPaths(bare))
	}
}

// A committed message re-parsed as a cause must fall back, never leak the
// summary count word into op causes.
func TestCauseFromCommittedMessageFallsBack(t *testing.T) {
	t.Parallel()
	ops := []Op{{Type: OpPutFile, Paths: []string{"a"}, Cause: "upload", Timestamp: 1}}
	msg := buildCommitMessage(ops, "abcdef0123456789")
	if got := causeFromMessage(msg); got != "update" {
		t.Fatalf("committed message must fall back to update, got %q", got)
	}
}

func TestCauseFromLegacyMessages(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"storhub: mkdir /a":        "mkdir",
		"storhub: update metadata": "update",
		"":                         "update",
		"storhub:":                 "update",
		"storhub: revert a to b":   "revert",
	}
	for in, want := range cases {
		if got := causeFromMessage(in); got != want {
			t.Fatalf("causeFromMessage(%q) = %q, want %q", in, got, want)
		}
	}
}

// Equality helpers ignore change stamps but catch content drift.
func TestFileAndDirEqualityIgnoresChangeStamps(t *testing.T) {
	t.Parallel()
	a := FileMeta{Size: 3, Mode: 0o644, Inode: 9, ChangedAt: 1}
	b := FileMeta{Size: 3, Mode: 0o644, Inode: 9, ChangedAt: 2}
	if !fileEqual(a, b, true) {
		t.Fatal("fileEqual must ignore ChangedAt")
	}
	b.Size = 4
	if fileEqual(a, b, true) {
		t.Fatal("fileEqual must catch size drift")
	}
	da := DirMeta{Mode: 0o755, Inode: 4, ModifiedAt: 1, ChangedAt: 1}
	db := DirMeta{Mode: 0o755, Inode: 4, ModifiedAt: 9, ChangedAt: 9}
	if !dirEqual(da, db, true) {
		t.Fatal("dirEqual must ignore time fields")
	}
	db.Mode = 0o700
	if dirEqual(da, db, true) {
		t.Fatal("dirEqual must catch mode drift")
	}
}

func TestMarkProjectDirtyFastLocked(t *testing.T) {
	t.Parallel()
	pm := &projectMetadata{triggerCh: make(chan struct{}, 1)}
	ch, ok := markProjectDirtyFastLocked(pm)
	if !ok || ch == nil {
		t.Fatal("live instance must take the fast dirty path")
	}
	if !pm.dirty {
		t.Fatal("fast path must mark dirty")
	}
	evicted := &projectMetadata{stopped: true, triggerCh: make(chan struct{}, 1)}
	if _, ok := markProjectDirtyFastLocked(evicted); ok {
		t.Fatal("evicted instance must miss the fast dirty path")
	}
}

func TestFreezeCommitBatchCapturesBatch(t *testing.T) {
	t.Parallel()
	h := &StorHub{config: DefaultConfig().WithDefaults()}
	pm := &projectMetadata{meta: NewRepoMetadata("p"), dirty: true}
	snap := h.freezeCommitBatch("p", pm)
	if snap == nil {
		t.Fatal("dirty project must yield a commit batch")
	}
	if snap.previousSHA != "" || snap.version != 0 {
		t.Fatalf("fresh batch must carry empty token and zero version, got %+v", snap)
	}
	pm.mu.Lock()
	pm.dirty = false
	pm.mu.Unlock()
	if h.freezeCommitBatch("p", pm) != nil {
		t.Fatal("clean project must yield no batch")
	}
}

func TestHealBaseTreeFillsNilBaseline(t *testing.T) {
	t.Parallel()
	meta := NewRepoMetadata("p")
	pm := &projectMetadata{meta: meta}
	healBaseTreeLocked(pm)
	if pm.baseTree != meta {
		t.Fatal("nil baseline must heal to the live tree")
	}
	other := NewRepoMetadata("p")
	pm.baseTree = other
	healBaseTreeLocked(pm)
	if pm.baseTree != other {
		t.Fatal("existing baseline must be left alone")
	}
}

func TestReplayBatchBuildsSharedState(t *testing.T) {
	t.Parallel()
	file := FileMeta{Inode: 11}
	ops := []Op{{Type: OpPutFile, Paths: []string{"a"}, Cause: "c", Timestamp: 1, File: &file}}
	plan, cidx := newReplayBatch(NewRepoMetadata("p"), ops)
	if plan == nil || cidx == nil {
		t.Fatal("replay batch must build plan and collision state")
	}
	if _, ok := plan.targets["a"]; !ok {
		t.Fatalf("batch targets must include asserted path, got %v", plan.targets)
	}
}

func TestShortSHADisplayTruncation(t *testing.T) {
	t.Parallel()
	if got := shortSHA("abcdef0123456789"); got != "abcdef012345" {
		t.Fatalf("long value must truncate to 12, got %q", got)
	}
	if got := shortSHA("abc"); got != "abc" {
		t.Fatalf("short value must pass through, got %q", got)
	}
}
