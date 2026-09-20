package storage

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
)

// TestAllocInodeAvoidingSkipsClaimed pins the pigeonhole behavior with a
// crafted replayPlan: the allocator must skip every claimed id and return
// the first fresh one with a nil error.
func TestAllocInodeAvoidingSkipsClaimed(t *testing.T) {
	t.Parallel()
	meta := newTestMeta("p")
	base := meta.NextInode
	plan := newReplayPlan([]Op{
		{Type: OpPutFile, Paths: []string{"a"}, File: &FileMeta{Inode: base}},
		{Type: OpPutFile, Paths: []string{"b"}, File: &FileMeta{Inode: base + 1}},
		{Type: OpMkdir, Paths: []string{"d"}, Dir: &DirMeta{Inode: base + 2}},
	})
	id, err := plan.allocInodeAvoiding(meta)
	if err != nil {
		t.Fatalf("allocInodeAvoiding: %v", err)
	}
	if id != base+3 {
		t.Fatalf("allocInodeAvoiding = %d, want %d (must skip claimed %d..%d)", id, base+3, base, base+2)
	}
}

// TestAllocChunkAvoidingSkipsClaimed is the chunk-id mirror of the inode
// skip test.
func TestAllocChunkAvoidingSkipsClaimed(t *testing.T) {
	t.Parallel()
	meta := newTestMeta("p")
	base := meta.NextChunkID
	plan := newReplayPlan([]Op{
		{Type: OpPutFile, Paths: []string{"a"}, File: &FileMeta{Chunks: []int64{base, base + 1}}},
	})
	id, err := plan.allocChunkAvoiding(meta)
	if err != nil {
		t.Fatalf("allocChunkAvoiding: %v", err)
	}
	if id != base+2 {
		t.Fatalf("allocChunkAvoiding = %d, want %d", id, base+2)
	}
}

// TestAllocInodeAvoidingCounterExhaustion drives the inode allocator to
// true exhaustion: the counter has no fresh value left (minting would wrap),
// so the allocator must fail closed with an ENOSPC-wrapped error.
func TestAllocInodeAvoidingCounterExhaustion(t *testing.T) {
	t.Parallel()
	meta := newTestMeta("p")
	meta.NextInode = math.MaxUint64
	plan := newReplayPlan(nil)
	if _, err := plan.allocInodeAvoiding(meta); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("allocInodeAvoiding at counter exhaustion = %v, want ENOSPC-wrapped error", err)
	}
}

// TestAllocChunkAvoidingCounterExhaustion is the chunk-id mirror: no fresh
// chunk id remains, so the allocator must fail closed with ENOSPC.
func TestAllocChunkAvoidingCounterExhaustion(t *testing.T) {
	t.Parallel()
	meta := newTestMeta("p")
	meta.NextChunkID = math.MaxInt64
	plan := newReplayPlan(nil)
	if _, err := plan.allocChunkAvoiding(meta); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("allocChunkAvoiding at counter exhaustion = %v, want ENOSPC-wrapped error", err)
	}
}

// TestOpTypeUnmarshalRejectsUnknown pins fail-closed decoding: an unknown
// wire value must error and name the bad value.
func TestOpTypeUnmarshalRejectsUnknown(t *testing.T) {
	t.Parallel()
	var v OpType
	if err := v.UnmarshalText([]byte("bogus")); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("UnmarshalText(bogus) = %v, want error naming the value", err)
	}
	var j OpType
	if err := json.Unmarshal([]byte(`"bogus"`), &j); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("UnmarshalJSON(bogus) = %v, want error naming the value", err)
	}
	if v.Valid() || j.Valid() {
		t.Fatal("unknown value must not report Valid")
	}
}

// TestOpTypeRoundTripKnownValues pins journal compat: all 11 wire strings
// decode to themselves through both Text and JSON paths and report Valid.
func TestOpTypeRoundTripKnownValues(t *testing.T) {
	t.Parallel()
	known := []OpType{
		OpPutFile, OpDeleteFile, OpMkdir, OpRmdir, OpRename, OpSetattr,
		OpTruncate, OpPatch, OpXattr, OpRelease, OpChunkPrune,
	}
	if len(known) != 11 {
		t.Fatalf("expected 11 known op types, got %d", len(known))
	}
	for _, want := range known {
		var viaText OpType
		if err := viaText.UnmarshalText([]byte(string(want))); err != nil {
			t.Fatalf("UnmarshalText(%q): %v", string(want), err)
		}
		if viaText != want || !viaText.Valid() {
			t.Fatalf("UnmarshalText(%q) = %q valid=%v", string(want), string(viaText), viaText.Valid())
		}
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal %q: %v", string(want), err)
		}
		if string(raw) != `"`+string(want)+`"` {
			t.Fatalf("marshal %q = %s, want plain string", string(want), raw)
		}
		var viaJSON OpType
		if err := json.Unmarshal(raw, &viaJSON); err != nil {
			t.Fatalf("UnmarshalJSON(%q): %v", string(want), err)
		}
		if viaJSON != want || !viaJSON.Valid() {
			t.Fatalf("UnmarshalJSON(%q) = %q valid=%v", string(want), string(viaJSON), viaJSON.Valid())
		}
	}
}

// TestOpDecodeRejectsUnknownType pins the struct-level effect: a journal
// line carrying an unknown type must fail decode, not replay silently.
func TestOpDecodeRejectsUnknownType(t *testing.T) {
	t.Parallel()
	var op Op
	if err := json.Unmarshal([]byte(`{"seq":1,"type":"bogus","paths":["a"],"members":[]}`), &op); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("decode op with unknown type = %+v, %v; want error naming the value", op, err)
	}
}
