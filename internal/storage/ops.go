package storage

import (
	"encoding/json"
	"fmt"
)

// OpType classifies one metadata operation in the pending op stack.
type OpType string

// Op wire values: frozen strings, validated on unmarshal.
const (
	OpPutFile    OpType = "put"
	OpDeleteFile OpType = "del"
	OpMkdir      OpType = "mkdir"
	OpRmdir      OpType = "rmdir"
	OpRename     OpType = "rename"
	OpSetattr    OpType = "setattr"
	OpTruncate   OpType = "trunc"
	OpPatch      OpType = "patch"
	OpXattr      OpType = "xattr"
	OpRelease    OpType = "release"
	OpChunkPrune OpType = "chunkprune"
)

// Valid reports whether t is one of the 11 known wire values. The wire
// strings are frozen for journal compatibility; unknown values fail closed
// at decode time (UnmarshalText/UnmarshalJSON) instead of replaying
// silently.
func (t OpType) Valid() bool {
	switch t {
	case OpPutFile, OpDeleteFile, OpMkdir, OpRmdir, OpRename, OpSetattr,
		OpTruncate, OpPatch, OpXattr, OpRelease, OpChunkPrune:
		return true
	default:
		return false
	}
}

// UnmarshalText validates one wire value, rejecting unknown op types with
// an error naming the bad value.
func (t *OpType) UnmarshalText(text []byte) error {
	v := OpType(text)
	if !v.Valid() {
		return fmt.Errorf("unknown op type %q", string(text))
	}
	*t = v
	return nil
}

// UnmarshalJSON validates a JSON-encoded wire value the same way. Marshal
// stays a plain string so the wire encoding is byte-identical.
func (t *OpType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	return t.UnmarshalText([]byte(s))
}

// Op is one discrete, self-contained metadata operation. Every op carries
// the FULL resulting state for its path scope, never a delta: replaying the
// stack against arbitrary upstream state is what makes rebase-on-conflict
// possible, and full-state assertions make replay idempotent (a journal
// replayed after a crash that landed between commit and journal truncation
// re-applies harmlessly).
type Op struct {
	Seq       uint64   `json:"seq"`
	Type      OpType   `json:"type"`
	Paths     []string `json:"paths"` // scope: 1 path, 2 (from, to) for rename
	Cause     string   `json:"cause"` // originating operation, e.g. "upload", "mkdir"
	Timestamp int64    `json:"ts"`    // unix nanoseconds of the latest coalesced mutation
	Times     int      `json:"times,omitempty"`

	// Full resulting state for the op scope. File/Dir is the complete
	// entry as it should exist after replay; Chunks carries the chunk
	// catalog records the entry references.
	File   *FileMeta           `json:"file,omitempty"`
	Dir    *DirMeta            `json:"dir,omitempty"`
	Chunks map[int64]ChunkInfo `json:"chunks,omitempty"`

	FreedChunks   int         `json:"freed_chunks,omitempty"`   // del: chunk records the removed entry referenced
	Tag           string      `json:"tag,omitempty"`            // release op
	Release       *ReleaseRef `json:"release,omitempty"`        // release op: nil = delete the tag
	RemovedChunks []int64     `json:"removed_chunks,omitempty"` // chunkprune op
	XAttr         string      `json:"xattr,omitempty"`          // xattr op

	// Members carries the explicit subtree member from-paths for a
	// directory rename, recorded at fold time from the candidate tree.
	// Replay moves exactly these members (missing members are skipped),
	// so a rename never sweeps up entries created under the old path by
	// a LATER op in the same batch: both delivery orders converge to the
	// candidate. The empty list is meaningful (rename covered an empty
	// subtree: move nothing but the dir record) and round-trips through
	// JSON as [], which is why this field has NO omitempty. Nil means
	// legacy (move the live subtree); only journals written before
	// member lists existed take that path.
	Members []string `json:"members"` // dir rename only

	// SnapSeq is retained for READ compatibility only: journals written
	// before generations existed stamped the snapshot mark outstanding at
	// append time here (`snap` key, 0 when none). New deltas never set it
	// (the generation boundary subsumes the mark), and the fold ignores
	// it: legacy marked lines merge freely within the legacy generation.
	// Shape-divergence vs the old fold is possible only for a chain split
	// by an in-flight snapshot at crash time, and both shapes replay to
	// the same tree (pinned by TestJournalGenCompatSnapMarkedLinesConverge),
	// so no reader needs the mark back. The field and its JSON key stay
	// so old lines keep parsing.
	SnapSeq uint64 `json:"snap,omitempty"`

	// Gen is the op's generation: the open generation at append time.
	// Merges (rename-chain collapse, rename-then-delete) apply only
	// within one generation, so a commit freeze between two appends keeps
	// them split in the live stack and in every refold. Absent in
	// journals written before generations existed, which decode as 0
	// (the legacy generation: merge freely, the old behavior).
	Gen uint64 `json:"gen,omitempty"`
}

// ConflictResolution records one policy decision made while replaying ops,
// so a commit message (and strict mode) can surface exactly what was
// resolved instead of silently picking winners.
type ConflictResolution struct {
	Seq  uint64
	Path string
	Note string
}

// opStack accumulates pending metadata operations with per-path coalescing:
// fifty writes of one file collapse into one op carrying the final state
// and a times counter, keeping stacks (and commit messages) proportional to
// what changed, not to how many syscalls produced it.
//
// Coalescing lookups are indexed (path -> stack position) so bulk imports
// of many distinct paths stay O(1) per append; a coalesce rebuilds the op
// at the END of the stack, preserving replay-order semantics when renames
// intervene. Rename chains and rename-then-delete collapse only when
// adjacent, for the same reason.
//
// The stack is bounded twice: maxPendingOpsPerProject caps the op count and
// opStackMaxBytes caps the serialized weight (each op carries full state
// plus chunk catalog records, so a few ops on huge files can outweigh
// thousands of tiny ones). Either bound crossing compacts the journal and
// force-retries the commit: acknowledged ops are never dropped.

func isStateClass(t OpType) bool {
	switch t {
	case OpPutFile, OpTruncate, OpPatch, OpSetattr, OpXattr, OpMkdir:
		return true
	}
	return false
}

func isDeleteClass(t OpType) bool {
	return t == OpDeleteFile || t == OpRmdir
}

func opPath(op Op) string {
	if len(op.Paths) > 0 {
		return op.Paths[0]
	}
	return ""
}

func recordResolution(resolutions *[]ConflictResolution, op Op, path, note string) {
	if resolutions == nil {
		return
	}
	*resolutions = append(*resolutions, ConflictResolution{Seq: op.Seq, Path: path, Note: note})
}

// cloneOpPayloads copies an op's state payloads so replay-time rewrites
// (collision remapping) never mutate the caller's stack entry.
func cloneOpPayloads(op Op) Op {
	if op.File != nil {
		f := op.File.Clone()
		op.File = &f
	}
	if op.Dir != nil {
		d := op.Dir.Clone()
		op.Dir = &d
	}
	if op.Chunks != nil {
		chunks := make(map[int64]ChunkInfo, len(op.Chunks))
		for id, info := range op.Chunks {
			chunks[id] = info
		}
		op.Chunks = chunks
	}
	return op
}

// collisionIndex snapshots inode occupancy once per replay batch so
// remapOpCollisions stays O(1) per op instead of scanning the whole tree per
// replayed op (bulk journal/cold replays were O(ops x tree)). The index is
// updated incrementally as the batch applies (every handler records its
// writes/removes/moves), so it tracks the live tree exactly without rescans:
//
//   - adds are exact: every materializing handler records its payload inode,
//     and batch inodes are unique within the candidate domain (hardlink
//     families share file inodes, which never remap);
//   - removes drop by path, so a stale entry can only cause a spurious
//     remap (fresh inode + note), never a missed collision;
//   - subtree moves relocate index keys with the same prefix rule as
//     remapSubtree.
//
// Chunk-ID collisions need no index: meta.Chunks() is already an O(1) map
// lookup and stays live.
