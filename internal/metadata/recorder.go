package metadata

import "maps"

// The intent recorder is the transaction-scoped half of intent-based op
// synthesis. UpdateRepoMetadataContext attaches a recorder to the private
// COW candidate before fn runs; every tracked mutator then records the
// PRE-TRANSACTION state of the path it is about to change. After admission
// the storage layer folds the recorded intents into ops (O(changes))
// instead of diffing the whole pre-transaction tree (O(tree)).
//
// Recording discipline (load-bearing):
//   - FIRST touch wins: the first intent recorded for a key pins that key's
//     pre-transaction state; later intents for the same key are no-ops. The
//     fold classifies the NET effect (original vs final entry), so
//     intermediate states within the transaction are irrelevant.
//   - The recorder is transaction-scoped: Clone deliberately drops it (a
//     clone is a different tree), and the field is unexported so it can
//     never reach the JSON shadow or the wire format.
//   - A nil recorder makes every hook a no-op: applyOps replay, apply-back,
//     load/migrate construction, and rollback validation all run unrecorded
//     at zero cost.
//   - Derived-state repair (Normalize, RecomputeStats, the chunk re-sort)
//     writes the stored maps directly instead of going through the tracked
//     mutators, so it is structurally invisible to the recorder - it is not
//     intent.
type IntentRecorder struct {
	files    map[string]FileIntent
	dirs     map[string]DirIntent
	chunks   map[int64]ChunkIntent
	releases map[string]ReleaseIntent
}

// FileIntent pins the pre-transaction state of one file path.
type FileIntent struct {
	Existed bool
	Old     FileMeta
}

// DirIntent pins the pre-transaction state of one directory path.
type DirIntent struct {
	Existed bool
	Old     DirMeta
}

// ChunkIntent pins whether a chunk id existed before the transaction. The
// fold only needs existence: chunk records ride along in put ops read from
// the final tree, and the prune op needs to know which ids the transaction
// removed from the catalog.
type ChunkIntent struct {
	Existed bool
}

// ReleaseIntent pins the pre-transaction state of one release tag.
type ReleaseIntent struct {
	Existed bool
	Old     ReleaseRef
}

// NewIntentRecorder returns an empty recorder.
func NewIntentRecorder() *IntentRecorder {
	return &IntentRecorder{
		files:    make(map[string]FileIntent),
		dirs:     make(map[string]DirIntent),
		chunks:   make(map[int64]ChunkIntent),
		releases: make(map[string]ReleaseIntent),
	}
}

// AttachIntentRecorder binds r to this tree so its tracked mutators record
// intents. One recorder belongs to exactly one transaction on one tree.
func (m *RepoMetadata) AttachIntentRecorder(r *IntentRecorder) { m.recorder = r }

// DetachIntentRecorder removes the binding. The transaction calls it after
// admission, before the candidate is published, so the shared tree never
// carries a live recorder.
func (m *RepoMetadata) DetachIntentRecorder() { m.recorder = nil }

// FileIntents returns a copy of the recorded file intents. The fold must
// not mutate the recorder's maps, so callers get a clone, not the live map.
func (r *IntentRecorder) FileIntents() map[string]FileIntent { return maps.Clone(r.files) }

// DirIntents returns a copy of the recorded directory intents. See
// FileIntents.
func (r *IntentRecorder) DirIntents() map[string]DirIntent { return maps.Clone(r.dirs) }

// ChunkIntents returns a copy of the recorded chunk intents. See
// FileIntents.
func (r *IntentRecorder) ChunkIntents() map[int64]ChunkIntent { return maps.Clone(r.chunks) }

// ReleaseIntents returns a copy of the recorded release intents. See
// FileIntents.
func (r *IntentRecorder) ReleaseIntents() map[string]ReleaseIntent { return maps.Clone(r.releases) }

// --- recording hooks (called by the tracked mutators) ---------------------
//
// Each hook is first-touch-wins and clones the old value, so the pinned
// pre-transaction state can never alias an entry the transaction replaces
// later.

// recordFirst inserts val under key unless the key already has a pinned
// intent: the first touch of a transaction wins, later touches are no-ops.
func recordFirst[K comparable, V any](intents map[K]V, key K, val V) {
	if _, ok := intents[key]; !ok {
		intents[key] = val
	}
}

func (m *RepoMetadata) recordFilePut(name string, old FileMeta, existed bool) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.files, name, FileIntent{Existed: existed, Old: old.Clone()})
}

func (m *RepoMetadata) recordFileRemove(name string, old FileMeta) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.files, name, FileIntent{Existed: true, Old: old.Clone()})
}

func (m *RepoMetadata) recordDirPut(path string, old DirMeta, existed bool) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.dirs, path, DirIntent{Existed: existed, Old: old.Clone()})
}

func (m *RepoMetadata) recordDirRemove(path string, old DirMeta) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.dirs, path, DirIntent{Existed: true, Old: old.Clone()})
}

func (m *RepoMetadata) recordChunkPut(id int64, existed bool) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.chunks, id, ChunkIntent{Existed: existed})
}

func (m *RepoMetadata) recordChunkDelete(id int64) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.chunks, id, ChunkIntent{Existed: true})
}

func (m *RepoMetadata) recordReleasePut(tag string, old ReleaseRef, existed bool) {
	if m.recorder == nil {
		return
	}
	// ReleaseRef is a pure value type: struct assignment pins the state.
	recordFirst(m.recorder.releases, tag, ReleaseIntent{Existed: existed, Old: old})
}

func (m *RepoMetadata) recordReleaseRemove(tag string, old ReleaseRef) {
	if m.recorder == nil {
		return
	}
	recordFirst(m.recorder.releases, tag, ReleaseIntent{Existed: true, Old: old})
}
