package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/FarelRA/storhub/internal/logging"
)

// CurrentVersion is the newest document version this build reads and writes
// (6, the split layout). The pure blob migrators below only ever produce
// maxBlobVersion (5); the 5->6 step is a write-time layout split, not a bytes
// transform.

// CurrentVersion is the newest metadata schema the code reads and writes.
const CurrentVersion = maxMetadataVersion

// versionProbe is the single version/shape envelope for every JSON document
// the package reads: v1 ("version"), v2-v5 ("v"), and v6 split manifests
// ("v" plus a non-empty "tr"). One struct replaces the three triplicated
// anonymous probes in detectVersion, UnmarshalJSON, and IsManifest.
type versionProbe struct {
	V        *int   `json:"v"`
	Version  *int   `json:"version"` // v1-era spelling, consumed here only
	TreeRoot string `json:"tr"`
}

// probeVersion decodes only the version/shape envelope of data.
func probeVersion(data []byte) (versionProbe, error) {
	var probe versionProbe
	if err := json.Unmarshal(data, &probe); err != nil {
		return probe, fmt.Errorf("metadata version probe failed: %w", err)
	}
	return probe, nil
}

// detectVersion reports a document's schema version. Historical spellings:
// v1 documents wrote "version"; v2 onward write "v". That history belongs to
// the migrator alone - the main parser never sees version detection. A
// split-index manifest is distinguished from a blob by IsManifest, not here.
func detectVersion(data []byte) (int, error) {
	probe, err := probeVersion(data)
	if err != nil {
		return 0, err
	}
	switch {
	case probe.V != nil:
		return *probe.V, nil
	case probe.Version != nil:
		return *probe.Version, nil
	default:
		return 0, errors.New("metadata payload has no version field; refusing to guess the schema")
	}
}

// migrators stacks one step per version boundary: migrators[n] upgrades a
// version-n document to version n+1. Every step is pure bytes->bytes:
// no clock, no I/O, deterministic output for a given input. There is no
// 5->6 step: that boundary is the write-time layout split (it emits objects),
// not a blob transform.
var migrators = [...]func([]byte) ([]byte, error){
	1: migrateV1ToV2,
	2: migrateV2ToV3,
	3: migrateV3ToV4,
	4: migrateV4ToV5,
}

// Migrate upgrades a serialized metadata BLOB to the current blob schema by
// applying every required step in order; a document already current (version
// maxBlobVersion) passes through unchanged. A split-index manifest is
// rejected: it loads through ParseManifest/LoadTree, never as a blob - and so
// is any version-6 document lacking a tree root, which is a truncated
// manifest, not a blob (v6 documents are manifests; blobs are v<=5). Loading
// is eager: every blob parse funnels through here, so no code path outside
// this file can observe an older schema shape. The upgraded document persists
// when the next mutation commits it (as a version-6 split).
func Migrate(data []byte) ([]byte, int, error) {
	if IsManifest(data) {
		return nil, maxMetadataVersion, fmt.Errorf("metadata version %d is the split-index manifest; load it via ParseManifest, not Migrate", maxMetadataVersion)
	}
	from, err := detectVersion(data)
	if err != nil {
		return nil, 0, err
	}
	if from > maxMetadataVersion {
		return nil, from, fmt.Errorf("metadata version %d is newer than supported version %d", from, maxMetadataVersion)
	}
	if from < 1 {
		return nil, from, fmt.Errorf("invalid metadata version %d", from)
	}
	if from == maxMetadataVersion {
		return nil, from, fmt.Errorf("metadata version %d with no tree root is a corrupt split-index manifest, not a blob; blobs are v%d or older", maxMetadataVersion, maxBlobVersion)
	}
	if from == maxBlobVersion {
		// Version 5 is the newest single-blob schema; the 5->6 step is the
		// write-time layout split, not a blob transform.
		return data, from, nil
	}
	started := time.Now()
	// Warn vocabulary (reason/from/to/step) is load-path convention: storage
	// verbs use step/message/project for commit work, while migration and
	// timestamp-fallback lines keep reason/from/to so manifest-era greps stay
	// stable. The split is deliberate, not drift.
	logging.Warn(metaLog(), "metadata migration", "reason", "upgrading metadata blob to the current schema", "from", from, "to", maxBlobVersion)
	for v := from; v < maxBlobVersion; v++ {
		step := migrators[v]
		if step == nil {
			return nil, v, fmt.Errorf("no migration path from metadata version %d", v)
		}
		if data, err = step(data); err != nil {
			logging.Error(metaLog(), "metadata migration failed", "from", from, "to", maxBlobVersion, "step", v, "err", err)
			return nil, v, fmt.Errorf("migrate metadata v%d->v%d: %w", v, v+1, err)
		}
	}
	logging.Debug(metaLog(), "metadata migration complete", "from", from, "to", maxBlobVersion, "elapsed", time.Since(started))
	return data, maxBlobVersion, nil
}

// checkedAdd returns a+b and whether it fit: chunk offsets near MaxInt64
// wrap negative with a naive sum and would corrupt size/overlap math.
func checkedAdd(a, b int64) (int64, bool) {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return 0, false
	}
	if b < 0 && a < int64(-1<<63)-b {
		return 0, false
	}
	return a + b, true
}

// incompleteTimes is the input to completeTimes: an entry's stored
// timestamps plus the document LastMod fallback and the authoritative-zero
// marker. One struct replaces the 5×int64 + bool flag soup whose call sites
// were unreadable positional argument lists.
type incompleteTimes struct {
	uploaded int64
	modified int64
	accessed int64
	changed  int64
	fallback int64
	explicit bool
}

// completeTimes fills zero-valued timestamps deterministically from the
// entry's own UploadedAt (then ModifiedAt), finally from the document's
// LastMod - the same chains the old load-time repair used, minus the wall
// clock. Entries flagged TimesExplicit are already authoritative and pass
// through untouched.
func completeTimes(in incompleteTimes) (uploaded, modified, accessed, changed int64) {
	uploaded, modified, accessed, changed = in.uploaded, in.modified, in.accessed, in.changed
	if in.explicit {
		return uploaded, modified, accessed, changed
	}
	if uploaded == 0 {
		uploaded = in.fallback
	}
	if modified == 0 {
		modified = uploaded
	}
	if accessed == 0 {
		accessed = modified
	}
	if changed == 0 {
		changed = modified
	}
	return uploaded, modified, accessed, changed
}

// modeOrDefault materializes a zero mode deterministically: directories get
// 0755, symlinks 0777, regular files 0644. It subsumes the old
// completeMode/fileModeOrDefault pair, whose only difference was which
// default the zero case picked.
func modeOrDefault(mode uint32, kind NodeKind, dir bool) uint32 {
	if mode != 0 {
		return mode
	}
	if dir {
		return defaultDirMode()
	}
	return defaultFileMode(kind)
}

// nanosPerSecond scales a seconds-era timestamp to nanoseconds.
const nanosPerSecond = 1_000_000_000

// secondsThreshold distinguishes the two eras on load: current epoch time is
// ~1.79e9 seconds vs ~1.79e18 nanoseconds, so any persisted time value below
// 1e12 is unambiguously seconds and is multiplied by 1e9.
const secondsThreshold = 1_000_000_000_000

// secsToNanos converts one seconds-era persisted timestamp to Unix
// nanoseconds. Values at or above secondsThreshold are already nanoseconds
// and pass through; zero (the authoritative epoch under the v4 contract)
// stays zero. MIGRATION-ONLY: live code assumes nanoseconds everywhere and
// never calls this.
func secsToNanos(v int64) int64 {
	if v > 0 && v < secondsThreshold {
		return v * nanosPerSecond
	}
	return v
}

func convertDirTimesToNano(d DirMeta) DirMeta {
	d.CreatedAt = secsToNanos(d.CreatedAt)
	d.ModifiedAt = secsToNanos(d.ModifiedAt)
	d.AccessedAt = secsToNanos(d.AccessedAt)
	d.ChangedAt = secsToNanos(d.ChangedAt)
	return d
}

func convertFileTimesToNano(f FileMeta) FileMeta {
	f.UploadedAt = secsToNanos(f.UploadedAt)
	f.ModifiedAt = secsToNanos(f.ModifiedAt)
	f.AccessedAt = secsToNanos(f.AccessedAt)
	f.ChangedAt = secsToNanos(f.ChangedAt)
	return f
}

// migrateTreeTimesToNano converts every sub-threshold timestamp of an
// in-memory tree to nanoseconds: root and stored dirs, stored files, release
// refs, and LastMod. Counters (NextInode/NextChunkID) are NOT times and are
// left untouched. Used by the v4->v5 blob migrator and by the split load
// path for pre-nanosecond manifests.
func migrateTreeTimesToNano(m *RepoMetadata) {
	m.LastMod = secsToNanos(m.LastMod)
	m.Root = convertDirTimesToNano(m.Root)
	for path, d := range m.dirs {
		m.dirs[path] = convertDirTimesToNano(d)
	}
	for path, f := range m.files {
		m.files[path] = convertFileTimesToNano(f)
	}
	for tag, r := range m.releases {
		r.CreatedAt = secsToNanos(r.CreatedAt)
		m.releases[tag] = r
	}
}
