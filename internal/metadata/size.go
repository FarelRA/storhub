package metadata

import (
	"encoding/json"
	"fmt"
	"reflect"
)

// sectionSize tracks the serialized size of one stored map: sum is
// Σ(len(quoted key)+1+len(value)) over its entries; the map's own braces and
// inter-entry commas are added by the caller. ok=false means the section is
// stale and must be recomputed before use.
type sectionSize struct {
	ok  bool
	ptr uintptr
	n   int
	sum int64
}

// entryTransition describes one map-key transition (old, present iff hadOld)
// -> (cur, present iff hasCur) for the incremental size/index/stat/record
// chain. A single struct replaces the (old, hadOld, cur, hasCur) boolean-flag
// soup that used to spread across ~10 helpers: call sites build one with
// putTransition (insert or replace) or removeTransition (delete) and the
// whole chain consumes it without re-deriving presence.
type entryTransition[V any] struct {
	old    V
	hadOld bool
	cur    V
	hasCur bool
}

// putTransition builds the transition for m[key] = cur, where old/hadOld
// describe the previous entry (hadOld=false for an insert).
func putTransition[V any](old V, hadOld bool, cur V) entryTransition[V] {
	return entryTransition[V]{old: old, hadOld: hadOld, cur: cur, hasCur: true}
}

// removeTransition builds the transition for delete(m, key), where old is
// the removed entry.
func removeTransition[V any](old V) entryTransition[V] {
	return entryTransition[V]{old: old, hadOld: true}
}

// Size-cache section indices, ordered as the JSON struct fields.
const (
	secDirs = iota
	secFiles
	secChunks
	secReleases
)

// mapPtr returns the runtime header pointer of a map for identity
// fingerprinting. The caller must pin the recorded map (the size section and
// the index fingerprint both hold a reference), so an equal pointer cannot
// alias a reallocated (GC'd) map. This is the single reflect-based map
// identity helper in the package.
func mapPtr[K comparable, V any](m map[K]V) uintptr {
	return reflect.ValueOf(m).Pointer()
}

// --- incremental serialized-size accounting -----------------------------

// SerializedSize returns the exact byte length of ToJSON's output,
// maintained incrementally: per-entry deltas are applied by the tracked
// mutators, and only the constant-size document skeleton (scalars + root)
// is marshalled per call. A section whose map changed outside the tracked
// paths (fingerprint mismatch) is recomputed once, on demand. Callers
// performing size admission can use this instead of marshalling the whole
// tree.
func (m *RepoMetadata) SerializedSize() (int, error) {
	base, err := m.sizeSkeletonLen()
	if err != nil {
		return 0, err
	}
	d := m.ensureDerived()
	total := int64(base)
	for _, s := range []struct {
		i     int
		extra func(*sectionSize) (int64, error)
	}{
		{secDirs, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.dirs) }},
		{secFiles, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.files) }},
		{secChunks, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.chunks) }},
		{secReleases, func(s *sectionSize) (int64, error) { return sectionExtra(s, m.releases) }},
	} {
		extra, err := s.extra(&d.sections[s.i])
		if err != nil {
			return 0, err
		}
		total += extra
	}
	return int(total), nil
}

// sizeSkeletonLen marshals the document with the four stored maps emptied
// (preserving nil vs non-nil) and returns its length: the constant overhead
// every SerializedSize answer is built on. It mirrors ToJSON's version trim
// exactly.
func (m *RepoMetadata) sizeSkeletonLen() (int, error) {
	sh := m.toShadow()
	if sh.Version > maxBlobVersion {
		sh.Version = maxBlobVersion
	}
	if sh.Dirs != nil {
		sh.Dirs = map[string]DirMeta{}
	}
	if sh.Files != nil {
		sh.Files = map[string]FileMeta{}
	}
	if sh.Chunks != nil {
		sh.Chunks = map[int64]ChunkInfo{}
	}
	if sh.Releases != nil {
		sh.Releases = map[string]ReleaseRef{}
	}
	data, err := json.Marshal(&sh)
	if err != nil {
		return 0, fmt.Errorf("marshal metadata skeleton: %w", err)
	}
	return len(data), nil
}

// sectionExtra returns the bytes the map contributes beyond its empty
// serialization ("{}" or "null"), recomputing the section when its
// fingerprint no longer matches the live map.
func sectionExtra[K comparable, V any](s *sectionSize, mp map[K]V) (int64, error) {
	if mp == nil {
		return 0, nil
	}
	ptr, n := mapPtr(mp), len(mp)
	if !s.ok || s.ptr != ptr || s.n != n {
		sum := int64(0)
		for k, v := range mp {
			c, err := entryBytes(k, v)
			if err != nil {
				return 0, err
			}
			sum += int64(c)
		}
		s.ok, s.ptr, s.n, s.sum = true, ptr, n, sum
	}
	extra := s.sum
	if n > 1 {
		extra += int64(n - 1)
	}
	return extra, nil
}

// entryBytes is the exact byte cost of one map entry inside its parent
// object: quoted key + colon + value (the inter-entry comma is accounted
// per-section, not per-entry).
func entryBytes[K comparable, V any](key K, val V) (int, error) {
	kb, err := json.Marshal(key)
	if err != nil {
		return 0, err
	}
	klen := len(kb)
	if _, isString := any(key).(string); !isString {
		// Numeric map keys are quoted in JSON objects.
		klen += 2
	}
	vb, err := json.Marshal(val)
	if err != nil {
		return 0, err
	}
	return klen + 1 + len(vb), nil
}

// sizeApplySection adjusts one section for a single key transition t on the
// map mp, which must already reflect the change. A section that no longer
// matches the map (external swap, or a length the delta cannot explain) is
// marked stale for on-demand recompute instead of being silently wrong.
func sizeApplySection[K comparable, V any](s *sectionSize, mp map[K]V, key K, t entryTransition[V]) {
	ptr, n := mapPtr(mp), len(mp)
	oldN := n
	if t.hasCur {
		oldN--
	}
	if t.hadOld {
		oldN++
	}
	if !s.ok || s.ptr != ptr || s.n != oldN {
		s.ok = false
		return
	}
	if t.hadOld {
		c, err := entryBytes(key, t.old)
		if err != nil {
			s.ok = false
			return
		}
		s.sum -= int64(c)
	}
	if t.hasCur {
		c, err := entryBytes(key, t.cur)
		if err != nil {
			s.ok = false
			return
		}
		s.sum += int64(c)
	}
	s.n = n
}

func (m *RepoMetadata) sizePutDir(path string, t entryTransition[DirMeta]) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.dirs, path, t)
}

func (m *RepoMetadata) sizeRemoveDir(path string, old DirMeta) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.dirs, path, removeTransition(old))
}

func (m *RepoMetadata) sizePutFile(name string, t entryTransition[FileMeta]) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.files, name, t)
}

func (m *RepoMetadata) sizeRemoveFile(name string, old FileMeta) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.files, name, removeTransition(old))
}

func (m *RepoMetadata) sizePutRelease(tag string, t entryTransition[ReleaseRef]) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, t)
}

func (m *RepoMetadata) sizeRemoveRelease(tag string, old ReleaseRef) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, removeTransition(old))
}
