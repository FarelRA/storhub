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
	ref any
	ptr uintptr
	n   int
	sum int64
}

// Size-cache section indices, ordered as the JSON struct fields.
const (
	secDirs = iota
	secFiles
	secChunks
	secReleases
)

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
		s.ok, s.ref, s.ptr, s.n, s.sum = true, mp, ptr, n, sum
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

// sizeApplySection adjusts one section for a single key transition
// (old, present iff hadOld) -> (cur, present iff hasCur) on the map mp,
// which must already reflect the change. A section that no longer matches
// the map (external swap, or a length the delta cannot explain) is marked
// stale for on-demand recompute instead of being silently wrong.
func sizeApplySection[K comparable, V any](s *sectionSize, mp map[K]V, key K, old V, hadOld bool, cur V, hasCur bool) {
	ptr, n := mapPtr(mp), len(mp)
	oldN := n
	if hasCur {
		oldN--
	}
	if hadOld {
		oldN++
	}
	if !s.ok || s.ptr != ptr || s.n != oldN {
		s.ok = false
		return
	}
	if hadOld {
		c, err := entryBytes(key, old)
		if err != nil {
			s.ok = false
			return
		}
		s.sum -= int64(c)
	}
	if hasCur {
		c, err := entryBytes(key, cur)
		if err != nil {
			s.ok = false
			return
		}
		s.sum += int64(c)
	}
	s.n = n
}

func (m *RepoMetadata) sizePutDir(path string, old DirMeta, hadOld bool, cur DirMeta) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.dirs, path, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveDir(path string, old DirMeta) {
	sizeApplySection(&m.ensureDerived().sections[secDirs], m.dirs, path, old, true, DirMeta{}, false)
}

func (m *RepoMetadata) sizePutFile(name string, old FileMeta, hadOld bool, cur FileMeta) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.files, name, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveFile(name string, old FileMeta) {
	sizeApplySection(&m.ensureDerived().sections[secFiles], m.files, name, old, true, FileMeta{}, false)
}

func (m *RepoMetadata) sizePutRelease(tag string, old ReleaseRef, hadOld bool, cur ReleaseRef) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, old, hadOld, cur, true)
}

func (m *RepoMetadata) sizeRemoveRelease(tag string, old ReleaseRef) {
	sizeApplySection(&m.ensureDerived().sections[secReleases], m.releases, tag, old, true, ReleaseRef{}, false)
}
