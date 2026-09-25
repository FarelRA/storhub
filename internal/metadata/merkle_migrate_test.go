package metadata

import (
	"reflect"
	"strings"
	"testing"
)

// F6 pins: classifier table, depth helper, build group helper, TreeResult
// embed, v4/v5 time-field parity.

func TestClassifyDocument(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		kind    docKind
		version int
		wantErr bool
	}{
		{"blob v5", `{"v":5,"p":"d"}`, kindBlob, 5, false},
		{"blob v4", `{"v":4,"p":"d"}`, kindBlob, 4, false},
		{"blob v1 spelling", `{"version":1,"project":"d"}`, kindBlob, 1, false},
		{"manifest v6", `{"v":6,"p":"d","tr":"` + strings.Repeat("a", 64) + `"}`, kindManifest, 6, false},
		{"manifest v5 seconds era", `{"v":5,"p":"d","tr":"` + strings.Repeat("b", 64) + `"}`, kindManifest, 5, false},
		{"truncated v6 no root", `{"v":6,"p":"d"}`, kindBlob, 6, false},
		{"future v7 with root is not manifest", `{"v":7,"p":"d","tr":"x"}`, kindBlob, 7, false},
		{"unversioned", `{"p":"d"}`, kindUnknown, 0, false},
		{"bad json", `{"v":`, kindUnknown, 0, true},
	}
	for _, c := range cases {
		kind, ver, err := classifyDocument([]byte(c.doc))
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: err = %v, wantErr %v", c.name, err, c.wantErr)
		}
		if err != nil {
			continue
		}
		if kind != c.kind || ver != c.version {
			t.Fatalf("%s: got (%v,%d), want (%v,%d)", c.name, kind, ver, c.kind, c.version)
		}
	}
}

func TestDepthExceeded(t *testing.T) {
	if depthExceeded(maxLoadTreeDepth) {
		t.Fatalf("depth at bound must pass")
	}
	if !depthExceeded(maxLoadTreeDepth + 1) {
		t.Fatalf("depth past bound must fail")
	}
}

func TestGroupFileNamesByParent(t *testing.T) {
	m := newBareRepoMetadata()
	m.files["docs/a"] = FileMeta{Size: 1}
	m.files["docs/b"] = FileMeta{Size: 2}
	m.files["top"] = FileMeta{Size: 3}
	got := groupFileNamesByParent(m)
	if len(got["docs"]) != 2 || len(got[""]) != 1 {
		t.Fatalf("bad groups: %v", got)
	}
	if _, ok := got["docs"]["a"]; !ok {
		t.Fatalf("keys must be base names: %v", got["docs"])
	}
}

func TestTreeResultCarriesStreamRefs(t *testing.T) {
	m := sampleTree(t)
	res, err := BuildTree(m)
	if err != nil {
		t.Fatal(err)
	}
	if res.RootSHA == "" || res.ReleasesSHA == "" {
		t.Fatalf("embedded refs must be set: %+v", res.TreeRefs)
	}
	refs, err := BuildTreeStream(m, nil, nil, func(string, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(res.TreeRefs, *refs) {
		t.Fatalf("BuildTree refs %+v != stream refs %+v", res.TreeRefs, refs)
	}
}

func TestNanoConvertersCoverEveryTimeField(t *testing.T) {
	sec := int64(1700000000)
	d := convertDirTimesToNano(DirMeta{CreatedAt: sec, ModifiedAt: sec, AccessedAt: sec, ChangedAt: sec})
	if d.CreatedAt != secsToNanos(sec) || d.ModifiedAt != secsToNanos(sec) ||
		d.AccessedAt != secsToNanos(sec) || d.ChangedAt != secsToNanos(sec) {
		t.Fatalf("dir converter misses a field: %+v", d)
	}
	f := convertFileTimesToNano(FileMeta{UploadedAt: sec, ModifiedAt: sec, AccessedAt: sec, ChangedAt: sec})
	if f.UploadedAt != secsToNanos(sec) || f.ModifiedAt != secsToNanos(sec) ||
		f.AccessedAt != secsToNanos(sec) || f.ChangedAt != secsToNanos(sec) {
		t.Fatalf("file converter misses a field: %+v", f)
	}
	m := newBareRepoMetadata()
	m.LastMod = sec
	m.Root = DirMeta{CreatedAt: sec, ModifiedAt: sec, AccessedAt: sec, ChangedAt: sec}
	m.dirs["docs"] = DirMeta{CreatedAt: sec, ModifiedAt: sec, AccessedAt: sec, ChangedAt: sec}
	m.files["docs/a"] = FileMeta{UploadedAt: sec, ModifiedAt: sec, AccessedAt: sec, ChangedAt: sec}
	m.releases["v1"] = ReleaseRef{CreatedAt: sec}
	m.NextInode, m.NextChunkID = 9, 9
	migrateTreeTimesToNano(m)
	if m.LastMod != secsToNanos(sec) || m.NextInode != 9 || m.NextChunkID != 9 {
		t.Fatalf("tree converter must scale LastMod and spare counters: %+v", m)
	}
	if m.Root.CreatedAt != secsToNanos(sec) || m.dirs["docs"].ChangedAt != secsToNanos(sec) ||
		m.files["docs/a"].UploadedAt != secsToNanos(sec) || m.releases["v1"].CreatedAt != secsToNanos(sec) {
		t.Fatalf("tree converter misses an entry class")
	}
}

func TestV3ToV4LinkConverges(t *testing.T) {
	link := fileV3ToV4(docFileV3{docFileV2: docFileV2{
		Size: 999, Chunks: []int64{7}, Symlink: "tgt",
	}}, 0)
	if link.Size != 3 || link.Chunks == nil || len(link.Chunks) != 0 {
		t.Fatalf("link residue must converge: %+v", link)
	}
	plain := fileV3ToV4(docFileV3{docFileV2: docFileV2{Size: 5}}, 0)
	if plain.Chunks == nil {
		t.Fatalf("nil chunks must materialize empty: %+v", plain)
	}
}

func TestIsManifestMatchesClassifier(t *testing.T) {
	manifest := `{"v":6,"p":"d","tr":"` + strings.Repeat("c", 64) + `"}`
	if !IsManifest([]byte(manifest)) {
		t.Fatalf("v6 with root must be manifest")
	}
	if IsManifest([]byte(`{"v":6,"p":"d"}`)) {
		t.Fatalf("v6 without root must not be manifest")
	}
	if IsManifest([]byte(`{"v":`)) {
		t.Fatalf("bad json must not be manifest")
	}
}
