package metadata

import (
	"encoding/json"
	"strings"
	"testing"
)

// v1 fixture in the verbose first-format shape (time strings, arrays).
const v1Fixture = `{
  "version": 1,
  "project": "demo",
  "next_inode": 9,
  "total_files": 1,
  "total_size": 3,
  "last_modified": "2024-01-01T00:00:00Z",
  "root": {
    "inode": 1, "mode": 493, "uid": 0, "gid": 0,
    "created_at": "2024-01-01T00:00:00Z",
    "modified_at": "2024-01-02T00:00:00Z"
  },
  "directories": [
    {"path": "docs", "created_at": "2024-01-01T00:00:00Z", "modified_at": "2024-01-01T00:00:00Z", "inode": 2}
  ],
  "releases": [
    {"tag": "v1", "asset_count": 1, "created_at": "2024-01-01T00:00:00Z", "files": [
      {"name": "docs/a.txt", "size": 3, "mode": 420, "uid": 1000, "gid": 1000, "inode": 5,
       "uploaded_at": "2024-01-01T00:00:00Z", "modified_at": "2024-01-01T00:00:00Z",
       "chunks": [{"size": 3, "offset": 0, "release": "v1", "asset_id": 77}],
       "xattrs": {"user.k": "raw"}}
    ]}
  ]
}`

func mustMigrateAll(t *testing.T, data []byte) []byte {
	t.Helper()
	out, version, err := Migrate(data)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if version != maxBlobVersion {
		t.Fatalf("version %d != current blob version %d", version, maxBlobVersion)
	}
	return out
}

func TestDetectVersionRejectsVersionless(t *testing.T) {
	t.Parallel()
	if _, err := detectVersion([]byte(`{"p":"x"}`)); err == nil {
		t.Fatal("versionless payload must be rejected")
	}
	if v, err := detectVersion([]byte(`{"version":1}`)); err != nil || v != 1 {
		t.Fatalf("v1 spelling must resolve: %d %v", v, err)
	}
	if v, err := detectVersion([]byte(`{"v":4}`)); err != nil || v != 4 {
		t.Fatalf("v spelling must resolve: %d %v", v, err)
	}
}

func TestMigrateRejectsNewerAndInvalid(t *testing.T) {
	t.Parallel()
	if _, _, err := Migrate([]byte(`{"v":99}`)); err == nil {
		t.Fatal("newer version must be refused")
	}
	if _, _, err := Migrate([]byte(`{"v":0}`)); err == nil {
		t.Fatal("invalid version must be refused")
	}
}

// Identity: a current document passes through byte-for-byte. ToJSON emits
// the blob layout (maxBlobVersion): v5 is manifest-only.
func TestMigrateIdentityOnCurrent(t *testing.T) {
	t.Parallel()
	m := NewRepoMetadata("demo")
	m.UpsertFile("f.txt", FileMeta{Size: 1}, 123)
	blob, err := m.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	out, version, err := Migrate(blob)
	if err != nil || version != maxBlobVersion {
		t.Fatalf("identity migrate failed: %d %v", version, err)
	}
	if string(out) != string(blob) {
		t.Fatal("current document must pass through unchanged")
	}
}

// Per-step goldens: each era decodes into its typed document and the next
// step emits exactly the documented transformations.
func TestStepV1ToV2(t *testing.T) {
	t.Parallel()
	out, version, err := Migrate([]byte(v1Fixture))
	if err != nil {
		t.Fatalf("chain: %v", err)
	}
	_ = version
	var mid docTopV2
	if err := json.Unmarshal(out, &mid); err != nil {
		t.Fatalf("chain output is not valid final JSON: %v", err)
	}
	// Step-level: run only the first migrator and inspect the v2 shape.
	v2, err := migrators[1]([]byte(v1Fixture))
	if err != nil {
		t.Fatal(err)
	}
	var doc docTopV2
	if err := json.Unmarshal(v2, &doc); err != nil {
		t.Fatalf("decode v2: %v", err)
	}
	if doc.V != 2 || doc.Project != "demo" {
		t.Fatalf("unexpected v2 header: %+v", doc)
	}
	f := doc.Files["docs/a.txt"]
	if f.Size != 3 || f.UID != 1000 || len(f.Chunks) != 1 {
		t.Fatalf("file not restructured: %+v", f)
	}
	if string(f.XAttrs["user.k"]) != "raw" {
		t.Fatalf("v1 raw xattr not carried as string: %+v", f.XAttrs)
	}
	if _, ok := doc.Dirs["docs"]; !ok {
		t.Fatal("directory missing from v2 map")
	}
}

func TestStepV2ToV3(t *testing.T) {
	t.Parallel()
	v2 := `{"v":2,"p":"demo","f":{"a.txt":{"s":1,"i":7,"ua":50}},"c":{"9":{"s":1,"r":"v1","a":3}}}`
	v3, err := migrators[2]([]byte(v2))
	if err != nil {
		t.Fatal(err)
	}
	var doc docTopV3
	if err := json.Unmarshal(v3, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.V != 3 {
		t.Fatalf("expected v3, got %d", doc.V)
	}
	f := doc.Files["a.txt"]
	// Zero means root and copies verbatim: the migrator must not invent
	// owners for entries that predate (or simply carry) explicit IDs.
	if f.UID != 0 || f.GID != 0 {
		t.Fatalf("owners must survive verbatim at v3, got %d:%d", f.UID, f.GID)
	}
	if doc.NextInode != 8 || doc.NextChunkID != 10 {
		t.Fatalf("counters must seed max+1: ni=%d nc=%d", doc.NextInode, doc.NextChunkID)
	}
	if _, hasX := f.XAttrs["none"]; hasX {
		t.Fatal("no xattrs expected")
	}
}

func TestStepV3ToV4(t *testing.T) {
	t.Parallel()
	v3 := `{"v":3,"p":"demo","lm":900,` +
		`"f":{"legacy.bin":{"s":2,"ua":0,"ma":0,"aa":0,"ca":0},"marked.bin":{"s":2,"tsx":true,"ua":0}},` +
		`"r":{"v1":{"ac":1,"ca":5}}}`
	v4, err := migrators[3]([]byte(v3))
	if err != nil {
		t.Fatal(err)
	}
	s := string(v4)
	if strings.Contains(s, `"ca"`) || strings.Contains(s, `"cha"`) {
		t.Fatal("legacy timestamp keys must not survive v4")
	}
	if strings.Contains(s, `"tsx"`) {
		t.Fatal("tsx must be consumed by v4")
	}
	var m RepoMetadata
	if err := json.Unmarshal(v4, &m); err != nil {
		t.Fatal(err)
	}
	legacy := m.files["legacy.bin"]
	// Deterministic completion: uploaded falls back to LastMod, the rest
	// chain from it.
	if legacy.UploadedAt != 900 || legacy.ModifiedAt != 900 || legacy.AccessedAt != 900 || legacy.ChangedAt != 900 {
		t.Fatalf("completion wrong: %+v", legacy)
	}
	marked := m.files["marked.bin"]
	if marked.UploadedAt != 0 {
		t.Fatalf("explicit zeros must survive verbatim: %+v", marked)
	}
	if m.releases["v1"].CreatedAt != 5 {
		t.Fatalf("release key rename lost data: %+v", m.Releases())
	}
}

// Full chain from the v1 fixture lands on a loadable current document.
func TestFullChainV1ToCurrent(t *testing.T) {
	t.Parallel()
	out := mustMigrateAll(t, []byte(v1Fixture))
	var m RepoMetadata
	if err := m.FromJSON(out); err != nil {
		t.Fatalf("load migrated document: %v", err)
	}
	f := m.FindFile("docs/a.txt")
	if f == nil {
		t.Fatal("file lost across chain")
	}
	if string(f.XAttrs["user.k"]) != "raw" {
		t.Fatalf("xattr bytes wrong across chain: %v", f.XAttrs)
	}
	if f.UploadedAt == 0 || m.Root.CreatedAt == 0 {
		t.Fatal("timestamps must be complete after migration")
	}
	if _, ok := m.dirs["docs"]; !ok {
		t.Fatal("directory lost across chain")
	}
}

// The v3->v4 dangling-reference repair must be real - strip the dead id
// (and the bytes it claimed) so the migrated document actually passes
// Validate instead of keeping a reference to a record the migration dropped.
func TestStepV3ToV4RepairsDanglingChunkRefs(t *testing.T) {
	t.Parallel()
	v3 := `{"v":3,"p":"demo","tf":2,"ts":14,"lm":900,` +
		`"rt":{"ca":100,"ma":101,"i":1},` +
		`"f":{"gone.bin":{"s":10,"cs":[7],"ua":300,"ma":301,"i":5},` +
		`"partial.bin":{"s":4,"cs":[1,9],"ua":300,"ma":301,"i":6}},` +
		`"c":{"1":{"s":2,"o":0,"r":"v1","a":3}},` +
		`"r":{"v1":{"ac":1,"ca":5}},"ni":7,"nc":2}`
	v4, err := migrators[3]([]byte(v3))
	if err != nil {
		t.Fatalf("migrate v3->v4: %v", err)
	}
	var m RepoMetadata
	if err := json.Unmarshal(v4, &m); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("repaired v4 document must pass Validate: %v", err)
	}
	gone := m.files["gone.bin"]
	if len(gone.Chunks) != 0 || gone.Size != 0 {
		t.Fatalf("fully dangling file not repaired: %+v", gone)
	}
	part := m.files["partial.bin"]
	if len(part.Chunks) != 1 || part.Chunks[0] != 1 || part.Size != 2 {
		t.Fatalf("partially dangling file not repaired: %+v", part)
	}
	if m.TotalSize != 2 {
		t.Fatalf("TotalSize %d must follow the repaired sizes", m.TotalSize)
	}
	// And the repaired document loads through the normal path.
	var back RepoMetadata
	if err := back.FromJSON(v4); err != nil {
		t.Fatalf("reloaded repaired document: %v", err)
	}
}

// Owner IDs in the v2->v3 migration copy verbatim: 0 means root, never
// unset. A v2 document carrying root-owned entries must migrate with
// UID/GID 0 intact no matter which user runs the daemon, and nonzero
// IDs must survive untouched.
func TestStepV2ToV3PreservesOwners(t *testing.T) {
	t.Parallel()
	v2 := `{"v":2,"p":"demo","rt":{"ca":1,"ma":1},"f":{"root.bin":{"s":1,"i":7,"ua":50},"owned.txt":{"s":1,"i":8,"ua":50,"u":1001,"g":1002}},"d":{"x":{"ca":1,"ma":1}}}`
	v3, err := migrators[2]([]byte(v2))
	if err != nil {
		t.Fatal(err)
	}
	var doc docTopV3
	if err := json.Unmarshal(v3, &doc); err != nil {
		t.Fatal(err)
	}
	if root := doc.Files["root.bin"]; root.UID != 0 || root.GID != 0 {
		t.Fatalf("root-owned file rewritten to %d:%d, want 0:0", root.UID, root.GID)
	}
	if owned := doc.Files["owned.txt"]; owned.UID != 1001 || owned.GID != 1002 {
		t.Fatalf("owned file rewritten to %d:%d, want 1001:1002", owned.UID, owned.GID)
	}
	if doc.Root.UID != 0 || doc.Root.GID != 0 {
		t.Fatalf("root dir rewritten to %d:%d, want 0:0", doc.Root.UID, doc.Root.GID)
	}
	if dir := doc.Dirs["x"]; dir.UID != 0 || dir.GID != 0 {
		t.Fatalf("dir rewritten to %d:%d, want 0:0", dir.UID, dir.GID)
	}
}

// Parser cleanliness: fed a RAW legacy payload with no migrator in the
// loop, the type itself refuses - no silent empty trees from ignored
// unknown fields. Through FromJSON the same payload migrates correctly.
func TestParserIsCurrentOnly(t *testing.T) {
	t.Parallel()
	rawV3 := `{"v":3,"rt":{"ca":1},"f":{"a":{"s":1,"ua":1,"ca":2}},"r":{"t":{"ac":1,"ca":3}}}`
	for name, payload := range map[string]string{"v1": v1Fixture, "v3": rawV3} {
		var m RepoMetadata
		err := json.Unmarshal([]byte(payload), &m)
		if err == nil {
			t.Fatalf("%s payload must be refused by the current parser", name)
		}
		if !strings.Contains(err.Error(), "version") {
			t.Fatalf("%s refusal must name the version contract: %v", name, err)
		}
	}

	var viaFromJSON RepoMetadata
	if err := viaFromJSON.FromJSON([]byte(rawV3)); err != nil {
		t.Fatalf("FromJSON must migrate: %v", err)
	}
	if viaFromJSON.files["a"].ChangedAt != 2 {
		t.Fatalf("migrated ChangedAt wrong: %+v", viaFromJSON.files["a"])
	}
}
