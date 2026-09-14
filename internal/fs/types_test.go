package fs

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestKindLabelVocabulary pins the single display vocabulary shared by every
// renderer: directory/symlink/file, decided by the IsDir/IsSymlink flags.
func TestKindLabelVocabulary(t *testing.T) {
	cases := []struct {
		name string
		info EntryInfo
		dir  DirEntry
		want string
	}{
		{"dir", EntryInfo{IsDir: true}, DirEntry{IsDir: true}, "directory"},
		{"symlink", EntryInfo{IsSymlink: true}, DirEntry{IsSymlink: true}, "symlink"},
		{"file", EntryInfo{}, DirEntry{}, "file"},
		// Flags are the source of truth: a dir bit wins over a symlink bit,
		// matching the historical if/else renderer order.
		{"dir wins", EntryInfo{IsDir: true, IsSymlink: true}, DirEntry{IsDir: true, IsSymlink: true}, "directory"},
	}
	for _, tc := range cases {
		if got := tc.info.KindLabel(); got != tc.want {
			t.Errorf("%s: EntryInfo.KindLabel() = %q, want %q", tc.name, got, tc.want)
		}
		if got := tc.dir.KindLabel(); got != tc.want {
			t.Errorf("%s: DirEntry.KindLabel() = %q, want %q", tc.name, got, tc.want)
		}
	}
	if !(EntryInfo{IsDir: true}).IsDirectory() || (EntryInfo{}).IsDirectory() {
		t.Error("EntryInfo.IsDirectory must mirror IsDir")
	}
	if !(DirEntry{IsSymlink: true}).IsLink() || (DirEntry{}).IsLink() {
		t.Error("DirEntry.IsLink must mirror IsSymlink")
	}
}

// TestKindJSONWireFormatUnchanged pins the wire format: the unified helpers
// add methods only, so marshaled documents must carry exactly the old keys.
func TestKindJSONWireFormatUnchanged(t *testing.T) {
	blob, err := json.Marshal(EntryInfo{Path: "a", IsDir: true, Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	s := string(blob)
	for _, key := range []string{`"path"`, `"is_dir"`, `"size"`} {
		if !strings.Contains(s, key) {
			t.Errorf("wire key %s missing from %s", key, s)
		}
	}
	for _, key := range []string{`kind_label`, `KindLabel`, `"is_directory"`, `"directory"`} {
		if strings.Contains(s, key) {
			t.Errorf("helper leak into wire format %q in %s", key, s)
		}
	}
}
