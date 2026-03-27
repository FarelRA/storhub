package fs

import "testing"

func TestPathHelpers(t *testing.T) {
	if got, err := NormalizePath(" docs/guide.txt "); err != nil || got != "docs/guide.txt" {
		t.Fatalf("unexpected normalized path: %q %v", got, err)
	}
	if got, err := NormalizePath("."); err != nil || got != "" {
		t.Fatalf("unexpected root normalization: %q %v", got, err)
	}
	if _, err := NormalizePath(""); err == nil {
		t.Fatal("expected empty path error")
	}
	if _, err := NormalizePath("/tmp/file"); err == nil {
		t.Fatal("expected absolute path error")
	}
	if _, err := NormalizePath("../escape"); err == nil {
		t.Fatal("expected path escape error")
	}
	if got := normalizeStoredPath(" /tmp/file "); got != "tmp/file" {
		t.Fatalf("unexpected stored path fallback: %q", got)
	}
	if ParentPath("docs/guide.txt") != "docs" || ParentPath("guide.txt") != "" || ParentPath("") != "" {
		t.Fatal("unexpected parent path results")
	}
	if !IsParentOrSame("", "docs/guide.txt") || !IsParentOrSame("docs", "docs/guide.txt") || IsParentOrSame("docs", "images/file") {
		t.Fatal("unexpected parent relationship")
	}
	if got := RemapPath("docs", "archive", "docs/guide.txt"); got != "archive/guide.txt" {
		t.Fatalf("unexpected remapped path: %q", got)
	}
	if got := RemapPath("docs", "archive", "docs"); got != "archive" {
		t.Fatalf("unexpected same-path remap: %q", got)
	}
}
