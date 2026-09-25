package test

import (
	"errors"
	"testing"
)

// TestOpenPathNeverCreates pins the creation guard: a permission-free open
// of a missing path reports ErrNotFound without creating anything, even
// when the caller passes create-if-missing.
func TestOpenPathNeverCreates(t *testing.T) {
	m := NewMemSurface()
	if _, err := m.Open("/pc-path-missing", OpenPath, CreateIfMissing); !errors.Is(err, ErrNotFound) {
		t.Fatalf("open-path of missing path: want ErrNotFound, got %v", err)
	}
	if _, err := m.Stat("/pc-path-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("guard created the path: stat got %v, want ErrNotFound", err)
	}
}

// TestOpenPathCarriesNoIORights pins the handle side of the permission-free
// open: opening an existing path succeeds, but reads and writes fail.
func TestOpenPathCarriesNoIORights(t *testing.T) {
	m := NewMemSurface()
	if err := m.CreateFile("/pc-path-file", 0o644, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	h, err := m.Open("/pc-path-file", OpenPath, CreateNever)
	if err != nil {
		t.Fatalf("open-path of existing file: %v", err)
	}
	defer func() { _ = h.Close() }()
	if _, err := h.Read(1); !errors.Is(err, ErrAccess) {
		t.Fatalf("open-path read: want ErrAccess, got %v", err)
	}
	if _, err := h.Write([]byte("x")); !errors.Is(err, ErrAccess) {
		t.Fatalf("open-path write: want ErrAccess, got %v", err)
	}
}
