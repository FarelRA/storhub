package fs

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

// CopyContext must check read access on the source, not just
// traversal, or unreadable 0600 files can be duplicated by strangers.
func TestCopyRequiresSourceReadAccess(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(740)
	backend.seedDir("mine")
	file := backend.seedFile("mine/secret.txt", []byte("hush"))
	file.Mode = 0o600
	file.UID = 11
	file.GID = 22
	backend.repo.UpsertFile("mine/secret.txt", *file, backend.now)
	dir := backend.repo.GetDirectory("mine")
	dir.Mode = 0o755
	backend.repo.Dirs()["mine"] = *dir
	backend.seedDir("theirs")
	theirs := backend.repo.GetDirectory("theirs")
	theirs.Mode = 0o777
	backend.repo.Dirs()["theirs"] = *theirs
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	attacker := WithIdentity(context.Background(), Identity{UID: 30, GID: 40, Groups: []uint32{40}})
	if err := svc.CopyContext(attacker, "demo", "mine/secret.txt", "theirs/copy.txt"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("expected EACCES copying unreadable source, got %v", err)
	}
	if backend.repo.FindFile("theirs/copy.txt") != nil {
		t.Fatal("denied copy must not create the destination")
	}
	owner := WithIdentity(context.Background(), Identity{UID: 11, GID: 22, Groups: []uint32{22}})
	if err := svc.CopyContext(owner, "demo", "mine/secret.txt", "theirs/copy.txt"); err != nil {
		t.Fatalf("owner copy: %v", err)
	}
}

// CopyContext must mint caller ownership and clear setuid/setgid for
// unprivileged callers (non-admin data writes clear setuid+setgid, see
// SanitizeWrittenFileModeForContext), matching CloneRange new
// destinations and data writes: a copy must never mint a root-owned
// setuid node for a non-root caller.
func TestCopySanitizesOwnerAndPrivilegeBits(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(800)
	backend.seedDir("pub")
	src := backend.seedFile("pub/tool", []byte("x"))
	src.Mode = 0o4755
	src.UID = 0
	src.GID = 0
	// WriteFileDirect, not UpsertFile: UpsertFile on an existing path
	// treats a zero UID as unset and restores the old owner, so only a
	// verbatim store can seed a true root-owned source.
	backend.repo.WriteFileDirect("pub/tool", *src)
	backend.seedDir("mine")
	mine := backend.repo.GetDirectory("mine")
	mineCopy := *mine
	mineCopy.Mode = 0o777
	backend.repo.Dirs()["mine"] = mineCopy
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	caller := WithIdentity(context.Background(), Identity{UID: 1001, GID: 1002, Groups: []uint32{1002}})
	if err := svc.CopyContext(caller, "demo", "pub/tool", "mine/tool"); err != nil {
		t.Fatalf("copy: %v", err)
	}
	got := backend.repo.FindFile("mine/tool")
	if got == nil {
		t.Fatal("copy destination missing")
	}
	if got.UID != 1001 || got.GID != 1002 {
		t.Fatalf("copy owner = %d:%d, want caller 1001:1002", got.UID, got.GID)
	}
	if got.Mode&0o6000 != 0 {
		t.Fatalf("copy mode = %o, privilege bits must clear for non-admin", got.Mode)
	}
	if got.Mode&0o777 != 0o755 {
		t.Fatalf("copy mode = %o, want permission bits 755", got.Mode)
	}
	// Admin keeps the source owner and bits (CAP_FSETID equivalent).
	admin := WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
	if err := svc.CopyContext(admin, "demo", "pub/tool", "mine/admintool"); err != nil {
		t.Fatalf("admin copy: %v", err)
	}
	agot := backend.repo.FindFile("mine/admintool")
	if agot == nil {
		t.Fatal("admin copy destination missing")
	}
	if agot.UID != 0 || agot.GID != 0 || agot.Mode != 0o4755 {
		t.Fatalf("admin copy = %d:%d %o, want 0:0 4755", agot.UID, agot.GID, agot.Mode)
	}
}

// Directory copies must sanitize every entry in the subtree, not just
// the root: each new node is caller-owned with privilege bits cleared
// for unprivileged callers.
func TestCopyDirSanitizesOwnerAndPrivilegeBits(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(810)
	backend.seedDir("pub")
	pub := backend.repo.GetDirectory("pub")
	pubCopy := *pub
	pubCopy.Mode = 0o2755
	pubCopy.UID = 0
	pubCopy.GID = 0
	backend.repo.Dirs()["pub"] = pubCopy
	src := backend.seedFile("pub/tool", []byte("x"))
	src.Mode = 0o4755
	src.UID = 0
	src.GID = 0
	backend.repo.WriteFileDirect("pub/tool", *src)
	backend.seedDir("mine")
	mine := backend.repo.GetDirectory("mine")
	mineCopy := *mine
	mineCopy.Mode = 0o777
	backend.repo.Dirs()["mine"] = mineCopy
	backend.repo.RebuildIndexes()
	svc := NewService(backend)
	caller := WithIdentity(context.Background(), Identity{UID: 1001, GID: 1002, Groups: []uint32{1002}})
	if err := svc.CopyContext(caller, "demo", "pub", "mine/pubcopy"); err != nil {
		t.Fatalf("dir copy: %v", err)
	}
	dir := backend.repo.GetDirectory("mine/pubcopy")
	if dir == nil {
		t.Fatal("copied dir missing")
	}
	if dir.UID != 1001 || dir.GID != 1002 {
		t.Fatalf("copied dir owner = %d:%d, want caller 1001:1002", dir.UID, dir.GID)
	}
	if dir.Mode&0o6000 != 0 {
		t.Fatalf("copied dir mode = %o, privilege bits must clear for non-admin", dir.Mode)
	}
	got := backend.repo.FindFile("mine/pubcopy/tool")
	if got == nil {
		t.Fatal("copied file missing")
	}
	if got.UID != 1001 || got.GID != 1002 {
		t.Fatalf("copied file owner = %d:%d, want caller 1001:1002", got.UID, got.GID)
	}
	if got.Mode&0o6000 != 0 {
		t.Fatalf("copied file mode = %o, privilege bits must clear for non-admin", got.Mode)
	}
}

// The file owner keeps the POSIX chgrp right (into a group they
// belong to) but may not hand the file away.
func TestCanChownOwnerRights(t *testing.T) {
	t.Parallel()
	entry := &EntryInfo{Path: "f", UID: 1000, GID: 100, Mode: 0o644}
	owner := WithIdentity(context.Background(), Identity{UID: 1000, GID: 100, Groups: []uint32{100, 200}})
	if err := CanChown(owner, entry, entry.UID, 200); err != nil {
		t.Fatalf("owner chgrp into member group: %v", err)
	}
	if err := CanChown(owner, entry, entry.UID, 999); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("owner chgrp into foreign group must be EPERM, got %v", err)
	}
	if err := CanChown(owner, entry, 1234, entry.GID); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("owner give-away must be EPERM, got %v", err)
	}
	stranger := WithIdentity(context.Background(), Identity{UID: 7, GID: 7, Groups: []uint32{7}})
	if err := CanChown(stranger, entry, entry.UID, 100); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("non-owner chown must be EPERM, got %v", err)
	}
	root := WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
	if err := CanChown(root, entry, 1234, 5678); err != nil {
		t.Fatalf("admin chown: %v", err)
	}
}
