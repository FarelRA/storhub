package posix

import (
	"context"
	"testing"
	"time"

	shfs "github.com/FarelRA/storhub/internal/fs"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Legacy ChtimesContext must behave exactly like the Explicit trinary form:
// zero maps to now, nonzero passes through verbatim.
func TestLegacyChtimesMatchesExplicitContract(t *testing.T) {
	t.Parallel()
	now := int64(9000)
	backend := newTestBackend(now)
	backend.seedDir("docs")
	backend.seedFile("docs/a.txt")
	backend.seedFile("docs/b.txt")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})

	if err := svc.ChtimesContext(ctx, "demo", "docs/a.txt", 0, 0); err != nil {
		t.Fatalf("legacy zero: %v", err)
	}
	atime := time.Unix(0, 111)
	mtime := time.Unix(0, 222)
	if err := svc.ChtimesExplicitContext(ctx, "demo", "docs/b.txt", &atime, &mtime); err != nil {
		t.Fatalf("explicit: %v", err)
	}
	legacy, err := svc.lookupEntryForAccess(ctx, "demo", "docs/a.txt")
	if err != nil {
		t.Fatalf("lookup legacy: %v", err)
	}
	if legacy.AccessedAt != now || legacy.ModifiedAt != now {
		t.Fatalf("legacy zero must map to now %d, got atime=%d mtime=%d", now, legacy.AccessedAt, legacy.ModifiedAt)
	}
	explicit, err := svc.lookupEntryForAccess(ctx, "demo", "docs/b.txt")
	if err != nil {
		t.Fatalf("lookup explicit: %v", err)
	}
	if explicit.AccessedAt != 111 || explicit.ModifiedAt != 222 {
		t.Fatalf("explicit passthrough broken, got %+v", explicit)
	}
	if err := svc.ChtimesContext(ctx, "demo", "docs/a.txt", 111, 222); err != nil {
		t.Fatalf("legacy nonzero: %v", err)
	}
	legacy2, _ := svc.lookupEntryForAccess(ctx, "demo", "docs/a.txt")
	if legacy2.AccessedAt != 111 || legacy2.ModifiedAt != 222 {
		t.Fatalf("legacy nonzero must match explicit, got %+v", legacy2)
	}
}

// Explicit timestamps outside the int64-nanos window must saturate,
// never wrap to a negative value.
func TestExplicitChtimesSaturatesFarFuture(t *testing.T) {
	t.Parallel()
	now := int64(9100)
	backend := newTestBackend(now)
	backend.seedFile("far.txt")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})

	far := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := svc.ChtimesExplicitContext(ctx, "demo", "far.txt", &far, &far); err != nil {
		t.Fatalf("explicit far future: %v", err)
	}
	entry, err := svc.lookupEntryForAccess(ctx, "demo", "far.txt")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if entry.AccessedAt != maxInt64 || entry.ModifiedAt != maxInt64 {
		t.Fatalf("far-future stamps must saturate to maxInt64, got %d/%d", entry.AccessedAt, entry.ModifiedAt)
	}
}

// ReplaceInodeFamily must preserve every field verbatim (WriteFileFamily
// semantics): a sibling's authoritative epoch zero survives instead of
// being rewritten by creation defaults or the updated entry's stamps.
func TestReplaceInodeFamilyPreservesEpochZero(t *testing.T) {
	t.Parallel()
	now := int64(9200)
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureDirectory("docs", now)
	base := meta.FileMeta{Size: 1, Chunks: []int64{}, AccessedAt: now, ModifiedAt: now, UploadedAt: now, ChangedAt: now}
	repo.UpsertFile("docs/a.txt", base, now)
	first := repo.FindFile("docs/a.txt")
	clone := first.Clone()
	repo.UpsertFile("docs/b.txt", clone, now)
	repo.RebuildIndexes()
	// Plant an authoritative epoch atime on one sibling.
	planted := repo.FindFile("docs/b.txt")
	planted.AccessedAt = 0
	repo.WriteFileDirect("docs/b.txt", *planted)

	updated := first.Clone()
	updated.AccessedAt = 777
	updated.ModifiedAt = 5
	ReplaceInodeFamily(repo, "docs/a.txt", first, updated, now+60)
	if got := repo.FindFile("docs/a.txt"); got.AccessedAt != now {
		t.Fatalf("docs/a.txt: sibling atime %d want %d", got.AccessedAt, now)
	}
	if got := repo.FindFile("docs/b.txt"); got.AccessedAt != 0 {
		t.Fatalf("docs/b.txt: epoch atime rewritten to %d", got.AccessedAt)
	}
	for _, name := range []string{"docs/a.txt", "docs/b.txt"} {
		got := repo.FindFile(name)
		if got == nil {
			t.Fatalf("missing %s", name)
		}
		if got.Inode != first.Inode {
			t.Fatalf("%s: inode changed %d want %d", name, got.Inode, first.Inode)
		}
		if got.ModifiedAt != 5 {
			t.Fatalf("%s: mtime %d want 5", name, got.ModifiedAt)
		}
	}
}

// ReplaceInodeFamily must keep each sibling's own atime.
func TestReplaceInodeFamilyKeepsSiblingAtime(t *testing.T) {
	t.Parallel()
	now := int64(9300)
	repo := meta.NewRepoMetadata("demo")
	repo.EnsureDirectory("docs", now)
	base := meta.FileMeta{Size: 1, Chunks: []int64{}, AccessedAt: 11, ModifiedAt: now, UploadedAt: now, ChangedAt: now}
	repo.UpsertFile("docs/a.txt", base, now)
	first := repo.FindFile("docs/a.txt")
	clone := first.Clone()
	clone.AccessedAt = 22
	repo.UpsertFile("docs/b.txt", clone, now)
	repo.RebuildIndexes()
	stored := map[string]int64{}
	for _, n := range repo.FindFilesByInode(first.Inode) {
		stored[n] = repo.FindFile(n).AccessedAt
	}
	if stored["docs/a.txt"] == stored["docs/b.txt"] {
		t.Fatalf("precondition needs distinct sibling atimes, got %+v", stored)
	}

	updated := first.Clone()
	updated.Mode = 0o600
	ReplaceInodeFamily(repo, "docs/a.txt", first, updated, now+60)
	for name, want := range stored {
		if got := repo.FindFile(name).AccessedAt; got != want {
			t.Fatalf("%s: atime %d want %d", name, got, want)
		}
		if got := repo.FindFile(name).Mode; got != 0o600 {
			t.Fatalf("%s: mode %#o want 0600", name, got)
		}
	}
}

// Patch times must land verbatim: HasTimes with explicit stamps stores them.
func TestMetadataPatchTimesLandVerbatim(t *testing.T) {
	t.Parallel()
	now := int64(9500)
	backend := newTestBackend(now)
	backend.seedFile("pt.txt")
	svc := NewService(backend)
	ctx := shfs.WithIdentity(context.Background(), shfs.Identity{UID: 0, GID: 0, Admin: true})

	at := time.Unix(0, 4242)
	mt := time.Unix(0, 4343)
	patch := shfs.MetadataPatch{HasTimes: true, ATime: at, MTime: mt}
	if err := svc.ApplyMetadataPatchContext(ctx, "demo", "pt.txt", patch); err != nil {
		t.Fatalf("patch times: %v", err)
	}
	entry, err := svc.lookupEntryForAccess(ctx, "demo", "pt.txt")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if entry.AccessedAt != 4242 || entry.ModifiedAt != 4343 {
		t.Fatalf("patch times dropped, got atime=%d mtime=%d", entry.AccessedAt, entry.ModifiedAt)
	}
}

// Patch mode sanitization must agree with the chmod verb: non-member group
// setgid bits are stripped in both paths.
func TestMetadataPatchModeMatchesChmodSanitize(t *testing.T) {
	t.Parallel()
	now := int64(9400)
	mkSvc := func() (*Service, *testBackend, context.Context) {
		backend := newTestBackend(now)
		backend.seedDir("docs")
		f := backend.seedFile("docs/f.txt")
		f.GID = 8
		backend.repo.UpsertFile("docs/f.txt", *f, backend.now)
		backend.repo.RebuildIndexes()
		return NewService(backend), backend, shfs.WithIdentity(context.Background(), shfs.Identity{UID: 1, GID: 2})
	}
	svc1, _, ctx := mkSvc()
	if err := svc1.ChmodContext(ctx, "demo", "docs/f.txt", 0o2755); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	e1, _ := svc1.lookupEntryForAccess(ctx, "demo", "docs/f.txt")

	svc2, _, ctx2 := mkSvc()
	patch := shfs.MetadataPatch{HasMode: true, Mode: 0o2755}
	if err := svc2.ApplyMetadataPatchContext(ctx2, "demo", "docs/f.txt", patch); err != nil {
		t.Fatalf("patch: %v", err)
	}
	e2, _ := svc2.lookupEntryForAccess(ctx2, "demo", "docs/f.txt")
	if e1.Mode != e2.Mode {
		t.Fatalf("patch mode %#o diverges from chmod %#o", e2.Mode, e1.Mode)
	}
	if e2.Mode&0o2000 != 0 {
		t.Fatalf("non-member setgid must be stripped, got %#o", e2.Mode)
	}
}
