package fs

import (
	"context"
	"errors"
	"slices"
	"syscall"
	"testing"

	meta "github.com/FarelRA/storhub/internal/metadata"
)

// countingBackend records readonly loads so StatFS cache behavior is
// observable through the Service's own mutation path.
type countingBackend struct {
	*testBackend
	loads int
}

func (c *countingBackend) LoadRepoMetadataReadonlyContext(ctx context.Context, project string) (*meta.RepoMetadata, string, error) {
	c.loads++
	return c.testBackend.LoadRepoMetadataReadonlyContext(ctx, project)
}

// TestStatFSContextCachesAndInvalidates pins the df-storm fix: the
// O(files+chunks) aggregate is served from cache while nothing moved, and
// a mutation through the Service invalidates it immediately.
func TestStatFSContextCachesAndInvalidates(t *testing.T) {
	t.Parallel()
	backend := &countingBackend{testBackend: newTestBackend(400)}
	backend.seedDir("docs")
	backend.seedFile("docs/a.txt", []byte("hello"))
	svc := NewService(backend)
	ctx := WithIdentity(context.Background(), Identity{UID: 0, GID: 0, Admin: true})
	first, err := svc.StatFSContext(ctx, "demo")
	if err != nil || first.Files != 1 {
		t.Fatalf("initial statfs: %+v err=%v", first, err)
	}
	second, err := svc.StatFSContext(ctx, "demo")
	if err != nil || *second != *first {
		t.Fatalf("cached statfs must agree: %+v vs %+v err=%v", second, first, err)
	}
	if _, err := svc.CreateFileContext(ctx, "demo", "docs/b.txt"); err != nil {
		t.Fatalf("create: %v", err)
	}
	third, err := svc.StatFSContext(ctx, "demo")
	if err != nil {
		t.Fatalf("statfs after mutation: %v", err)
	}
	if third.Files != 2 {
		t.Fatalf("mutation must invalidate the statfs cache: %+v", third)
	}
}

// TestNormalizeIdentityFastPath pins the per-request identity caching:
// an already-normalized identity (what WithIdentity stores) must flow
// through every DAC check without cloning or sorting the group slice
// again, while unnormalized input still canonicalizes.
func TestNormalizeIdentityFastPath(t *testing.T) {
	t.Parallel()
	id := Identity{UID: 5, GID: 7, Groups: []uint32{3, 7}}
	normalized := normalizeIdentity(id)
	if !slices.IsSorted(normalized.Groups) || len(normalized.Groups) != 2 {
		t.Fatalf("first normalization wrong: %+v", normalized.Groups)
	}
	again := normalizeIdentity(normalized)
	if len(again.Groups) == 0 || &again.Groups[0] != &normalized.Groups[0] {
		t.Fatalf("already-normalized identity must not be re-cloned: %v", again.Groups)
	}
	raw := Identity{GID: 2, Groups: []uint32{5, 2, 5, 9}}
	got := normalizeIdentity(raw)
	want := []uint32{2, 5, 9}
	if !slices.Equal(got.Groups, want) {
		t.Fatalf("normalizeIdentity(%v) = %v, want %v", raw.Groups, got.Groups, want)
	}
	missing := Identity{GID: 4, Groups: []uint32{1, 2}}
	got = normalizeIdentity(missing)
	if !slices.Equal(got.Groups, []uint32{1, 2, 4}) {
		t.Fatalf("primary GID must join the group set: %v", got.Groups)
	}
}

// TestCheckAccessResolvedConsumesTraversedChain pins the single-resolve
// DAC contract: the traversed chain from StatResolveTracked covers the
// symlink walk (a 0700 directory containing an absolute link must not
// leak through the link), while the plain check on the concrete key alone
// would miss it.
func TestCheckAccessResolvedConsumesTraversedChain(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(500)
	backend.seedDir("pub")
	backend.seedFile("pub/x.txt", []byte("data"))
	backend.seedDir("secret")
	dir := backend.repo.GetDirectory("secret")
	dir.Mode = 0o700
	dir.UID = 11
	dir.GID = 22
	backend.repo.Dirs()["secret"] = *dir
	backend.repo.UpsertFile("secret/link", meta.FileMeta{
		Mode: 0o777, UID: 11, GID: 22, Symlink: "/pub/x.txt",
		UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now,
	}, backend.now)
	repo := backend.repo
	clean, traversed, err := StatResolveTracked(repo, "secret/link")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if clean != "pub/x.txt" {
		t.Fatalf("clean = %q, want pub/x.txt", clean)
	}
	if !slices.Contains(traversed, "secret") {
		t.Fatalf("traversed chain must include the link's parent: %v", traversed)
	}
	stranger := WithIdentity(context.Background(), Identity{UID: 12, GID: 12})
	if err := CheckReadAccess(stranger, repo, clean); err != nil {
		t.Fatalf("plain check on the concrete key alone must pass (documents the leak the chain closes): %v", err)
	}
	if err := CheckReadAccessResolved(stranger, repo, clean, traversed); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("resolved check must deny through the 0700 link parent, got %v", err)
	}
	owner := WithIdentity(context.Background(), Identity{UID: 11, GID: 22})
	if err := CheckReadAccessResolved(owner, repo, clean, traversed); err != nil {
		t.Fatalf("resolved check must allow the link's owner: %v", err)
	}
}

// TestStatResolveTrackedPhysicalSemantics guards the prefix-stack rewrite
// of resolvePathTracked: ".." pops the resolved stack (physical parent),
// absolute links reset the cursor, and relative links splice against the
// link's parent - all while the traversed chain stays in walk order.
func TestStatResolveTrackedPhysicalSemantics(t *testing.T) {
	t.Parallel()
	backend := newTestBackend(600)
	backend.seedDir("a")
	backend.seedDir("a/b")
	backend.seedDir("b")
	backend.seedDir("b/c")
	backend.seedFile("b/c/f.txt", []byte("x"))
	backend.repo.UpsertFile("a/link", meta.FileMeta{
		Mode: 0o777, Symlink: "b/c",
		UploadedAt: backend.now, ModifiedAt: backend.now, AccessedAt: backend.now, ChangedAt: backend.now,
	}, backend.now)
	repo := backend.repo

	// link -> b/c resolves relative to its parent a, so the physical
	// parent of a/b/c is a/b - not the lexical "a" a naive join yields.
	clean, traversed, err := LstatResolveTracked(repo, "a/link/..")
	if err != nil {
		t.Fatalf("resolve a/link/..: %v", err)
	}
	if clean != "a/b" {
		t.Fatalf("physical parent of a/link (-> b/c) must be a/b, got %q", clean)
	}
	if !slices.Contains(traversed, "a/b") {
		t.Fatalf("traversed must record the descended chain in walk order: %v", traversed)
	}
	clean, _, err = StatResolveTracked(repo, "a/link/f.txt")
	if err != nil {
		t.Fatalf("resolve a/link/f.txt: %v", err)
	}
	if clean != "a/b/c/f.txt" {
		t.Fatalf("relative link must splice against its parent dir, got %q", clean)
	}
}
