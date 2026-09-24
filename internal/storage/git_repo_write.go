package storage

import (
	"context"
	"errors"
	"fmt"
	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	gitindex "github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/storage"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// writeCommitPush writes a file, commits, and pushes. Returns (commitSHA, contentSHA, error).
func (r *gitRepo) writeCommitPush(ctx context.Context, path string, content []byte, message string) (string, string, error) {
	return r.writeCommitPushCAS(ctx, path, content, message, "")
}

// writeCommitPushCAS is writeCommitPush with compare-and-swap: expectedOld is
// the caller-observed HEAD commit OID (empty disables the pre-check). After
// syncing, a non-empty expectedOld that no longer matches HEAD aborts with a
// 409 conflict instead of silently overwriting the concurrent writer.
// Regardless of the pre-check, the push carries a force-with-lease on the
// post-sync HEAD so a writer racing the sync→push window is rejected rather
// than silently won or lost against.
//
// Token-type note: both commit functions return the HEAD COMMIT
// sha as the content token, not the blob sha the REST contents path
// returns. That is deliberate: the git-path CAS pre-check compares
// expectedOld against HEAD, so only a commit-pairing token is
// self-consistent for this backend. A backend toggle surfaces the foreign
// token type as a 409 and the commit loop's rebase re-reads the correct
// type - noisy, never silent.
func (r *gitRepo) writeCommitPushCAS(ctx context.Context, path string, content []byte, message, expectedOld string) (string, string, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return "", "", err
	}
	if err := r.syncLocked(ctx); err != nil {
		return "", "", err
	}
	base, err := r.headHashNoLock()
	if err != nil {
		return "", "", err
	}
	if expectedOld != "" {
		if base.IsZero() || base.String() != expectedOld {
			return "", "", &ghapi.APIError{
				StatusCode: http.StatusConflict,
				Message:    fmt.Sprintf("stale metadata version: expected %s, HEAD is %s", shortSHA(expectedOld), shortSHA(base.String())),
			}
		}
	}
	w, err := r.repo.Worktree()
	if err != nil {
		return "", "", fmt.Errorf("worktree: %w", err)
	}
	metaDir := filepath.Join(r.dir, filepath.Dir(path))
	if err := os.MkdirAll(metaDir, 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir %s: %w", metaDir, err)
	}
	fsPath := filepath.Join(r.dir, path)
	if err := os.WriteFile(fsPath, content, 0o644); err != nil {
		return "", "", fmt.Errorf("write %s: %w", fsPath, err)
	}
	if _, err := w.Add(path); err != nil {
		return "", "", fmt.Errorf("add %s: %w", path, err)
	}
	hash, err := w.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "storhub",
			Email: "storhub@users.noreply.github.com",
			When:  r.nowUTC(),
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("commit: %w", err)
	}
	pushOpts := &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	}
	if !base.IsZero() {
		pushOpts.ForceWithLease = &git.ForceWithLease{
			RefName: plumbing.ReferenceName("refs/heads/" + defaultBranch),
			Hash:    base,
		}
	}
	if err := r.repo.PushContext(ctx, pushOpts); err != nil {
		return "", "", casConflict(err, base)
	}
	commitSHA := hash.String()
	return commitSHA, commitSHA, nil
}

// writeCommitPushCASMulti commits several files in ONE commit with the same
// compare-and-swap semantics as writeCommitPushCAS: the split index writes its
// content-addressed objects and the manifest together so the manifest CAS is
// the single atomic point (objects become referenced exactly when the
// manifest that names them lands). Git's own content addressing makes
// re-writing an unchanged object file a no-op at the blob level.
func (r *gitRepo) writeCommitPushCASMulti(ctx context.Context, files map[string][]byte, message, expectedOld string) (string, string, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return "", "", err
	}
	if err := r.syncLocked(ctx); err != nil {
		return "", "", err
	}
	base, err := r.headHashNoLock()
	if err != nil {
		return "", "", err
	}
	if expectedOld != "" {
		if base.IsZero() || base.String() != expectedOld {
			return "", "", &ghapi.APIError{
				StatusCode: http.StatusConflict,
				Message:    fmt.Sprintf("stale metadata version: expected %s, HEAD is %s", shortSHA(expectedOld), shortSHA(base.String())),
			}
		}
	}
	w, err := r.repo.Worktree()
	if err != nil {
		return "", "", fmt.Errorf("worktree: %w", err)
	}
	for _, path := range sortedFileKeys(files) {
		content := files[path]
		metaDir := filepath.Join(r.dir, filepath.Dir(path))
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			return "", "", fmt.Errorf("mkdir %s: %w", metaDir, err)
		}
		fsPath := filepath.Join(r.dir, path)
		if err := os.WriteFile(fsPath, content, 0o644); err != nil {
			return "", "", fmt.Errorf("write %s: %w", fsPath, err)
		}
		if _, err := w.Add(path); err != nil {
			return "", "", fmt.Errorf("add %s: %w", path, err)
		}
	}
	hash, err := w.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "storhub",
			Email: "storhub@users.noreply.github.com",
			When:  r.nowUTC(),
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("commit: %w", err)
	}
	pushOpts := &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	}
	if !base.IsZero() {
		pushOpts.ForceWithLease = &git.ForceWithLease{
			RefName: plumbing.ReferenceName("refs/heads/" + defaultBranch),
			Hash:    base,
		}
	}
	if err := r.repo.PushContext(ctx, pushOpts); err != nil {
		return "", "", casConflict(err, base)
	}
	commitSHA := hash.String()
	return commitSHA, commitSHA, nil
}

func sortedFileKeys(files map[string][]byte) []string {
	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// deleteCommitPushCAS removes the given paths in one commit with the same
// compare-and-swap semantics as writeCommitPushCASMulti. Prune uses it to
// drop orphaned objects atomically.
func (r *gitRepo) deleteCommitPushCAS(ctx context.Context, paths []string, message, expectedOld string) (string, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return "", err
	}
	if err := r.syncLocked(ctx); err != nil {
		return "", err
	}
	base, err := r.headHashNoLock()
	if err != nil {
		return "", err
	}
	if expectedOld != "" && (base.IsZero() || base.String() != expectedOld) {
		return "", &ghapi.APIError{
			StatusCode: http.StatusConflict,
			Message:    fmt.Sprintf("stale prune base: expected %s, HEAD is %s", shortSHA(expectedOld), shortSHA(base.String())),
		}
	}
	w, err := r.repo.Worktree()
	if err != nil {
		return "", fmt.Errorf("worktree: %w", err)
	}
	for _, path := range paths {
		fsPath := filepath.Join(r.dir, filepath.FromSlash(path))
		if err := os.Remove(fsPath); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("remove %s: %w", path, err)
		}
		if _, err := w.Remove(path); err != nil {
			// go-git reports an untracked removal with the index's
			// ErrEntryNotFound sentinel; that is benign here (the path was
			// already absent from the index). Match the sentinel, never
			// error prose - "not found" also appears in unrelated failures.
			if !errors.Is(err, gitindex.ErrEntryNotFound) {
				return "", fmt.Errorf("git rm %s: %w", path, err)
			}
		}
	}
	hash, err := w.Commit(message, &git.CommitOptions{
		Author: &object.Signature{Name: "storhub", Email: "storhub@users.noreply.github.com", When: r.nowUTC()},
	})
	if err != nil {
		return "", fmt.Errorf("commit delete: %w", err)
	}
	pushOpts := &git.PushOptions{ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())}}
	if !base.IsZero() {
		pushOpts.ForceWithLease = &git.ForceWithLease{RefName: plumbing.ReferenceName("refs/heads/" + defaultBranch), Hash: base}
	}
	if err := r.repo.PushContext(ctx, pushOpts); err != nil {
		return "", casConflict(err, base)
	}
	return hash.String(), nil
}

// casConflict maps a rejected lease/non-fast-forward push to a 409 conflict
// so git-path CAS failures surface exactly like the REST path's stale-SHA
// rejection; any other push error passes through untouched. The match set
// is go-git's actual lease-failure vocabulary ("non-fast-forward update:
// <ref>" from the lease check, "stale info" from server-side rejection):
// the bare "lease" substring used to live here too and matched "release"
// in remote-hook prose and transport errors quoting releases/ URLs,
// misclassifying hard failures as conflicts the commit loop then rebased
// forever instead of surfacing.
func casConflict(err error, base plumbing.Hash) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "non-fast-forward") || strings.Contains(msg, "stale info") {
		return &ghapi.APIError{
			StatusCode: http.StatusConflict,
			Message:    fmt.Sprintf("concurrent metadata write (HEAD moved past %s): %s", shortSHA(base.String()), msg),
		}
	}
	return fmt.Errorf("push: %w", err)
}

// squashHistory creates a single orphan commit with the current metadata content and force pushes it.
func (r *gitRepo) squashHistory(ctx context.Context, path, message string) error {
	return r.squashHistoryCAS(ctx, path, message, "")
}

// squashHistoryCAS is squashHistory with compare-and-swap: it syncs from
// remote BEFORE reading HEAD (so the squashed content is the latest remote
// truth, never a stale local copy), and the force-push carries a
// force-with-lease on the post-sync HEAD instead of a blind force-push, so a
// concurrent writer racing the squash is rejected with a 409 conflict rather
// than silently discarded. A non-empty expectedOld additionally aborts when
// the post-sync HEAD no longer matches the caller's observation.
func (r *gitRepo) squashHistoryCAS(ctx context.Context, path, message, expectedOld string) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return err
	}
	// Sync first: HEAD and content below must reflect remote truth.
	if err := r.syncLocked(ctx); err != nil {
		return err
	}
	base, err := r.headHashNoLock()
	if err != nil {
		return err
	}
	if base.IsZero() {
		return fmt.Errorf("squash: no HEAD to squash")
	}
	if expectedOld != "" && base.String() != expectedOld {
		return &ghapi.APIError{
			StatusCode: http.StatusConflict,
			Message:    fmt.Sprintf("stale squash base: expected %s, HEAD is %s", shortSHA(expectedOld), shortSHA(base.String())),
		}
	}
	// Read current content from the post-sync HEAD
	content, err := r.readFileContentsNoLock(ctx, path)
	if err != nil {
		return fmt.Errorf("read current content: %w", err)
	}
	// Create a root commit using the storer directly
	storer := r.repo.Storer
	now := r.nowUTC()

	// 1. Create blob
	blobHash, err := storeBlob(storer, content)
	if err != nil {
		return fmt.Errorf("store blob: %w", err)
	}
	// 2. Create nested trees for path like ".storhub/metadata.json"
	dir, file := filepath.Split(path)
	entries := []object.TreeEntry{{
		Name: file,
		Hash: blobHash,
		Mode: 0o100644,
	}}
	treeHash, err := storeTree(storer, entries)
	if err != nil {
		return fmt.Errorf("store leaf tree: %w", err)
	}
	// Walk up the directory chain to build parent trees. A root file needs
	// no parent trees: treeHash is already the root tree.
	dir = filepath.Clean(dir)
	if dir != "." && dir != "" {
		parts := strings.Split(dir, string(filepath.Separator))
		for i := len(parts) - 1; i >= 0; i-- {
			entries = []object.TreeEntry{{
				Name: parts[i],
				Hash: treeHash,
				Mode: 0o040000,
			}}
			treeHash, err = storeTree(storer, entries)
			if err != nil {
				return fmt.Errorf("store tree for %s: %w", parts[i], err)
			}
		}
	}
	// 3. Create commit with no parents
	commit := &object.Commit{
		Author: object.Signature{
			Name: "storhub", Email: "storhub@users.noreply.github.com", When: now,
		},
		Committer: object.Signature{
			Name: "storhub", Email: "storhub@users.noreply.github.com", When: now,
		},
		Message:  message,
		TreeHash: treeHash,
	}
	commitObj := storer.NewEncodedObject()
	if err := commit.Encode(commitObj); err != nil {
		return fmt.Errorf("encode commit: %w", err)
	}
	commitHash, err := storer.SetEncodedObject(commitObj)
	if err != nil {
		return fmt.Errorf("store commit: %w", err)
	}
	commit.Hash = commitHash

	// 4. Update HEAD reference to the new orphan commit
	refName := plumbing.ReferenceName("refs/heads/" + defaultBranch)
	if err := storer.RemoveReference(refName); err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("remove ref: %w", err)
	}
	ref := plumbing.NewHashReference(refName, commitHash)
	if err := storer.SetReference(ref); err != nil {
		return fmt.Errorf("set ref: %w", err)
	}

	// 5. Force push with a lease on the post-sync HEAD: a concurrent
	// writer that advanced the remote since our sync rejects the push
	// with a conflict instead of being silently discarded.
	pushOpts := &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
		Force:         true,
		ForceWithLease: &git.ForceWithLease{
			RefName: plumbing.ReferenceName("refs/heads/" + defaultBranch),
			Hash:    base,
		},
	}
	if err := r.repo.PushContext(ctx, pushOpts); err != nil {
		return casConflict(err, base)
	}
	return nil
}

// squashTreeCAS collapses history into a single orphan commit that keeps the
// ENTIRE current tree (manifest + every object), unlike squashHistoryCAS
// which rebuilds a tree from one path. The split index must never lose its
// objects to a history prune, so this is the history-compaction primitive for
// split (version-5) projects. The force-push carries a lease on the post-sync HEAD so a
// concurrent writer is rejected with 409 rather than discarded.
func (r *gitRepo) squashTreeCAS(ctx context.Context, message, expectedOld string) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return err
	}
	if err := r.syncLocked(ctx); err != nil {
		return err
	}
	base, err := r.headHashNoLock()
	if err != nil {
		return err
	}
	if base.IsZero() {
		return fmt.Errorf("squash: no HEAD to squash")
	}
	if expectedOld != "" && base.String() != expectedOld {
		return &ghapi.APIError{
			StatusCode: http.StatusConflict,
			Message:    fmt.Sprintf("stale squash base: expected %s, HEAD is %s", shortSHA(expectedOld), shortSHA(base.String())),
		}
	}
	head, err := r.repo.CommitObject(base)
	if err != nil {
		return fmt.Errorf("head commit: %w", err)
	}
	storer := r.repo.Storer
	now := r.nowUTC()
	commit := &object.Commit{
		Author:    object.Signature{Name: "storhub", Email: "storhub@users.noreply.github.com", When: now},
		Committer: object.Signature{Name: "storhub", Email: "storhub@users.noreply.github.com", When: now},
		Message:   message,
		TreeHash:  head.TreeHash,
	}
	obj := storer.NewEncodedObject()
	if err := commit.Encode(obj); err != nil {
		return fmt.Errorf("encode squash commit: %w", err)
	}
	commitHash, err := storer.SetEncodedObject(obj)
	if err != nil {
		return fmt.Errorf("store squash commit: %w", err)
	}
	refName := plumbing.ReferenceName("refs/heads/" + defaultBranch)
	if err := storer.RemoveReference(refName); err != nil && !errors.Is(err, plumbing.ErrReferenceNotFound) {
		return fmt.Errorf("remove ref: %w", err)
	}
	if err := storer.SetReference(plumbing.NewHashReference(refName, commitHash)); err != nil {
		return fmt.Errorf("set ref: %w", err)
	}
	pushOpts := &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
		Force:         true,
		ForceWithLease: &git.ForceWithLease{
			RefName: plumbing.ReferenceName("refs/heads/" + defaultBranch),
			Hash:    base,
		},
	}
	if err := r.repo.PushContext(ctx, pushOpts); err != nil {
		return casConflict(err, base)
	}
	return nil
}

func storeBlob(s storage.Storer, data []byte) (plumbing.Hash, error) {
	o := s.NewEncodedObject()
	o.SetType(plumbing.BlobObject)
	w, err := o.Writer()
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return plumbing.ZeroHash, err
	}
	// The writer must be closed before the object is stored: some storers
	// only flush (and some only compute size) on Close, so skipping it
	// works with the memory storer and breaks on others.
	if err := w.Close(); err != nil {
		return plumbing.ZeroHash, err
	}
	return s.SetEncodedObject(o)
}

func storeTree(s storage.Storer, entries []object.TreeEntry) (plumbing.Hash, error) {
	tree := &object.Tree{Entries: entries}
	o := s.NewEncodedObject()
	if err := tree.Encode(o); err != nil {
		return plumbing.ZeroHash, fmt.Errorf("encode tree: %w", err)
	}
	return s.SetEncodedObject(o)
}
