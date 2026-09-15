package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	gitindex "github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
	"github.com/go-git/go-git/v6/storage"
)

const defaultBranch = "main"

type gitRepo struct {
	dir     string
	base    string
	key     string // cache-dir and lock identity (owner-qualified, see gitCacheKey)
	owner   string
	project string
	token   string
	// remoteBase overrides the https://github.com default. Empty in
	// production; the unit harness points it at a local bare repository.
	remoteBase string

	mu   sync.Mutex
	repo *git.Repository
}

func newGitRepo(cacheDir, owner, project, token string) *gitRepo {
	key := gitCacheKey(owner, project)
	return &gitRepo{
		// cacheDir is the shared base (~/.cache/storhub/git); each
		// project gets one lock-protected directory beneath it.
		dir:     filepath.Join(cacheDir, key),
		base:    cacheDir,
		key:     key,
		owner:   owner,
		project: project,
		token:   token,
	}
}

// gitCacheKey is the cache-directory and lock identity for a project.
// Owner-qualifying it keeps same-named projects from different owners
// (several tokens sharing one GitCacheDir) from colliding in a single
// worktree. An unresolved owner (the lazy /user lookup has not run yet)
// falls back to the bare project name; the gitRepo instance is created
// once per process and keeps its key, so the two spellings never mix
// within one mount.
func gitCacheKey(owner, project string) string {
	if owner == "" {
		return project
	}
	return owner + "__" + project
}

// ensure opens or creates the local worktree. Caller must hold r.mu.
// A directory left behind by a dead process is reclaimed first
// ("cleanup on startup"); a directory held by a live process fails
// honestly instead of corrupting it.
func (r *gitRepo) ensure(ctx context.Context) error {
	if r.repo != nil {
		return nil
	}
	// Claim BEFORE touching the directory. The old check-then-claim let
	// two processes both pass the pidAlive check and both reclaim the same
	// cache dir; a live foreign holder now refuses us up front and only
	// the claimant may wipe or reuse the tree. (claimProjectLock itself
	// still needs O_EXCL to close the remaining window - owned elsewhere.)
	heldByUs := projectLockPid(r.base, r.key) == os.Getpid()
	if err := claimProjectLock(r.base, r.key); err != nil {
		return err
	}
	if _, err := os.Stat(r.dir); err == nil && !heldByUs {
		if err := os.RemoveAll(r.dir); err != nil {
			return fmt.Errorf("reclaim stale cache dir %s: %w", r.dir, err)
		}
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return wrapNoSpace(filepath.Dir(r.dir), fmt.Errorf("mkdir %s: %w", r.dir, err))
	}
	repo, err := git.PlainOpen(r.dir)
	if err == nil {
		r.repo = repo
		return nil
	}
	if !errors.Is(err, git.ErrRepositoryNotExists) {
		return fmt.Errorf("open repo %s: %w", r.project, err)
	}
	// Full history is required: rollback and revision listing resolve
	// arbitrary past commits. The metadata repo is tiny, so depth is cheap.
	repo, err = git.PlainCloneContext(ctx, r.dir, &git.CloneOptions{
		URL:           r.remoteURL(),
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	})
	if err != nil {
		return wrapNoSpace(r.base, fmt.Errorf("clone %s: %w", r.project, err))
	}
	r.repo = repo
	return nil
}

func (r *gitRepo) remoteURL() string {
	if r.remoteBase != "" {
		return r.remoteBase
	}
	return fmt.Sprintf("https://github.com/%s/%s.git", r.owner, r.project)
}

func (r *gitRepo) auth() *githttp.BasicAuth {
	return &githttp.BasicAuth{Username: r.owner, Password: r.token}
}

// release drops this process's claim on the cache dir. remove also
// deletes the directory, per the Shutdown contract: the worktree is a
// pure cache of remote state and is re-cloned on demand.
func (r *gitRepo) release(remove bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.repo = nil
	if remove {
		if err := os.RemoveAll(r.dir); err != nil {
			releaseProjectLock(r.base, r.key)
			return fmt.Errorf("remove git cache dir %s: %w", r.dir, err)
		}
		releaseProjectLock(r.base, r.key)
		return nil
	}
	releaseProjectLock(r.base, r.key)
	return nil
}

// sync fetches from remote and resets the worktree to match origin/main.
func (r *gitRepo) sync(ctx context.Context) error {
	if err := r.repo.FetchContext(ctx, &git.FetchOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	}); err != nil {
		if !errors.Is(err, git.NoErrAlreadyUpToDate) && !errors.Is(err, git.ErrRemoteNotFound) {
			return fmt.Errorf("fetch %s: %w", r.project, err)
		}
	}
	// Resolve origin/main after fetch
	remoteRef, err := r.repo.Reference(plumbing.ReferenceName("refs/remotes/origin/"+defaultBranch), false)
	if err != nil {
		// No remote tracking ref yet - use HEAD as-is
		return nil
	}
	w, err := r.repo.Worktree()
	if err != nil {
		return fmt.Errorf("worktree: %w", err)
	}
	if err := w.Reset(&git.ResetOptions{
		Mode:   git.HardReset,
		Commit: remoteRef.Hash(),
	}); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// readFileRef reads a file from the repo at the given reference (SHA, branch, tag).
func (r *gitRepo) readFileRef(ctx context.Context, ref, path string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	// Sync first: without a fetch, a commit created by another client
	// after our last sync is unresolvable here while the REST path would
	// serve it - the backends must agree on what a ref resolves to.
	if err := r.sync(ctx); err != nil {
		return nil, err
	}
	hash, err := r.resolveRevision(ref)
	if err != nil {
		return nil, err
	}
	commit, err := r.repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("commit %s: %w", hash, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("tree: %w", err)
	}
	file, err := tree.File(path)
	if err != nil {
		return nil, fmt.Errorf("file %s at %s: %w", path, ref, err)
	}
	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return []byte(content), nil
}

// readFileHead reads the file from the latest HEAD, syncing first. The read
// goes through the HEAD TREE, not the worktree filesystem: go-git's
// HardReset never deletes untracked files, so a file left behind by a
// write that was canceled between os.WriteFile and Commit would otherwise
// be served as HEAD truth - the hub would operate on state that was never
// pushed.
func (r *gitRepo) readFileHead(ctx context.Context, path string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	if err := r.sync(ctx); err != nil {
		return nil, err
	}
	return r.readFileContentsNoLock(ctx, path)
}

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
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return "", "", err
	}
	if err := r.sync(ctx); err != nil {
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
			When:  time.Now().UTC(),
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return "", "", err
	}
	if err := r.sync(ctx); err != nil {
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
			When:  time.Now().UTC(),
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

// listTreePaths returns every tracked file path under prefix (e.g.
// ".storhub/objects") from the synced worktree. Used by prune to enumerate
// the repo's content-addressed objects.
func (r *gitRepo) listTreePaths(ctx context.Context, prefix string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	if err := r.sync(ctx); err != nil {
		return nil, err
	}
	root := filepath.Join(r.dir, filepath.FromSlash(prefix))
	var out []string
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(r.dir, p)
		if rerr != nil {
			return rerr
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	return out, nil
}

// deleteCommitPushCAS removes the given paths in one commit with the same
// compare-and-swap semantics as writeCommitPushCASMulti. Prune uses it to
// drop orphaned objects atomically.
func (r *gitRepo) deleteCommitPushCAS(ctx context.Context, paths []string, message, expectedOld string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return "", err
	}
	if err := r.sync(ctx); err != nil {
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
		Author: &object.Signature{Name: "storhub", Email: "storhub@users.noreply.github.com", When: time.Now().UTC()},
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

// headHashNoLock returns the post-sync HEAD commit hash, or the zero hash
// when the repo has no HEAD yet (brand-new, nothing committed). Callers must
// hold r.mu.
func (r *gitRepo) headHashNoLock() (plumbing.Hash, error) {
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return plumbing.ZeroHash, nil
		}
		return plumbing.ZeroHash, fmt.Errorf("HEAD ref: %w", err)
	}
	return ref.Hash(), nil
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

// listFileCommits returns commits that touch the given path, newest first.
func (r *gitRepo) listFileCommits(ctx context.Context, path string) ([]MetadataRevision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	if err := r.sync(ctx); err != nil {
		return nil, err
	}
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		return nil, fmt.Errorf("HEAD ref: %w", err)
	}
	iter, err := r.repo.Log(&git.LogOptions{
		From:  ref.Hash(),
		Order: git.LogOrderCommitterTime,
	})
	if err != nil {
		return nil, fmt.Errorf("log: %w", err)
	}
	defer iter.Close()
	var revisions []MetadataRevision
	if err := iter.ForEach(func(c *object.Commit) error {
		touched, err := commitTouchesPath(c, path)
		if err != nil {
			// A corrupt commit/tree object must fail the listing loudly,
			// not silently omit a revision (no silent error swallowing).
			return err
		}
		if !touched {
			return nil
		}
		revisions = append(revisions, MetadataRevision{
			CommitSHA:   c.Hash.String(),
			Message:     strings.SplitN(c.Message, "\n", 2)[0],
			CommittedAt: c.Committer.When.Unix(),
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("iterate commits: %w", err)
	}
	return revisions, nil
}

// commitTouchesPath reports whether c changed path relative to its parents.
// REST's commits?path= lists only commits that TOUCH the path; a
// tree-containment test would also count every later commit that merely
// still carries it, so revision lists diverged by backend.
// A root commit touches every path it carries; a commit is untouched when
// any parent's tree holds the same blob (git's TREESAME rule).
func commitTouchesPath(c *object.Commit, path string) (bool, error) {
	tree, err := c.Tree()
	if err != nil {
		return false, err
	}
	entry, err := tree.FindEntry(path)
	if err != nil {
		return false, nil // path absent at c: it cannot have touched it
	}
	touched := true
	parents := c.Parents()
	defer parents.Close()
	if err := parents.ForEach(func(p *object.Commit) error {
		ptree, err := p.Tree()
		if err != nil {
			return nil // unreadable parent: treat as differing
		}
		pentry, err := ptree.FindEntry(path)
		if err == nil && pentry.Hash == entry.Hash {
			touched = false // TREESAME to this parent
		}
		return nil
	}); err != nil {
		return false, err
	}
	return touched, nil
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return err
	}
	// Sync first: HEAD and content below must reflect remote truth.
	if err := r.sync(ctx); err != nil {
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
	now := time.Now().UTC()

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
	// Walk up the directory chain to build parent trees
	dir = filepath.Clean(dir)
	if dir == "." || dir == "" {
		// File is in the root - treeHash is already the root tree
	} else {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return err
	}
	if err := r.sync(ctx); err != nil {
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
	now := time.Now().UTC()
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

// readFileContentsNoLock reads current file content from HEAD (no lock, callers must hold r.mu).
func (r *gitRepo) readFileContentsNoLock(ctx context.Context, path string) ([]byte, error) {
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		return nil, fmt.Errorf("HEAD ref: %w", err)
	}
	commit, err := r.repo.CommitObject(ref.Hash())
	if err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("tree: %w", err)
	}
	file, err := tree.File(path)
	if err != nil {
		return nil, fmt.Errorf("file %s: %w", path, err)
	}
	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return []byte(content), nil
}

func (r *gitRepo) resolveRevision(ref string) (plumbing.Hash, error) {
	if len(ref) >= 7 {
		// Try as full SHA first
		h := plumbing.NewHash(ref)
		if _, err := r.repo.CommitObject(h); err == nil {
			return h, nil
		}
	}
	if h, err := r.repo.ResolveRevision(plumbing.Revision(ref)); err == nil {
		return *h, nil
	}
	return plumbing.ZeroHash, fmt.Errorf("cannot resolve %q", ref)
}

// headCommitSHA returns the SHA of the HEAD commit, or empty string if not
// available. It takes r.mu: every caller (index/prune/commit loops) runs
// off-lock while writeCommitPushCAS, squashTreeCAS and release mutate
// r.repo and its refs under the lock - an unlocked read raced release(true)
// into a nil-deref and could pair a CAS token with a mid-commit HEAD.
func (r *gitRepo) headCommitSHA() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.repo == nil {
		return ""
	}
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		return ""
	}
	return ref.Hash().String()
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
