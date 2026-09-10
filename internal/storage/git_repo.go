package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing/object"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
	"github.com/go-git/go-git/v6/storage"
)

const defaultBranch = "main"

type gitRepo struct {
	dir     string
	base    string
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
	return &gitRepo{
		// cacheDir is the shared base (~/.cache/storhub/git); each
		// project gets one lock-protected directory beneath it.
		dir:     filepath.Join(cacheDir, project),
		base:    cacheDir,
		owner:   owner,
		project: project,
		token:   token,
	}
}

// ensure opens or creates the local worktree. Caller must hold r.mu.
// A directory left behind by a dead process is reclaimed first
// ("cleanup on startup"); a directory held by a live process fails
// honestly instead of corrupting it.
func (r *gitRepo) ensure(ctx context.Context) error {
	if r.repo != nil {
		return nil
	}
	if _, err := os.Stat(r.dir); err == nil {
		if pid := projectLockPid(r.base, r.project); pid != 0 && pid != os.Getpid() && pidAlive(pid) {
			return fmt.Errorf("cache dir %s is held by live process %d", r.dir, pid)
		}
		if projectLockPid(r.base, r.project) != os.Getpid() {
			if err := os.RemoveAll(r.dir); err != nil {
				return fmt.Errorf("reclaim stale cache dir %s: %w", r.dir, err)
			}
		}
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return wrapNoSpace(filepath.Dir(r.dir), fmt.Errorf("mkdir %s: %w", r.dir, err))
	}
	if err := claimProjectLock(r.base, r.project); err != nil {
		return err
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
			releaseProjectLock(r.base, r.project)
			return fmt.Errorf("remove git cache dir %s: %w", r.dir, err)
		}
		releaseProjectLock(r.base, r.project)
		return nil
	}
	releaseProjectLock(r.base, r.project)
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

// readFileHead reads the file from the latest HEAD, syncing first.
func (r *gitRepo) readFileHead(ctx context.Context, path string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	if err := r.sync(ctx); err != nil {
		return nil, err
	}
	w, err := r.repo.Worktree()
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}
	content, err := w.Filesystem().Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = content.Close() }()
	data, err := io.ReadAll(content)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
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
// rejection; any other push error passes through untouched.
func casConflict(err error, base plumbing.Hash) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "non-fast-forward") || strings.Contains(msg, "stale info") || strings.Contains(msg, "lease") {
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
		tree, err := c.Tree()
		if err != nil {
			return nil
		}
		if _, err := tree.File(path); err != nil {
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

// headCommitSHA returns the SHA of the HEAD commit, or empty string if not available.
func (r *gitRepo) headCommitSHA() string {
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
