package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	githttp "github.com/go-git/go-git/v6/plumbing/transport/http"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	// now is the injectable clock for commit timestamps, threaded from
	// h.config.Now (clock injection). Nil means time.Now: git timestamps
	// previously used wall time while the mock used logical
	// 1700000000+id, so ordering diverged and tests could not freeze
	// time. All commit signatures must go through nowUTC().
	now func() time.Time

	// stateMu guards in-memory state (the repo pointer). RLock serves
	// HEAD reads and existence checks; only ensure/release take the
	// write lock. Network I/O (fetch/clone/push) and worktree mutation
	// never hold stateMu: they serialize on syncMu instead, so same-
	// project git traffic does not convoy behind a single mutex held
	// across the network.
	stateMu sync.RWMutex
	// syncMu serializes remote synchronization and mutating worktree
	// operations (fetch+reset, commit+push, squash). Reads pinned to a
	// commit SHA do not need it beyond repo-pointer access.
	syncMu sync.Mutex
	repo   *git.Repository
}

// newGitRepo is the single constructor; the commit clock stays nil here
// and callers thread their clock through r.now after construction.
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

// nowUTC returns the commit timestamp through the injectable clock.
func (r *gitRepo) nowUTC() time.Time {
	if r != nil && r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
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

// ensure opens or creates the local worktree. It serializes concurrent
// creators on syncMu; the repo pointer itself is guarded by stateMu and
// network I/O never holds stateMu.
func (r *gitRepo) ensure(ctx context.Context) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	return r.ensureLocked(ctx)
}

// ensureLocked opens or creates the worktree. Caller holds syncMu.
// A directory left behind by a dead process is reclaimed first
// ("cleanup on startup"); a directory held by a live process fails
// honestly instead of corrupting it.
func (r *gitRepo) ensureLocked(ctx context.Context) error {
	r.stateMu.RLock()
	if r.repo != nil {
		r.stateMu.RUnlock()
		return nil
	}
	r.stateMu.RUnlock()
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
		r.stateMu.Lock()
		r.repo = repo
		r.stateMu.Unlock()
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
	r.stateMu.Lock()
	r.repo = repo
	r.stateMu.Unlock()
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
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	r.stateMu.Lock()
	r.repo = nil
	r.stateMu.Unlock()
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
// It serializes on syncMu; the repo pointer is read under stateMu.RLock
// so HEAD observers never block on network I/O beyond the fetch itself.
func (r *gitRepo) sync(ctx context.Context) error {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return err
	}
	return r.syncLocked(ctx)
}

// syncLocked fetches and hard-resets. Caller holds syncMu.
func (r *gitRepo) syncLocked(ctx context.Context) error {
	r.stateMu.RLock()
	repo := r.repo
	r.stateMu.RUnlock()
	if repo == nil {
		return errors.New("git repo not initialized")
	}
	if err := repo.FetchContext(ctx, &git.FetchOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	}); err != nil {
		if !errors.Is(err, git.NoErrAlreadyUpToDate) && !errors.Is(err, git.ErrRemoteNotFound) {
			return fmt.Errorf("fetch %s: %w", r.project, err)
		}
	}
	// Resolve origin/main after fetch
	remoteRef, err := repo.Reference(plumbing.ReferenceName("refs/remotes/origin/"+defaultBranch), false)
	if err != nil {
		// No remote tracking ref yet - use HEAD as-is
		return nil
	}
	w, err := repo.Worktree()
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

// syncAndPinHEAD syncs once and returns the post-sync HEAD hash. Batch
// loaders (index object fetches) pin this hash and resolve every object
// against it via readFileAtPinned instead of paying a fetch+hard-reset
// per object (a 100k-file cold load paid thousands of
// serialized fetch+reset cycles on one mutex).
func (r *gitRepo) syncAndPinHEAD(ctx context.Context) (plumbing.Hash, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return plumbing.ZeroHash, err
	}
	if err := r.syncLocked(ctx); err != nil {
		return plumbing.ZeroHash, err
	}
	return r.headHashLocked(), nil
}

// headHashLocked returns the post-sync HEAD hash. Caller holds syncMu or
// stateMu (read or write); it reads the repo pointer under stateMu.RLock
// when the caller does not already guarantee stability.
func (r *gitRepo) headHashLocked() plumbing.Hash {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if r.repo == nil {
		return plumbing.ZeroHash
	}
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		return plumbing.ZeroHash
	}
	return ref.Hash()
}

// readFileAtPinned reads path at the given commit hash without syncing.
// It takes only stateMu.RLock (no network, no worktree mutation), so
// concurrent pinned reads do not convoy behind fetches. The hash must
// come from syncAndPinHEAD or headCommitSHA; a concurrent sync advancing
// HEAD does not affect this read.
func (r *gitRepo) readFileAtPinned(ctx context.Context, head plumbing.Hash, path string) ([]byte, error) {
	_ = ctx
	r.stateMu.RLock()
	repo := r.repo
	r.stateMu.RUnlock()
	if repo == nil {
		return nil, fmt.Errorf("git repo not initialized")
	}
	if head.IsZero() {
		return nil, fmt.Errorf("no HEAD to read %s", path)
	}
	commit, err := repo.CommitObject(head)
	if err != nil {
		return nil, fmt.Errorf("commit %s: %w", head, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("tree: %w", err)
	}
	file, err := tree.File(path)
	if err != nil {
		return nil, fmt.Errorf("file %s at %s: %w", path, head, err)
	}
	content, err := file.Contents()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return []byte(content), nil
}

// readFileRef reads a file from the repo at the given reference (SHA, branch, tag).
func (r *gitRepo) readFileRef(ctx context.Context, ref, path string) ([]byte, error) {
	// Sync first: without a fetch, a commit created by another client
	// after our last sync is unresolvable here while the REST path would
	// serve it - the backends must agree on what a ref resolves to.
	// The sync holds syncMu; the pinned read below holds only stateMu.
	if err := r.ensure(ctx); err != nil {
		return nil, err
	}
	r.syncMu.Lock()
	if err := r.ensureLocked(ctx); err != nil {
		r.syncMu.Unlock()
		return nil, err
	}
	if err := r.syncLocked(ctx); err != nil {
		r.syncMu.Unlock()
		return nil, err
	}
	hash, err := r.resolveRevisionLocked(ref)
	r.syncMu.Unlock()
	if err != nil {
		return nil, err
	}
	r.stateMu.RLock()
	repo := r.repo
	r.stateMu.RUnlock()
	if repo == nil {
		return nil, fmt.Errorf("git repo not initialized")
	}
	commit, err := repo.CommitObject(hash)
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
	head, err := r.syncAndPinHEAD(ctx)
	if err != nil {
		return nil, err
	}
	return r.readFileAtPinned(ctx, head, path)
}

// headHashNoLock returns the post-sync HEAD commit hash, or the zero hash
// when the repo has no HEAD yet (brand-new, nothing committed). Callers must
// hold syncMu (writers) or have pinned HEAD via syncAndPinHEAD.
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

// readFileContentsNoLock reads current file content from HEAD (no lock, callers must hold syncMu).
func (r *gitRepo) readFileContentsNoLock(_ context.Context, path string) ([]byte, error) {
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

// resolveRevisionLocked resolves a revision. Caller holds syncMu.
func (r *gitRepo) resolveRevisionLocked(ref string) (plumbing.Hash, error) {
	r.stateMu.RLock()
	repo := r.repo
	r.stateMu.RUnlock()
	if repo == nil {
		return plumbing.ZeroHash, fmt.Errorf("cannot resolve %q", ref)
	}
	return resolveRevisionIn(repo, ref)
}

func resolveRevisionIn(repo *git.Repository, ref string) (plumbing.Hash, error) {
	if len(ref) >= 7 {
		// Try as full SHA first
		h := plumbing.NewHash(ref)
		if _, err := repo.CommitObject(h); err == nil {
			return h, nil
		}
	}
	if h, err := repo.ResolveRevision(plumbing.Revision(ref)); err == nil {
		return *h, nil
	}
	return plumbing.ZeroHash, fmt.Errorf("cannot resolve %q", ref)
}

// headCommitSHA returns the SHA of the HEAD commit, or empty string if not
// available. It takes stateMu.RLock: every caller (index/prune/commit loops)
// runs off-lock while writeCommitPushCAS, squashTreeCAS and release mutate
// r.repo and its refs under syncMu+stateMu-write - an unlocked read raced
// release(true) into a nil-deref and could pair a CAS token with a
// mid-commit HEAD.
func (r *gitRepo) headCommitSHA() string {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	if r.repo == nil {
		return ""
	}
	ref, err := r.repo.Reference(plumbing.HEAD, true)
	if err != nil {
		return ""
	}
	return ref.Hash().String()
}
