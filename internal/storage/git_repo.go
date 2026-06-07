package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v6"
	gitclient "github.com/go-git/go-git/v6/plumbing/client"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport/http"
	"github.com/go-git/go-git/v6/storage"
)

const defaultBranch = "main"

type gitRepo struct {
	dir     string
	owner   string
	project string
	token   string

	mu   sync.Mutex
	repo *git.Repository
}

func newGitRepo(cacheDir, owner, project, token string) *gitRepo {
	return &gitRepo{
		dir:     filepath.Join(cacheDir, "repos", project),
		owner:   owner,
		project: project,
		token:   token,
	}
}

func (r *gitRepo) remoteURL() string {
	return fmt.Sprintf("https://github.com/%s/%s.git", r.owner, r.project)
}

func (r *gitRepo) auth() *http.BasicAuth {
	return &http.BasicAuth{Username: r.owner, Password: r.token}
}

func (r *gitRepo) ensure(ctx context.Context) error {
	if r.repo != nil {
		return nil
	}
	repo, err := git.PlainOpen(r.dir)
	if err == nil {
		r.repo = repo
		return nil
	}
	if !errors.Is(err, git.ErrRepositoryNotExists) {
		return fmt.Errorf("open repo %s: %w", r.project, err)
	}
	if err := os.MkdirAll(r.dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", r.dir, err)
	}
	repo, err = git.PlainCloneContext(ctx, r.dir, &git.CloneOptions{
		URL:           r.remoteURL(),
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
		Depth:         1,
	})
	if err != nil {
		return fmt.Errorf("clone %s: %w", r.project, err)
	}
	r.repo = repo
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
		// No remote tracking ref yet — use HEAD as-is
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
	defer content.Close()
	data, err := io.ReadAll(content)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return data, nil
}

// writeCommitPush writes a file, commits, and pushes. Returns (commitSHA, contentSHA, error).
func (r *gitRepo) writeCommitPush(ctx context.Context, path string, content []byte, message string) (string, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return "", "", err
	}
	if err := r.sync(ctx); err != nil {
		return "", "", err
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
	if err := r.repo.PushContext(ctx, &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
	}); err != nil {
		return "", "", fmt.Errorf("push: %w", err)
	}
	commitSHA := hash.String()
	return commitSHA, commitSHA, nil
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
			CommittedAt: c.Committer.When,
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("iterate commits: %w", err)
	}
	return revisions, nil
}

// squashHistory creates a single orphan commit with the current metadata content and force pushes it.
func (r *gitRepo) squashHistory(ctx context.Context, path, message string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ensure(ctx); err != nil {
		return err
	}
	// Read current content from HEAD
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
		// File is in the root — treeHash is already the root tree
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
	if err := storer.RemoveReference(refName); err != nil {
		if !strings.Contains(err.Error(), "not found") {
			return fmt.Errorf("remove ref: %w", err)
		}
	}
	ref := plumbing.NewHashReference(refName, commitHash)
	if err := storer.SetReference(ref); err != nil {
		return fmt.Errorf("set ref: %w", err)
	}

	// 5. Force push
	if err := r.repo.PushContext(ctx, &git.PushOptions{
		ClientOptions: []gitclient.Option{gitclient.WithHTTPAuth(r.auth())},
		Force:         true,
	}); err != nil {
		return fmt.Errorf("force push: %w", err)
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
