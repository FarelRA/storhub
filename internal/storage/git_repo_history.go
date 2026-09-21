package storage

import (
	"context"
	"fmt"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"os"
	"path/filepath"
	"strings"
)

// listTreePaths returns every tracked file path under prefix (e.g.
// ".storhub/objects") from the synced worktree. Used by prune to enumerate
// the repo's content-addressed objects.
func (r *gitRepo) listTreePaths(ctx context.Context, prefix string) ([]string, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return nil, err
	}
	if err := r.syncLocked(ctx); err != nil {
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

// listFileCommits returns commits that touch the given path, newest first.
func (r *gitRepo) listFileCommits(ctx context.Context, path string) ([]MetadataRevision, error) {
	r.syncMu.Lock()
	defer r.syncMu.Unlock()
	if err := r.ensureLocked(ctx); err != nil {
		return nil, err
	}
	if err := r.syncLocked(ctx); err != nil {
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
			CommittedAt: c.Committer.When.UnixNano(),
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
