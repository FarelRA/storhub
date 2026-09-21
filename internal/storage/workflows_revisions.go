package storage

import (
	"context"
	"errors"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	ghapi "github.com/FarelRA/storhub/internal/github"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

func (h *StorHub) listMetadataRevisions(ctx context.Context, project string) ([]MetadataRevision, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	path, err := h.activeIndexPath(ctx, project)
	if err != nil {
		return nil, err
	}
	if repo := h.getGitRepo(project); repo != nil {
		revisions, err := repo.listFileCommits(ctx, path)
		if err != nil {
			// Infrastructure failures must not masquerade as "project not
			// found"; propagate them so callers can retry or report.
			return nil, fmt.Errorf("list metadata revisions: %w", err)
		}
		if len(revisions) == 0 {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return revisions, nil
	}
	commits, err := h.gh.ListFileCommits(ctx, h.owner, project, path)
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, shfs.NotFound(fmt.Sprintf("project %s", project))
		}
		return nil, err
	}
	revisions := make([]MetadataRevision, 0, len(commits))
	for _, commit := range commits {
		revisions = append(revisions, MetadataRevision{CommitSHA: commit.SHA, Message: commit.Message, CommittedAt: commit.CommittedAt.UnixNano()})
	}
	return revisions, nil
}

// activeIndexPath returns the repo path carrying the project's current index
// history: the split manifest when the project is (or will be) version 5,
// else the legacy metadata blob. Revision listing and history walks must
// follow the active layout or they see an empty history for a split project.

// activeIndexPath returns the repo path carrying the project's current index
// history: the split manifest when the project is (or will be) version 5,
// else the legacy metadata blob. Revision listing and history walks must
// follow the active layout or they see an empty history for a split project.
func (h *StorHub) activeIndexPath(ctx context.Context, project string) (string, error) {
	if pm := h.lookupProjectMeta(project); pm != nil {
		pm.mu.RLock()
		split := pm.meta.IsSplit()
		pm.mu.RUnlock()
		if split {
			return indexFilePath, nil
		}
	}
	data, _, found, err := h.readIndexHead(ctx, project)
	if err != nil {
		return "", err
	}
	if found && meta.IsManifest(data) {
		return indexFilePath, nil
	}
	return metadataFilePath, nil
}

func (h *StorHub) getMetadataRevision(ctx context.Context, project, commitSHA string) (*RepoMetadata, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	// Detect the layout AT THIS REVISION by shape: a split-era commit carries
	// the manifest, a legacy-era commit the metadata blob. Reading the
	// manifest first makes a legacy revision still readable across the
	// migration boundary (the grace window) and a split revision load objects.
	if data, found, err := h.readIndexRevision(ctx, project, commitSHA); err != nil {
		return nil, err
	} else if found {
		m, _, err := h.loadIndexTreeAtRef(ctx, project, commitSHA, data)
		if err != nil {
			return nil, fmt.Errorf("parse metadata revision: %w", err)
		}
		m.Normalize(project, h.config.Now().UnixNano())
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("validate metadata revision: %w", err)
		}
		return m, nil
	}
	return nil, shfs.NotFound(fmt.Sprintf("metadata revision %s", shortSHA(commitSHA)))
}

// readIndexRevision fetches the manifest or metadata blob at a specific commit
// SHA. Callers distinguish the layout with meta.IsManifest(data). Dispatches
// through readIndexDoc (index.go), the single git-vs-REST point.

// readIndexRevision fetches the manifest or metadata blob at a specific commit
// SHA. Callers distinguish the layout with meta.IsManifest(data). Dispatches
// through readIndexDoc (index.go), the single git-vs-REST point.
func (h *StorHub) readIndexRevision(ctx context.Context, project, commitSHA string) ([]byte, bool, error) {
	if d, _, ok, rerr := h.readIndexDoc(ctx, project, indexFilePath, commitSHA); rerr != nil {
		return nil, false, rerr
	} else if ok {
		return d, true, nil
	}
	d, _, ok, rerr := h.readIndexDoc(ctx, project, metadataFilePath, commitSHA)
	if rerr != nil {
		return nil, false, rerr
	}
	return d, ok, nil
}

// loadIndexTreeAtRef materializes a revision's tree, detecting the layout by
// shape and fetching split objects at the same ref so a historical manifest
// resolves its historical objects.

// loadIndexTreeAtRef materializes a revision's tree, detecting the layout by
// shape and fetching split objects at the same ref so a historical manifest
// resolves its historical objects.
func (h *StorHub) loadIndexTreeAtRef(ctx context.Context, project, ref string, data []byte) (*RepoMetadata, uint64, error) {
	if !meta.IsManifest(data) {
		m := NewRepoMetadata(project)
		if err := m.FromJSON(data); err != nil {
			return nil, 0, err
		}
		return m, 0, nil
	}
	manifest, err := meta.ParseManifest(data)
	if err != nil {
		return nil, 0, err
	}
	fetched := func(sha string) ([]byte, error) { return h.fetchObjectAtRef(ctx, project, ref, sha) }
	loaded, err := meta.LoadTreeParallel(manifest, fetched)
	if err != nil {
		return nil, 0, err
	}
	loaded.Project = project
	return loaded, manifest.ObjectCount, nil
}

// fetchObjectAtRef loads one index object pinned to a commit SHA (cache is
// content-addressed and layout-agnostic, so it serves any ref). Backend
// bytes come from readObjectBytes (objects.go); the ref-pinned error
// wording is preserved.

// fetchObjectAtRef loads one index object pinned to a commit SHA (cache is
// content-addressed and layout-agnostic, so it serves any ref). Backend
// bytes come from readObjectBytes (objects.go); the ref-pinned error
// wording is preserved.
func (h *StorHub) fetchObjectAtRef(ctx context.Context, project, ref, sha string) ([]byte, error) {
	cache := h.objectCacheFor(project)
	if data, ok := cache.get(sha); ok {
		return data, nil
	}
	data, err := h.readObjectBytes(ctx, project, ref, sha)
	if err != nil {
		return nil, fmt.Errorf("fetch object %s at %s: %w", shortSHA(sha), shortSHA(ref), err)
	}
	if meta.ObjectSHA(data) != sha {
		return nil, fmt.Errorf("object %s failed content verification at %s", shortSHA(sha), shortSHA(ref))
	}
	cache.put(sha, data)
	return data, nil
}

// validateSnapshotRefs checks a snapshot against live server state before
// a rollback/revert commits it. Structural validation is total; asset
// existence is checked LAZILY and TARGETED: only releases the snapshot
// references are examined, and a release's full asset list is paginated
// only when its embedded view is untrustworthy (at/above the truncation
// danger band, or an expected ID is missing from it). The old form built
// an index over every asset of every release - O(total assets) transient
// memory per call, three calls per rollback.
//
// op names the caller ("rollback"/"revert") and prefixes every error so a
// shared validator does not misattribute failures to rollback when it also
// serves the revert path (audit 24).
//
// Range geometry is enforced, not just membership (audit 18): a reverted
// chunk pointing at a live asset with an out-of-range window previously
// committed successfully and failed later as a 416 at read time. Every
// chunk must satisfy AssetOffset >= 0 and AssetOffset+Size <= asset Size.
// Sizes ride the same targeted listing as membership (embedded view when
// trusted, paginated ListReleaseAssets otherwise).
//
// The release list itself stays a fresh (uncached) listReleases: the point
// of the re-checks around the commit is to catch deletions that landed
// after the previous check, which a TTL cache would hide.
// NEEDS-INTEGRATION(13): a targeted GET /releases/assets/{id} would replace
// the per-release fallback entirely; the ghapi client exposes no such
// method today, so the fallback paginates ListReleaseAssets instead.
