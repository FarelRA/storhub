package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
	meta "github.com/FarelRA/storhub/internal/metadata"
)

// Granular prune. Full-history retention is a deliberate choice (locked
// decision), so nothing referenced by any retained manifest is ever
// auto-deleted; prune is explicit and reclaims only genuine garbage:
//
//	objects   content-addressed index objects referenced by NO retained
//	          manifest (orphans from failed CAS attempts, or garbage left
//	          after a history compaction). Both backends.
//	assets    unreferenced release assets (the existing PurgeUntracked).
//	          Both backends.
//	history   collapse manifests older than the checkpoint into one commit.
//	          GIT BACKEND ONLY: on REST, history is GitHub-owned and the
//	          contents API cannot delete revisions, so prune reports that
//	          honestly instead of pretending.
//	all       history (where possible) + objects + assets.

// PruneScope selects what a prune run reclaims.
type PruneScope string

const (
	PruneObjects PruneScope = "objects"
	PruneAssets  PruneScope = "assets"
	PruneHistory PruneScope = "history"
	PruneAll     PruneScope = "all"
)

// PruneResult reports what a prune did (or would do, under DryRun).
type PruneResult struct {
	Scope            PruneScope `json:"scope"`
	DryRun           bool       `json:"dry_run"`
	DeletedObjects   int        `json:"deleted_objects"`
	DeletedReleases  int        `json:"deleted_releases"`
	DeletedAssets    int        `json:"deleted_assets"`
	HistoryCompacted bool       `json:"history_compacted"`
	Notes            []string   `json:"notes,omitempty"`
}

// PruneProject is the context-free CLI/embedder entry point: scope is one of
// "objects", "assets", "history", or "all".
func (h *StorHub) PruneProject(project, scope string, keep int, dryRun bool) (*PruneResult, error) {
	return h.Prune(context.Background(), project, PruneScope(scope), keep, dryRun)
}

// Prune runs the requested scope. keep bounds history compaction (git):
// manifests newer than keep commits are always retained; the checkpoint
// collapses everything older. dryRun reports without deleting.
func (h *StorHub) Prune(ctx context.Context, project string, scope PruneScope, keep int, dryRun bool) (*PruneResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	if h.projectHasUncommittedState(project) {
		return nil, fmt.Errorf("prune refused for project %s: uncommitted metadata changes pending; flush before pruning", project)
	}
	res := &PruneResult{Scope: scope, DryRun: dryRun}
	switch scope {
	case PruneAssets:
		if err := h.pruneAssets(ctx, project, res); err != nil {
			return nil, err
		}
	case PruneObjects:
		if err := h.pruneObjects(ctx, project, res, dryRun); err != nil {
			return nil, err
		}
	case PruneHistory:
		if err := h.pruneHistory(ctx, project, keep, res, dryRun); err != nil {
			return nil, err
		}
	case PruneAll:
		if err := h.pruneHistory(ctx, project, keep, res, dryRun); err != nil {
			return nil, err
		}
		if err := h.pruneObjects(ctx, project, res, dryRun); err != nil {
			return nil, err
		}
		if err := h.pruneAssets(ctx, project, res); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown prune scope %q (want objects|assets|history|all)", scope)
	}
	return res, nil
}

func (h *StorHub) pruneAssets(ctx context.Context, project string, res *PruneResult) error {
	if res.DryRun {
		res.Notes = append(res.Notes, "assets: dry-run does not enumerate release assets; run without --dry-run to see the reclaim")
		return nil
	}
	purged, err := h.PurgeUntrackedContext(ctx, project)
	if err != nil {
		return err
	}
	res.DeletedReleases = purged.DeletedReleases
	res.DeletedAssets = purged.DeletedAssets
	return nil
}

// pruneObjects deletes index objects referenced by no retained manifest.
func (h *StorHub) pruneObjects(ctx context.Context, project string, res *PruneResult, dryRun bool) error {
	pm := h.getOrCreateProjectMeta(project)
	pm.mu.RLock()
	isV2 := pm.isV2
	pm.mu.RUnlock()
	if !isV2 {
		res.Notes = append(res.Notes, "objects: project uses the v1 single-blob layout; no content-addressed objects to prune")
		return nil
	}
	referenced, err := h.referencedObjects(ctx, project)
	if err != nil {
		return err
	}
	all, err := h.listRepoObjects(ctx, project)
	if err != nil {
		return err
	}
	var orphans []objectRef
	for _, obj := range all {
		sha := objectSHAFromPath(obj.path)
		if sha == "" {
			continue
		}
		if !referenced[sha] {
			orphans = append(orphans, obj)
		}
	}
	sort.Slice(orphans, func(i, j int) bool { return orphans[i].path < orphans[j].path })
	res.DeletedObjects = len(orphans)
	if dryRun || len(orphans) == 0 {
		return nil
	}
	if err := h.deleteRepoObjects(ctx, project, orphans); err != nil {
		return err
	}
	// Drop the deleted objects from the local cache so a later load refetches
	// (they are gone upstream).
	cache := h.objectCacheFor(project)
	for _, o := range orphans {
		if sha := objectSHAFromPath(o.path); sha != "" {
			cache.remove(sha)
		}
	}
	logging.Info(h.projectLogger(project), "pruned orphaned index objects", "count", len(orphans))
	return nil
}

// referencedObjects unions every object reachable from any retained manifest
// (the current one plus every historical revision still in git/file history).
func (h *StorHub) referencedObjects(ctx context.Context, project string) (map[string]bool, error) {
	referenced := map[string]bool{}
	revs, err := h.listMetadataRevisions(ctx, project)
	if err != nil {
		return nil, err
	}
	for _, rev := range revs {
		data, isV2, found, rerr := h.readIndexRevision(ctx, project, rev.CommitSHA)
		if rerr != nil || !found || !isV2 {
			continue
		}
		manifest, perr := meta.ParseManifest(data)
		if perr != nil {
			continue
		}
		if err := h.addManifestReachable(ctx, project, manifest, referenced); err != nil {
			return nil, err
		}
	}
	return referenced, nil
}

func (h *StorHub) addManifestReachable(ctx context.Context, project string, m *meta.ManifestV2, out map[string]bool) error {
	var walk func(sha string) error
	walk = func(sha string) error {
		if sha == "" || out[sha] {
			return nil
		}
		out[sha] = true
		data, err := h.fetchObject(ctx, project, sha)
		if err != nil {
			return err
		}
		var node meta.TreeNode
		if err := json.Unmarshal(data, &node); err != nil {
			return err
		}
		for _, child := range node.Subdirs {
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(m.TreeRoot); err != nil {
		return err
	}
	for _, b := range m.ChunkBuckets {
		out[b] = true
	}
	if m.Releases != "" {
		out[m.Releases] = true
	}
	return nil
}

type objectRef struct {
	path    string
	blobSHA string // REST only (contents-API blob oid); empty on git
}

func (h *StorHub) listRepoObjects(ctx context.Context, project string) ([]objectRef, error) {
	if repo := h.getGitRepo(project); repo != nil {
		paths, err := repo.listTreePaths(ctx, ".storhub/objects")
		if err != nil {
			return nil, err
		}
		refs := make([]objectRef, 0, len(paths))
		for _, p := range paths {
			refs = append(refs, objectRef{path: p})
		}
		return refs, nil
	}
	if err := h.ensureOwner(ctx); err != nil {
		return nil, err
	}
	var refs []objectRef
	dirs, err := h.gh.ListDir(ctx, h.owner, project, ".storhub/objects")
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return nil, nil
		}
		return nil, err
	}
	for _, d := range dirs {
		if d.Type != "dir" {
			continue
		}
		files, ferr := h.gh.ListDir(ctx, h.owner, project, d.Path)
		if ferr != nil {
			return nil, ferr
		}
		for _, f := range files {
			if f.Type == "dir" {
				continue
			}
			refs = append(refs, objectRef{path: f.Path, blobSHA: f.SHA})
		}
	}
	return refs, nil
}

func (h *StorHub) deleteRepoObjects(ctx context.Context, project string, orphans []objectRef) error {
	if len(orphans) == 0 {
		return nil
	}
	if repo := h.getGitRepo(project); repo != nil {
		paths := make([]string, 0, len(orphans))
		for _, o := range orphans {
			paths = append(paths, o.path)
		}
		head := repo.headCommitSHA()
		_, err := repo.deleteCommitPushCAS(ctx, paths, "storhub: prune orphaned index objects", head)
		return err
	}
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	for _, o := range orphans {
		if o.blobSHA == "" {
			continue
		}
		if _, err := h.gh.DeleteFileContent(ctx, h.owner, project, o.path, o.blobSHA, "storhub: prune orphaned index object"); err != nil {
			var apiErr *ghapi.APIError
			if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusNotFound || apiErr.StatusCode == http.StatusConflict) {
				continue // already gone or changed under us; a later prune retries
			}
			return fmt.Errorf("delete object %s: %w", o.path, err)
		}
	}
	return nil
}

// pruneHistory compacts old manifests into a checkpoint. Git backend only:
// REST history is GitHub-owned and the contents API cannot delete revisions,
// so we say so rather than pretend to reclaim space we cannot touch.
func (h *StorHub) pruneHistory(ctx context.Context, project string, keep int, res *PruneResult, dryRun bool) error {
	if keep < 1 {
		keep = 1
	}
	repo := h.getGitRepo(project)
	if repo == nil {
		res.Notes = append(res.Notes, "history: REST history is owned by GitHub and the contents API cannot delete revisions; use the git backend to compact history")
		return nil
	}
	revs, err := repo.listFileCommits(ctx, indexFilePath)
	if err != nil {
		return err
	}
	if len(revs) <= keep {
		res.Notes = append(res.Notes, fmt.Sprintf("history: %d manifest commits, at or below keep=%d; nothing to compact", len(revs), keep))
		return nil
	}
	res.HistoryCompacted = true
	if dryRun {
		res.Notes = append(res.Notes, fmt.Sprintf("history: would collapse %d manifest commits to a checkpoint (keep %d)", len(revs), keep))
		return nil
	}
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	head := repo.headCommitSHA()
	if err := repo.squashTreeCAS(ctx, fmt.Sprintf("storhub: prune history (checkpoint, keep %d)", keep), head); err != nil {
		return err
	}
	logging.Info(h.projectLogger(project), "pruned index history to a checkpoint", "revisions", len(revs), "keep", keep)
	return nil
}

// objectSHAFromPath recovers the content address from an object repo path
// (.storhub/objects/<2hex>/<62hex>).
func objectSHAFromPath(path string) string {
	const marker = "/objects/"
	i := strings.LastIndex(path, marker)
	if i < 0 {
		return ""
	}
	rest := path[i+len(marker):] // "<2hex>/<62hex>"
	rest = strings.Replace(rest, "/", "", 1)
	if len(rest) != 64 {
		return ""
	}
	return rest
}
