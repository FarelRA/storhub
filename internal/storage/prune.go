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
//	assets    unreferenced release assets (the assets scope below).
//	          Both backends.
//	history   collapse every manifest older than the checkpoint into ONE
//	          commit (git backend only): keep is a threshold (compact only
//	          when history exceeds it), not a retention count; exactly one
//	          checkpoint survives, so keep > 1 is rejected rather than
//	          silently destroying the retention it implies. On REST,
//	          history is GitHub-owned and the contents API cannot delete
//	          revisions, so prune reports that honestly instead of
//	          pretending.
//	all       history (where possible) + objects + assets.

// PruneScope selects what a prune run reclaims.
type PruneScope string

const (
	PruneObjects PruneScope = "objects"
	PruneAssets  PruneScope = "assets"
	PruneHistory PruneScope = "history"
	// PruneChunks collects orphaned chunk records from the live catalog:
	// records no file and no pending edit references. Unlike the other
	// scopes it works on live (possibly dirty) state by construction —
	// its roots include the pending op stack — so it skips the
	// flush-first gate below. Refuses while any session holds the
	// project; nothing runs automatically.
	PruneChunks PruneScope = "chunks"
	PruneAll    PruneScope = "all"
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
	// Chunk-GC tallies, populated by the chunks scope only.
	ScannedChunks   int   `json:"scanned_chunks,omitempty"`
	OrphanChunks    int   `json:"orphan_chunks,omitempty"`
	OrphanBytes     int64 `json:"orphan_bytes,omitempty"`
	CollectedChunks int   `json:"collected_chunks,omitempty"`
	CollectedBytes  int64 `json:"collected_bytes,omitempty"`
}

// PruneProject is the context-free CLI/embedder entry point: scope is one of
// "objects", "assets", "history", or "all".
func (h *StorHub) PruneProject(project, scope string, keep int, dryRun bool) (*PruneResult, error) {
	return h.PruneContext(context.Background(), project, scope, keep, dryRun)
}

// PruneContext is the string-scoped entry point used by the REST layer (scope
// is "objects"|"assets"|"history"|"chunks"|"all"); it adapts to the typed Prune.
func (h *StorHub) PruneContext(ctx context.Context, project, scope string, keep int, dryRun bool) (*PruneResult, error) {
	return h.Prune(ctx, project, PruneScope(scope), keep, dryRun)
}

// PruneRequest is the flag-free prune invocation: scope selects what to
// reclaim, keep is the history-compaction threshold (git only; keep > 1 is
// rejected), dryRun reports without deleting. It replaces the
// boolean-flag Prune(..., keep, dryRun) 4-way switch with per-scope
// methods sharing one validation front.
type PruneRequest struct {
	Scope  PruneScope
	Keep   int
	DryRun bool
}

// PruneReq runs a PruneRequest. Prune/PruneContext/PruneProject are thin
// public-compat wrappers over it.
func (h *StorHub) PruneReq(ctx context.Context, project string, req PruneRequest) (*PruneResult, error) {
	if err := validateProject(project); err != nil {
		return nil, err
	}
	res := &PruneResult{Scope: req.Scope, DryRun: req.DryRun}
	// The chunks scope works on live state (its roots include the pending
	// op stack) and skips the flush-first gate: collecting catalog
	// orphans is safe exactly when the tree is dirty, which is when the
	// other scopes must refuse. Every other scope classifies against
	// committed state and refuses a dirty tree.
	if req.Scope == PruneChunks {
		return res, h.pruneChunks(ctx, project, res)
	}
	if h.projectHasUncommittedState(project) {
		return nil, fmt.Errorf("prune refused for project %s: uncommitted metadata changes pending; flush before pruning", project)
	}
	switch req.Scope {
	case PruneAssets:
		if err := h.pruneAssets(ctx, project, res); err != nil {
			return nil, err
		}
	case PruneObjects:
		if err := h.pruneObjects(ctx, project, res, req.DryRun); err != nil {
			return nil, err
		}
	case PruneHistory:
		if err := h.pruneHistory(ctx, project, req.Keep, res, req.DryRun); err != nil {
			return nil, err
		}
	case PruneAll:
		if err := h.pruneHistory(ctx, project, req.Keep, res, req.DryRun); err != nil {
			return nil, err
		}
		if err := h.pruneObjects(ctx, project, res, req.DryRun); err != nil {
			return nil, err
		}
		if err := h.pruneAssets(ctx, project, res); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown prune scope %q (want objects|assets|history|chunks|all)", req.Scope)
	}
	return res, nil
}

// Prune runs the requested scope. keep is a history-compaction threshold
// (git): compaction runs only when manifest commits exceed keep, and it
// collapses every older manifest into ONE checkpoint commit, so exactly one
// revision survives; keep > 1 is rejected because it would promise a
// retention the checkpoint cannot provide. dryRun reports without deleting.
func (h *StorHub) Prune(ctx context.Context, project string, scope PruneScope, keep int, dryRun bool) (*PruneResult, error) {
	return h.PruneReq(ctx, project, PruneRequest{Scope: scope, Keep: keep, DryRun: dryRun})
}

func (h *StorHub) pruneAssets(ctx context.Context, project string, res *PruneResult) error {
	if res.DryRun {
		// Dry-run reports the would-delete counts (PruneResult contract:
		// "what a prune did (or would do, under DryRun)"), like
		// pruneObjects reports len(orphans). Classification is
		// side-effect free; nothing is deleted.
		releaseTasks, assetTasks, err := h.classifyUntracked(ctx, project)
		if err != nil {
			return err
		}
		res.DeletedReleases = len(releaseTasks)
		res.DeletedAssets = len(assetTasks)
		if len(releaseTasks)+len(assetTasks) == 0 {
			res.Notes = append(res.Notes, "assets: dry-run found nothing to reclaim")
		} else {
			res.Notes = append(res.Notes, fmt.Sprintf("assets: dry-run would delete %d releases and %d assets", len(releaseTasks), len(assetTasks)))
		}
		return nil
	}
	return pruneAssetsLive(h, ctx, project, res)
}

// pruneChunks is the chunks scope: orphaned chunk records from the live
// catalog, via the chunk-GC engine (cleanup.go). Dry-run scans only;
// a live run collects, refusing while any session holds the project.
// It runs on live state by design (see PruneReq), so it never joins
// PruneAll: `all` classifies committed state, chunks classifies live
// state, and mixing the two rules in one run would be dishonest.
func (h *StorHub) pruneChunks(ctx context.Context, project string, res *PruneResult) error {
	gc, err := h.ScanChunkGC(ctx, project)
	if err != nil {
		return err
	}
	res.ScannedChunks = gc.ScannedChunks
	res.OrphanChunks = gc.OrphanChunks
	res.OrphanBytes = gc.OrphanBytes
	if res.DryRun {
		return nil
	}
	got, err := h.CompactOrphanChunks(ctx, project, false)
	if err != nil {
		return err
	}
	res.CollectedChunks = got.CollectedChunks
	res.CollectedBytes = got.CollectedBytes
	return nil
}

// pruneObjects deletes index objects referenced by no retained manifest.
func (h *StorHub) pruneObjects(ctx context.Context, project string, res *PruneResult, dryRun bool) error {
	// Detect the layout from actual HEAD, not the (possibly uninitialized)
	// cache: a legacy single-blob project has no objects to prune.
	headData, _, headFound, err := h.readIndexHead(ctx, project)
	if err != nil {
		return err
	}
	if !headFound {
		res.Notes = append(res.Notes, "objects: project has no index yet (uninitialized); nothing to prune")
		return nil
	}
	if !meta.IsManifest(headData) {
		res.Notes = append(res.Notes, "objects: project still uses the legacy single-blob layout; no content-addressed objects to prune (it migrates on its next write)")
		return nil
	}
	referenced, err := h.referencedObjects(ctx, project, headData)
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
	// Drop each deleted object from the local cache the moment its upstream
	// delete succeeds (inside deleteRepoObjects), independent of whether a
	// later delete fails: a stale cached sha would make a future commit
	// skip re-uploading bytes that no longer exist upstream.
	cache := h.objectCacheFor(project)
	if err := h.deleteRepoObjects(ctx, project, orphans, cache); err != nil {
		return err
	}
	logging.Info(h.projectLogger(project), "pruned orphaned index objects", "count", len(orphans))
	return nil
}

// referencedObjects unions every object reachable from any retained manifest
// (the current one plus every historical revision still in git/file history).
// Every failure to read or parse a revision is fatal: a revision that cannot
// be classified must abort the prune, never be skipped — silently dropping
// one revision from the union would orphan (and let prune delete) the objects
// only it references, including the live tree if the skipped read was HEAD.
func (h *StorHub) referencedObjects(ctx context.Context, project string, headData []byte) (map[string]bool, error) {
	referenced := map[string]bool{}
	// Seed the union from the HEAD manifest the caller already read: the
	// live tree can never be orphaned by a revision-walk hiccup.
	head, err := meta.ParseManifest(headData)
	if err != nil {
		return nil, fmt.Errorf("parse current manifest: %w", err)
	}
	if err := h.addManifestReachable(ctx, project, head, referenced); err != nil {
		return nil, err
	}
	revs, err := h.listMetadataRevisions(ctx, project)
	if err != nil {
		return nil, err
	}
	if len(revs) == 0 {
		return nil, fmt.Errorf("enumerate manifest revisions for %s: no history found for a split-layout project; refusing to prune (an empty union would orphan every object)", project)
	}
	for _, rev := range revs {
		data, found, rerr := h.readIndexRevision(ctx, project, rev.CommitSHA)
		if rerr != nil {
			return nil, fmt.Errorf("read manifest revision %s: %w (prune aborted: an unreadable revision cannot be classified)", shortSHA(rev.CommitSHA), rerr)
		}
		if !found || !meta.IsManifest(data) {
			continue // not a split-era manifest revision (legacy blob or vanished)
		}
		manifest, perr := meta.ParseManifest(data)
		if perr != nil {
			return nil, fmt.Errorf("parse manifest revision %s: %w (prune aborted)", shortSHA(rev.CommitSHA), perr)
		}
		if err := h.addManifestReachable(ctx, project, manifest, referenced); err != nil {
			return nil, err
		}
	}
	return referenced, nil
}

func (h *StorHub) addManifestReachable(ctx context.Context, project string, m *meta.Manifest, out map[string]bool) error {
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

// contentsListingCap is GitHub's hard cap on a contents-API directory
// listing: larger directories are truncated or rejected outright. Prune
// must never classify reachability from a possibly truncated enumeration —
// an object the cap hid would be deleted as an orphan. This equals
// releaseAssetCap numerically but is a different ceiling (API listing page
// vs release assets), so it keeps its own name on purpose.
const contentsListingCap = 1000

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
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusForbidden {
			return nil, fmt.Errorf("enumerate objects for %s: GitHub refuses oversized contents listings (the API caps a directory at %d entries); this project is too large to prune over REST, use the git backend: %w", project, contentsListingCap, err)
		}
		return nil, err
	}
	if len(dirs) >= contentsListingCap {
		return nil, fmt.Errorf("enumerate objects for %s: the objects root listing returned %d entries, at GitHub's contents-API cap; the enumeration may be truncated and prune refuses to delete from a partial union", project, len(dirs))
	}
	for _, d := range dirs {
		if d.Type != "dir" {
			continue
		}
		files, ferr := h.gh.ListDir(ctx, h.owner, project, d.Path)
		if ferr != nil {
			return nil, fmt.Errorf("enumerate objects under %s: %w", d.Path, ferr)
		}
		if len(files) >= contentsListingCap {
			return nil, fmt.Errorf("enumerate objects under %s: listing returned %d entries, at GitHub's contents-API cap; the enumeration may be truncated and prune refuses to delete from a partial union", d.Path, len(files))
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

// deleteRepoObjects removes the orphan set upstream. Every object whose
// delete succeeds (or is proven already gone) is dropped from the local
// cache immediately, so a mid-loop failure can never leave deleted shas
// cached: a stale cache entry makes a later commit skip re-uploading bytes
// that no longer exist upstream, and the manifest would then reference
// absent objects.
func (h *StorHub) deleteRepoObjects(ctx context.Context, project string, orphans []objectRef, cache *objectCache) error {
	if len(orphans) == 0 {
		return nil
	}
	drop := func(path string) {
		if cache == nil {
			return
		}
		if sha := objectSHAFromPath(path); sha != "" {
			cache.remove(sha)
		}
	}
	if repo := h.getGitRepo(project); repo != nil {
		paths := make([]string, 0, len(orphans))
		for _, o := range orphans {
			paths = append(paths, o.path)
		}
		head := repo.headCommitSHA()
		if _, err := repo.deleteCommitPushCAS(ctx, paths, "storhub: prune orphaned index objects", head); err != nil {
			return err
		}
		for _, o := range orphans {
			drop(o.path)
		}
		return nil
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
				// 404: already gone upstream. 409: the path no longer holds
				// the blob we meant to delete. Either way the cached bytes
				// are not what upstream has, so drop the entry and let a
				// later prune retry.
				drop(o.path)
				continue
			}
			return fmt.Errorf("delete object %s: %w", o.path, err)
		}
		drop(o.path)
	}
	return nil
}

// pruneHistory compacts old manifests into a checkpoint. Git backend only:
// REST history is GitHub-owned and the contents API cannot delete revisions,
// so we say so rather than pretend to reclaim space we cannot touch.
//
// The checkpoint is a single orphan commit carrying the current tree, so
// exactly one revision survives: keep is a threshold that gates whether
// compaction runs at all, never a number of revisions retained. keep > 1
// would promise retention the squash cannot deliver, so it is rejected.
func (h *StorHub) pruneHistory(ctx context.Context, project string, keep int, res *PruneResult, dryRun bool) error {
	if keep > 1 {
		return fmt.Errorf("prune history: keep=%d is not supported: compaction collapses all but the newest checkpoint into a single commit, so exactly one revision survives; use keep=1", keep)
	}
	if keep < 1 {
		keep = 1 // "keep nothing" is unrepresentable: the checkpoint must retain the current tree
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
	if dryRun {
		res.Notes = append(res.Notes, fmt.Sprintf("history: would collapse %d manifest commits to a single checkpoint (keep %d is a threshold, not a retention count)", len(revs), keep))
		return nil
	}
	if err := h.ensureOwner(ctx); err != nil {
		return err
	}
	head := repo.headCommitSHA()
	if err := repo.squashTreeCAS(ctx, fmt.Sprintf("storhub: prune history (checkpoint, keep %d)", keep), head); err != nil {
		return err
	}
	res.HistoryCompacted = true
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
