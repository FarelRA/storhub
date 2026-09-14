package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	ghapi "github.com/FarelRA/storhub/internal/github"
	"github.com/FarelRA/storhub/internal/logging"
)

// Rebase-on-conflict: when the manifest CAS fails (another writer advanced
// the metadata), the pending op stack is replayed onto the upstream state
// instead of discarding local work. Ops are self-contained state
// assertions, so replay is mechanical; conflicts (upstream changed a path
// an op asserts over) resolve per the locked policy: per-path
// last-writer-wins with put-wins-on-delete-vs-put, strict mode opt-in.

// rebaseExhaustedError marks a commit that kept conflicting past the rebase
// attempt budget. Recovery must RETAIN the pending ops for a later retry -
// treating this as a plain 409 would discard exactly the work the rebase
// was trying to save.
type rebaseExhaustedError struct {
	err      error
	attempts int
}

func (e *rebaseExhaustedError) Error() string {
	return fmt.Sprintf("rebase exhausted after %d attempts: %v", e.attempts, e.err)
}
func (e *rebaseExhaustedError) Unwrap() error { return e.err }

// hashEntry fingerprints one metadata entry for change detection. JSON
// marshaling of a struct is field-order deterministic, so the hash is
// stable across processes.
func hashEntry(v any) [16]byte {
	data, err := json.Marshal(v)
	if err != nil {
		return [16]byte{}
	}
	sum := sha256.Sum256(data)
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// hashPaths fingerprints the file/dir namespace (including root) of a
// metadata tree. Keys are prefixed "f:"/"d:" so the namespace is total.
func hashPaths(meta *RepoMetadata) map[string][16]byte {
	out := make(map[string][16]byte, len(meta.Files)+len(meta.Dirs)+1)
	for path, f := range meta.Files {
		out["f:"+path] = hashEntry(f)
	}
	for path, d := range meta.Dirs {
		out["d:"+path] = hashEntry(d)
	}
	out["d:"] = hashEntry(meta.Root)
	return out
}

// changedPaths reports which namespace keys upstream differs from the base
// snapshot the pending ops were built against: changed entries, entries
// upstream added, and entries upstream deleted.
func changedPaths(base map[string][16]byte, upstream *RepoMetadata) map[string]bool {
	current := hashPaths(upstream)
	changed := make(map[string]bool)
	for key, h := range current {
		if bh, ok := base[key]; !ok || bh != h {
			changed[key] = true
		}
	}
	for key := range base {
		if _, ok := current[key]; !ok {
			changed[key] = true
		}
	}
	return changed
}

// opConflictKeys returns the namespace keys an op asserts over.
func opConflictKeys(op Op) []string {
	switch op.Type {
	case OpMkdir, OpRmdir:
		if len(op.Paths) > 0 {
			return []string{"d:" + op.Paths[0]}
		}
	case OpSetattr, OpXattr:
		if len(op.Paths) == 0 {
			return nil
		}
		if op.File != nil {
			return []string{"f:" + op.Paths[0]}
		}
		return []string{"d:" + op.Paths[0]}
	case OpRename:
		if len(op.Paths) != 2 {
			return nil
		}
		p := "f:"
		if op.File == nil {
			p = "d:"
		}
		return []string{p + op.Paths[0], p + op.Paths[1]}
	case OpDeleteFile:
		if len(op.Paths) > 0 {
			return []string{"f:" + op.Paths[0]}
		}
	case OpRelease, OpChunkPrune:
		// Catalog ops apply idempotently/defensively; they do not
		// path-conflict.
		return nil
	default:
		if len(op.Paths) > 0 {
			return []string{"f:" + op.Paths[0]}
		}
	}
	return nil
}

// rebaseWorkingTree replays ops onto upstream, resolving conflicts per the
// policy table. Returns the rebased tree and every resolution made (for
// the commit message and strict-mode reporting).
func rebaseWorkingTree(upstream *RepoMetadata, ops []Op, base map[string][16]byte, strict bool) (*RepoMetadata, []ConflictResolution, error) {
	changed := changedPaths(base, upstream)
	working := upstream.Clone()
	var resolutions []ConflictResolution
	for _, op := range ops {
		conflictPath := ""
		for _, key := range opConflictKeys(op) {
			if changed[key] {
				conflictPath = strings.TrimPrefix(strings.TrimPrefix(key, "f:"), "d:")
				break
			}
		}
		if conflictPath != "" && strict {
			return nil, nil, fmt.Errorf("strict conflict policy: upstream changed %s since our base; refusing to auto-resolve op %d (%s)", conflictPath, op.Seq, op.Type)
		}
		if conflictPath != "" && isDeleteClass(op.Type) {
			// delete vs put: the upstream entry survives (data
			// preservation). A delete of an entry upstream also deleted
			// applies as a harmless no-op instead.
			upstreamHas := false
			for _, key := range opConflictKeys(op) {
				if strings.HasPrefix(key, "f:") {
					if _, ok := working.Files[strings.TrimPrefix(key, "f:")]; ok {
						upstreamHas = true
						break
					}
				} else if strings.HasPrefix(key, "d:") {
					p := strings.TrimPrefix(key, "d:")
					if p == "" || working.HasDirectory(p) {
						upstreamHas = true
						break
					}
				}
			}
			if upstreamHas {
				resolutions = append(resolutions, ConflictResolution{Seq: op.Seq, Path: conflictPath,
					Note: fmt.Sprintf("put wins over our delete (data preservation): %s", conflictPath)})
				continue
			}
		}
		if err := applyOneOp(&working, op, &resolutions); err != nil {
			return nil, nil, err
		}
		if conflictPath != "" {
			resolutions = append(resolutions, ConflictResolution{Seq: op.Seq, Path: conflictPath,
				Note: fmt.Sprintf("LWW %s (our %s wins)", conflictPath, op.Type)})
		}
	}
	working.Normalize(upstream.Project, upstream.LastMod)
	working.RecomputeStats()
	if err := working.Validate(); err != nil {
		return nil, nil, fmt.Errorf("rebased tree failed validation: %w", err)
	}
	return &working, resolutions, nil
}

// maxRebaseNoteBytes bounds the resolution detail carried in a commit
// message so a pathological conflict storm cannot bloat the summary line.
const maxRebaseNoteBytes = 300

// rebaseMessageNote renders the rebase summary appended to a commit
// message that landed after conflict resolution.
func rebaseMessageNote(resolutions []ConflictResolution, upstreamSHA string) string {
	if len(resolutions) == 0 {
		return fmt.Sprintf("rebase: clean onto %s", shortSHA(upstreamSHA))
	}
	notes := make([]string, 0, len(resolutions))
	for _, r := range resolutions {
		notes = append(notes, r.Note)
	}
	joined := strings.Join(notes, "; ")
	if len(joined) > maxRebaseNoteBytes {
		joined = joined[:maxRebaseNoteBytes] + "..."
	}
	return fmt.Sprintf("rebase: rebased onto %s: %d resolved (%s)", shortSHA(upstreamSHA), len(resolutions), joined)
}

// loadUpstreamMetadata fetches the current remote metadata WITHOUT touching
// the project cache: the rebase needs pristine upstream state while the
// cache holds local diverged truth. A missing metadata file (brand-new or
// wiped project) is an empty tree, not an error.
func (h *StorHub) loadUpstreamMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	if err := h.ensureOwner(ctx); err != nil {
		return nil, "", err
	}
	if repo := h.getGitRepo(project); repo != nil {
		data, err := repo.readFileHead(ctx, metadataFilePath)
		if err != nil {
			if isMetadataNotFound(err) {
				return NewRepoMetadata(project), "", nil
			}
			return nil, "", err
		}
		meta := NewRepoMetadata(project)
		if err := meta.FromJSON(data); err != nil {
			return nil, "", fmt.Errorf("parse upstream metadata: %w", err)
		}
		meta.Normalize(project, h.config.Now().Unix())
		if err := meta.Validate(); err != nil {
			return nil, "", fmt.Errorf("validate upstream metadata: %w", err)
		}
		return meta, repo.headCommitSHA(), nil
	}
	data, sha, err := h.gh.GetFileContent(ctx, h.owner, project, metadataFilePath, "")
	if err != nil {
		var apiErr *ghapi.APIError
		if errors.As(err, &apiErr) && apiErr.NotFound() {
			return NewRepoMetadata(project), "", nil
		}
		return nil, "", err
	}
	meta := NewRepoMetadata(project)
	if err := meta.FromJSON(data); err != nil {
		return nil, "", fmt.Errorf("parse upstream metadata: %w", err)
	}
	meta.Normalize(project, h.config.Now().Unix())
	if err := meta.Validate(); err != nil {
		return nil, "", fmt.Errorf("validate upstream metadata: %w", err)
	}
	return meta, sha, nil
}

// maxCommitAttempts bounds the commit/rebase cycle: one initial attempt
// plus two rebases. Persistent conflict past the budget retains the stack
// for a later trigger instead of spinning.
const maxCommitAttempts = 3

// rebaseOntoUpstream is the I/O wrapper around rebaseWorkingTree used by
// the commit loop: fetch upstream, replay, log the outcome.
func (h *StorHub) rebaseOntoUpstream(ctx context.Context, project string, ops []Op, base map[string][16]byte) (*RepoMetadata, string, []ConflictResolution, error) {
	upstream, upstreamSHA, err := h.loadUpstreamMetadata(ctx, project)
	if err != nil {
		return nil, "", nil, fmt.Errorf("fetch upstream for rebase: %w", err)
	}
	rebased, resolutions, err := rebaseWorkingTree(upstream, ops, base, h.config.StrictConflicts)
	if err != nil {
		return nil, "", nil, err
	}
	logging.Info(h.projectLogger(project), "op stack rebased onto upstream",
		"ops", len(ops), "resolutions", len(resolutions), "upstream_sha", shortSHA(upstreamSHA))
	return rebased, upstreamSHA, resolutions, nil
}

// remapOpCollisions rewrites an op's identifiers that collide with diverged
// upstream state before replay: chunk IDs allocated by both writers for
// different records are remapped to fresh IDs (data integrity), and node
// inodes colliding across kinds are remapped (Validate forbids file/dir
// inode collisions and duplicate directory inodes; file/file sharing is
// legal hardlink semantics and stays). Caller provides storage for the
// recorded resolutions.
func remapOpCollisions(meta *RepoMetadata, op *Op, resolutions *[]ConflictResolution) {
	if op.File != nil {
		idRemap := make(map[int64]int64)
		for id, record := range op.Chunks {
			if existing, ok := meta.Chunks[id]; ok && existing != record {
				idRemap[id] = meta.AllocateChunkID()
			}
		}
		if len(idRemap) > 0 {
			newChunks := make(map[int64]ChunkInfo, len(op.Chunks))
			for id, record := range op.Chunks {
				if newID, ok := idRemap[id]; ok {
					newChunks[newID] = record
				} else {
					newChunks[id] = record
				}
			}
			op.Chunks = newChunks
			for i, id := range op.File.Chunks {
				if newID, ok := idRemap[id]; ok {
					op.File.Chunks[i] = newID
				}
			}
			recordResolution(resolutions, *op, opPath(*op),
				"chunk ids remapped (divergent allocation between writers)")
		}
		if op.File.Inode != 0 && inodeCollidesWithDirFamily(meta, op.File.Inode) {
			op.File.Inode = meta.AllocateInode()
			recordResolution(resolutions, *op, opPath(*op),
				"inode remapped (collides with upstream directory inode)")
		}
	}
	if op.Dir != nil && op.Dir.Inode != 0 {
		// A root setattr asserts the SAME root node, not a competing one:
		// its inode matching upstream root is identity, not collision.
		isRootOp := opPath(*op) == "" && op.Type == OpSetattr
		if !isRootOp && inodeTakenByAnyNode(meta, op.Dir.Inode) {
			op.Dir.Inode = meta.AllocateInode()
			recordResolution(resolutions, *op, opPath(*op),
				"directory inode remapped (collides with upstream node)")
		}
	}
}

// inodeCollidesWithDirFamily reports whether inode is taken by the root or
// any directory - the collisions Validate forbids for an incoming FILE
// node. File/file sharing stays legal (hardlinks) and never remaps.
func inodeCollidesWithDirFamily(meta *RepoMetadata, inode uint64) bool {
	if inode == meta.Root.Inode {
		return true
	}
	for _, d := range meta.Dirs {
		if d.Inode == inode {
			return true
		}
	}
	return false
}

// inodeTakenByAnyNode reports whether inode is taken by the root, any
// directory, or any file - everything an incoming DIRECTORY node must
// avoid (duplicate directory inodes and file/dir sharing both fail
// Validate).
func inodeTakenByAnyNode(meta *RepoMetadata, inode uint64) bool {
	if inodeCollidesWithDirFamily(meta, inode) {
		return true
	}
	for _, f := range meta.Files {
		if f.Inode == inode {
			return true
		}
	}
	return false
}
