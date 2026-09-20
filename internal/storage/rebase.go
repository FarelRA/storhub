package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

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
// stable across processes. The ok result is false when the entry cannot
// marshal (these structs always marshal in practice); callers fall back to
// the zero hash, which fails closed by flagging the path changed on one
// side only, never by hiding a change both sides share.
func hashEntry(v any) ([16]byte, bool) {
	data, err := json.Marshal(v)
	if err != nil {
		return [16]byte{}, false
	}
	sum := sha256.Sum256(data)
	var out [16]byte
	copy(out[:], sum[:16])
	return out, true
}

// hashPaths fingerprints the file/dir namespace (including root) of a
// metadata tree. Keys are prefixed "f:"/"d:" so the namespace is total.
func hashPaths(meta *RepoMetadata) map[string][16]byte {
	out := make(map[string][16]byte, len(meta.Files())+len(meta.Dirs())+1)
	for path, f := range meta.Files() {
		h, _ := hashEntry(f)
		out["f:"+path] = h
	}
	for path, d := range meta.Dirs() {
		h, _ := hashEntry(d)
		out["d:"+path] = h
	}
	h, _ := hashEntry(meta.Root)
	out["d:"] = h
	return out
}

// changedByHash reports which namespace keys upstream differs from the base
// snapshot the pending ops were built against: changed entries, entries
// upstream added, and entries upstream deleted. It FINDS conflicts (content
// hash over the rebase baseline); changedByTime then resolves a known
// conflict by timestamp.
func changedByHash(base map[string][16]byte, upstream *RepoMetadata) map[string]bool {
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

// changedPaths is the historical name of changedByHash; prefer changedByHash.
func changedPaths(base map[string][16]byte, upstream *RepoMetadata) map[string]bool {
	return changedByHash(base, upstream)
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
	changed := changedByHash(base, upstream)
	working := upstream.Clone()
	// One shared replay plan for the batch: moves recorded by earlier
	// renames resolve later from-references, and removals skipped by
	// conflict resolution below (plan.unremove) reappear as live children
	// for later rmdirs. One shared collision index likewise: identifier
	// occupancy is snapshotted once and updated incrementally, not
	// rescanned per op.
	plan := newReplayPlan(ops)
	cidx := newCollisionIndex(working)
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
					if _, ok := working.Files()[strings.TrimPrefix(key, "f:")]; ok {
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
				// The skipped removal never lands: drop its paths from the
				// doomed set so a later rmdir sees the survivor again, and
				// from the target set so live occupants count as genuine
				// again for later collision checks.
				plan.unremove(op)
				plan.untarget(op)
				continue
			}
		}
		if conflictPath != "" && isStateClass(op.Type) && changedByTime(working, op) {
			// True last-writer-wins: "we commit later" is not "we
			// wrote later". When the upstream entry changed after our op
			// was recorded, upstream owns the newer write and our stale
			// state-class assertion is dropped (recorded) instead of
			// clobbering it. Its target leaves the target set with it:
			// the overwrite never lands, so occupants there are genuine.
			resolutions = append(resolutions, ConflictResolution{Seq: op.Seq, Path: conflictPath,
				Note: fmt.Sprintf("upstream newer for %s (kept upstream, our stale %s dropped)", conflictPath, op.Type)})
			plan.untarget(op)
			continue
		}
		if conflictPath != "" && op.Type == OpRename && renameSupersededByUpstream(working, op) {
			// Renames overwrite their target unconditionally at apply
			// time, so a stale rename would clobber a newer upstream
			// target with no timestamp check — the rename counterpart of
			// the state-class LWW drop above.
			resolutions = append(resolutions, ConflictResolution{Seq: op.Seq, Path: conflictPath,
				Note: fmt.Sprintf("upstream newer for rename target %s (kept upstream, our stale %s dropped)", op.Paths[1], op.Type)})
			plan.unremove(op)
			plan.untarget(op)
			continue
		}
		if err := applyOneOpIndexed(working, op, plan, &resolutions, cidx); err != nil {
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
	return working, resolutions, nil
}

// opTargetChangedAt returns the newest change time among the entries an op
// asserts over, and whether any such entry exists upstream. It is the shared
// timestamp source for staleness checks: changedByTime (rebase LWW) and
// renameSupersededByUpstream (cold-replay rename guard) both read through it
// so the "what counts as newer" definition cannot drift between the two
// paths. Change time is ChangedAt for files and the later of
// ChangedAt/ModifiedAt for directories. An entry upstream deleted after our
// op is not "newer": recreating it is the data-preserving choice, so a
// missing entry reports ok=false instead of a zero time.
func opTargetChangedAt(meta *RepoMetadata, op Op) (int64, bool) {
	var newest int64
	var found bool
	for _, key := range opConflictKeys(op) {
		switch {
		case strings.HasPrefix(key, "f:"):
			if f, ok := meta.Files()[strings.TrimPrefix(key, "f:")]; ok {
				found = true
				if f.ChangedAt > newest {
					newest = f.ChangedAt
				}
			}
		case key == "d:":
			found = true
			if t := max(meta.Root.ChangedAt, meta.Root.ModifiedAt); t > newest {
				newest = t
			}
		case strings.HasPrefix(key, "d:"):
			if d, ok := meta.Dirs()[strings.TrimPrefix(key, "d:")]; ok {
				found = true
				if t := max(d.ChangedAt, d.ModifiedAt); t > newest {
					newest = t
				}
			}
		}
	}
	return newest, found
}

// changedByTime reports whether any entry the op asserts over changed
// upstream after the op was recorded. The comparison uses the entry's change
// time (see opTargetChangedAt) against the op's timestamp. An entry upstream
// deleted after our op is not "newer": recreating it is the data-preserving
// choice and stays ours.
func changedByTime(working *RepoMetadata, op Op) bool {
	ts, ok := opTargetChangedAt(working, op)
	return ok && ts > op.Timestamp
}

// renameSupersededByUpstream reports whether a rename must not replay over
// current upstream state: its target exists upstream with a change time
// newer than the op. File/dir renames overwrite their target
// unconditionally at apply time, so a stale rename (journaled before a
// crash, rival advanced the target since) would clobber newer remote bytes —
// unlike single-target state ops, which the timestamp guard already drops. A
// missing from-path is NOT superseding: replay is defensive and applies the
// payload literally.
//
// Contract for the cold-replay owner (workflows.go dropSupersededOps,
// ~line 1009): timestamp-check renames instead of keeping them
// unconditionally. Suggested edit (pipeline agent owns that file):
//
//	default: // renames + catalog ops reach here today
//	    if op.Type == OpRename && renameSupersededByUpstream(meta, op) {
//	        logging.Warn(logger, "op journal rename superseded by newer remote state; skipped",
//	            "project", project, "op", op.Type, "to", op.Paths[1], ...)
//	        continue
//	    }
//	    kept = append(kept, op)
//
// From-liveness needs no check: a rename whose source is gone upstream
// replays its payload literally (the dir-rename path writes op.Dir at the
// target even when the source is missing), matching current behavior.
func renameSupersededByUpstream(meta *RepoMetadata, op Op) bool {
	if op.Type != OpRename || len(op.Paths) != 2 {
		return false
	}
	to := op.Paths[1]
	var changedAt int64
	var exists bool
	if op.File != nil {
		if f, ok := meta.Files()[to]; ok {
			changedAt, exists = f.ChangedAt, true
		}
	} else {
		if d, ok := meta.Dirs()[to]; ok {
			changedAt, exists = max(d.ChangedAt, d.ModifiedAt), true
		}
	}
	return exists && changedAt > op.Timestamp
}

// maxRebaseNoteBytes bounds the resolution detail carried in a commit
// message so a pathological conflict storm cannot bloat the summary line.
const maxRebaseNoteBytes = 300

// truncateRuneSafe cuts s to at most maxBytes without splitting a UTF-8
// sequence and emitting invalid bytes: trailing continuation bytes are
// dropped, then a dangling rune start whose tail was cut.
func truncateRuneSafe(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := s[:maxBytes]
	for len(cut) > 0 && !utf8.RuneStart(cut[len(cut)-1]) {
		cut = cut[:len(cut)-1]
	}
	if len(cut) > 0 {
		if r, size := utf8.DecodeRuneInString(cut[len(cut)-1:]); r == utf8.RuneError && size == 1 {
			cut = cut[:len(cut)-1]
		}
	}
	return cut
}

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
		joined = truncateRuneSafe(joined, maxRebaseNoteBytes) + "..."
	}
	return fmt.Sprintf("rebase: rebased onto %s: %d resolved (%s)", shortSHA(upstreamSHA), len(resolutions), joined)
}

// loadUpstreamMetadata fetches the current remote metadata WITHOUT touching
// the project cache: the rebase needs pristine upstream state while the
// cache holds local diverged truth. A missing index (brand-new or wiped
// project) is an empty tree, not an error. The returned sha is the CAS token
// for the upstream layout (manifest blob sha for the split index, metadata
// blob sha for a legacy blob, HEAD commit sha on the git backend).
func (h *StorHub) loadUpstreamMetadata(ctx context.Context, project string) (*RepoMetadata, string, error) {
	data, sha, found, err := h.readIndexHead(ctx, project)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return NewRepoMetadata(project), "", nil
	}
	meta, _, err := h.loadIndexTree(ctx, project, data)
	if err != nil {
		return nil, "", err
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

// remapOpCollisionsIndexed is the batch entry: cidx snapshots identifier
// occupancy once per batch (newCollisionIndex) and is updated incrementally
// as the batch applies, so per-op checks are O(1) instead of O(tree) scans.
// Chunk-ID checks stay live against meta (O(1) map lookups); only the inode
// occupancy reads come from the index.
func remapOpCollisionsIndexed(meta *RepoMetadata, op *Op, plan *replayPlan, resolutions *[]ConflictResolution, cidx *collisionIndex) error {
	if op.File != nil {
		idRemap := make(map[int64]int64)
		for id, record := range op.Chunks {
			if existing, ok := meta.Chunks()[id]; ok && existing != record {
				fresh, err := plan.allocChunkAvoiding(meta)
				if err != nil {
					return err
				}
				idRemap[id] = fresh
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
		if op.File.Inode != 0 && cidx.fileCollidesWithLiveDir(op.File.Inode, plan, opPath(*op)) {
			fresh, err := plan.allocInodeAvoiding(meta)
			if err != nil {
				return err
			}
			op.File.Inode = fresh
			recordResolution(resolutions, *op, opPath(*op),
				"inode remapped (collides with upstream directory inode)")
		}
	}
	if op.Dir != nil && op.Dir.Inode != 0 {
		// A root setattr asserts the SAME root node, not a competing one:
		// its inode matching upstream root is identity, not collision.
		isRootOp := opPath(*op) == "" && op.Type == OpSetattr
		// A rename replays its own entry mid-move: the from-path holding
		// the payload's inode in the target is the SAME record, not a
		// competing allocation - and so is its forward-resolved location
		// when an earlier rename in this batch already relocated the
		// subtree there. Without this exemption every rename replay
		// renumbers its own entry - differently per application order -
		// defeating rename identity AND order-independent replay. A
		// genuinely divergent upstream record at either path is already
		// surfaced as a rebase conflict before replay begins. The same
		// holds for any dir op at its own target path: a setattr, mkdir
		// or put asserting an entry whose path already holds that inode
		// is updating that same record in place (a competing allocation
		// would be a different inode, and a genuinely divergent upstream
		// record is a rebase conflict, not a replay collision).
		except := map[string]struct{}{}
		if len(op.Paths) > 0 {
			except[op.Paths[0]] = struct{}{}
			if op.Type == OpRename && plan != nil {
				// The entry may already live at its batch-forwarded
				// location when an earlier rename relocated the subtree.
				except[plan.translateForward(op.Paths[0])] = struct{}{}
			}
		}
		// Batch-asserted paths join the exemption via the targets-aware
		// check below (not by copying the set here): the batch overwrites
		// them with its own records in every delivery order, so a live
		// occupant is replay scaffolding (an ensureParentFor mint for a
		// not-yet-replayed target), not a divergent allocation. Genuine
		// upstream occupants live off-target and still remap.
		var targets map[string]struct{}
		if plan != nil {
			targets = plan.targets
		}
		if !isRootOp && cidx.takenByAnotherNodeExceptTargets(op.Dir.Inode, except, targets) {
			fresh, err := plan.allocInodeAvoiding(meta)
			if err != nil {
				return err
			}
			op.Dir.Inode = fresh
			recordResolution(resolutions, *op, opPath(*op),
				"directory inode remapped (collides with upstream node)")
		}
	}
	return nil
}
