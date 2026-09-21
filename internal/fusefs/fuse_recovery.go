package fusefs

import (
	"context"
	"encoding/json"
	"fmt"
	shfs "github.com/FarelRA/storhub/internal/fs"
	"github.com/FarelRA/storhub/internal/logging"
	"hash/fnv"
	"log/slog"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

func (s *Filesystem) recoveryDir() string {
	return path.Join(s.cacheDir, "recovery")
}

// RecoveryEntry describes one quarantined overlay: uncommitted bytes that
// survived a commit failure or a crashed mount. The manifest sidecar
// (<saved>.json) records the original target path so the data can be
// replayed or manually recovered; without it the bytes are anonymous.
// FullImage + Fingerprint make an entry eligible for startup auto-redrive:
// a temp proven to hold the complete file image whose target still matches
// the recorded fingerprint is re-uploaded automatically. Anything else
// (range fragments, unknown provenance, changed targets) stays for manual
// recovery: redriving a partial temp as a full file, or over a changed
// target, would destroy data instead of rescuing it.
type RecoveryEntry struct {
	SavedPath  string `json:"saved_path"`
	OrigTemp   string `json:"orig_temp"`
	TargetPath string `json:"target_path,omitempty"`
	Reason     string `json:"reason"`
	Size       int64  `json:"size"`
	PID        int    `json:"pid"`
	CreatedAt  int64  `json:"created_unix_nano"`
	// FullImage reports the temp holds the complete file image
	// (tempAuthoritative or dirty ranges covering [0, logicalSize) at
	// quarantine time). Only full images are auto-redrive candidates.
	FullImage bool `json:"full_image,omitempty"`
	// Ranges records the dirty spans covered, for manual recovery
	// context on range fragments (which are never auto-redriven).
	Ranges [][2]int64 `json:"ranges,omitempty"`
	// BaseSize/LogicalSize are the overlay's sizes at quarantine time,
	// for manual recovery context.
	BaseSize    int64 `json:"base_size,omitempty"`
	LogicalSize int64 `json:"logical_size,omitempty"`
	// Pending records a metadata patch (chmod/chown/utimes) that was
	// staged alongside the data commit and never landed. Redrive applies
	// it after the data, under the same fingerprint guard.
	Pending *shfs.MetadataPatch `json:"pending,omitempty"`
	// Fingerprint pins the target's identity at quarantine time. The
	// redrive compares it against the live target and proceeds only on
	// an exact match: any concurrent modification refuses loudly and
	// the entry stays quarantined.
	Fingerprint *targetFingerprint `json:"fingerprint,omitempty"`
}

// targetFingerprint is the compare-and-swap token for auto-redrive: every
// content mutation stamps ChangedAt (and replace-family ops reassign the
// inode), so an exact match proves the target is untouched since quarantine.
// Same-second same-size rewrites can theoretically slip (mtime granularity);
// the redrive logs loudly so even that case is auditable.
type targetFingerprint struct {
	Size       int64  `json:"size"`
	Inode      uint64 `json:"inode"`
	ModifiedAt int64  `json:"modified_at"`
	ChangedAt  int64  `json:"changed_at"`
}

// quarantineIntent carries what quarantineIntoDir records in the sidecar
// beyond the payload itself. Zero value = unknown provenance: manual
// recovery only, never auto-redrive.
type quarantineIntent struct {
	fullImage   bool
	ranges      [][2]int64
	baseSize    int64
	logicalSize int64
	pending     shfs.MetadataPatch
	hasPending  bool
	fingerprint *targetFingerprint
}

// quarantineIntoDir persists tempPath into recoveryDir crash-safely: the
// file is fsynced before the rename, the manifest is written atomically
// (temp + fsync + rename), and the directory itself is fsynced after, so
// a crash at any point leaves either the old state or the complete new
// state - never a renamed file with lost bytes or a manifest without data.
// It returns the saved data path, or "" when nothing could be preserved
// (failures are logged, never fatal: the leftover stays for the next sweep).
func quarantineIntoDir(tempPath, recoveryDir, targetPath, reason string, logger *slog.Logger) string {
	return quarantineIntoDirWithIntent(tempPath, recoveryDir, targetPath, reason, quarantineIntent{}, logger)
}

// quarantineIntoDirWithIntent is quarantineIntoDir plus the redrive intent
// for the sidecar: whether the temp is a proven full image, the dirty
// spans and sizes for manual context, and the target fingerprint for the
// startup compare-and-swap. A zero intent records unknown provenance:
// manual recovery only, never auto-redrive.
func quarantineIntoDirWithIntent(tempPath, recoveryDir, targetPath, reason string, intent quarantineIntent, logger *slog.Logger) string {
	if logger == nil {
		logger = slog.Default()
	}
	info, err := os.Stat(tempPath)
	if err != nil {
		logging.Error(logger, "quarantine skipped; leftover vanished", "path", tempPath, "err", err)
		return ""
	}
	if err := os.MkdirAll(recoveryDir, 0o700); err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	// Flush the payload before the rename moves it: otherwise a crash
	// between rename and page writeback loses acknowledged writes.
	if f, err := os.OpenFile(tempPath, os.O_RDWR, 0o600); err == nil {
		syncErr := f.Sync()
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil {
			return failQuarantine(logger, tempPath, "syncErr", syncErr, "closeErr", closeErr)
		}
	} else {
		return failQuarantine(logger, tempPath, "err", err)
	}
	stamp := time.Now().UnixNano()
	target := path.Join(recoveryDir, fmt.Sprintf("%s.%d", path.Base(tempPath), stamp))
	entry := RecoveryEntry{
		SavedPath:   target,
		OrigTemp:    tempPath,
		TargetPath:  targetPath,
		Reason:      reason,
		Size:        info.Size(),
		PID:         os.Getpid(),
		CreatedAt:   stamp,
		FullImage:   intent.fullImage,
		Ranges:      intent.ranges,
		BaseSize:    intent.baseSize,
		LogicalSize: intent.logicalSize,
		Fingerprint: intent.fingerprint,
	}
	if intent.hasPending {
		pending := intent.pending
		entry.Pending = &pending
	}
	manifest, err := json.Marshal(entry)
	if err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	manifestTmp, err := os.CreateTemp(recoveryDir, ".manifest*.tmp")
	if err != nil {
		return failQuarantine(logger, tempPath, "err", err)
	}
	manifestTmpName := manifestTmp.Name()
	if _, err := manifestTmp.Write(append(manifest, '\n')); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := manifestTmp.Sync(); err != nil {
		_ = manifestTmp.Close()
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := manifestTmp.Close(); err != nil {
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := os.Rename(manifestTmpName, target+".json"); err != nil {
		_ = os.Remove(manifestTmpName)
		return failQuarantine(logger, tempPath, "err", err)
	}
	if err := os.Rename(tempPath, target); err != nil {
		// The manifest is already durable; remove it so a manifest
		// never points at data that did not arrive.
		_ = os.Remove(target + ".json")
		return failQuarantine(logger, tempPath, "err", err)
	}
	syncDir(recoveryDir)
	logging.Warn(logger, "quarantined dirty overlay for manual recovery", "path", tempPath, "saved", target, "reason", reason)
	return target
}

// failQuarantine logs a quarantine failure (the dirty overlay stays in
// the cache for the next sweep) and returns "".
func failQuarantine(logger *slog.Logger, tempPath string, args ...any) string {
	args = append([]any{"path", tempPath}, args...)
	logging.Error(logger, "quarantine failed; dirty overlay left in cache", args...)
	return ""
}

// syncDir fsyncs a directory so preceding renames inside it survive a
// crash. Best-effort: the quarantine above is already complete without it.
func syncDir(dir string) {
	f, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = f.Sync()
	_ = f.Close()
}

// quarantinePath moves tempPath into <cacheDir>/recovery without needing
// a Filesystem (used by the startup sweep before one exists). Failures
// are logged, never fatal: the leftover stays for the next sweep.
func quarantinePath(tempPath string, logger *slog.Logger) {
	dir := path.Join(path.Dir(tempPath), "recovery")
	quarantineIntoDir(tempPath, dir, "", quarantineReasonStartupSweep, logger)
}

// quarantineFile moves tempPath into the recovery directory, which the
// mount-start sweep preserves. Called when a commit fails and the overlay
// holds the only copy of data the application has already written.
// targetPath is the file the bytes belong to ("" when unknown); reason
// records which path triggered the quarantine.
func (s *Filesystem) quarantineFile(tempPath, targetPath, reason string, intent quarantineIntent) {
	if reason == "" {
		reason = quarantineReasonCommitFailure
	}
	saved := quarantineIntoDirWithIntent(tempPath, s.recoveryDir(), targetPath, reason, intent, s.opts.Logger)
	if saved == "" {
		s.errorf("quarantine failed; dirty overlay left in cache path=%s", tempPath)
		return
	}
	s.errorf("commit failed; dirty overlay quarantined for manual recovery path=%s saved=%s", tempPath, saved)
}

// RecoveryInventory replays the recovery directory: every quarantined
// overlay from earlier commit failures or crash sweeps, newest last.
// Manifest-less files (pre-hardening leftovers, manual drops) are reported
// with best-effort stat so nothing is silently hidden.
func (s *Filesystem) RecoveryInventory() ([]RecoveryEntry, error) {
	return readRecoveryInventory(s.recoveryDir())
}

func readRecoveryInventory(recoveryDir string) ([]RecoveryEntry, error) {
	entries, err := os.ReadDir(recoveryDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []RecoveryEntry
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".manifest-") {
			continue
		}
		full := path.Join(recoveryDir, name)
		if raw, err := os.ReadFile(full + ".json"); err == nil {
			var entry RecoveryEntry
			if err := json.Unmarshal(raw, &entry); err == nil && entry.SavedPath != "" {
				entry.SavedPath = full
				out = append(out, entry)
				continue
			}
		}
		var size int64
		if info, err := e.Info(); err == nil {
			size = info.Size()
		}
		out = append(out, RecoveryEntry{SavedPath: full, OrigTemp: name, Reason: "unknown", Size: size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SavedPath < out[j].SavedPath })
	return out, nil
}

// redriveRecoveryInventory auto-redrives eligible quarantined overlays:
// entries whose target still matches the recorded fingerprint are
// re-uploaded (full images replace, recorded spans patch, bare size
// changes truncate; staged metadata patches apply after the data), then
// committed via the hub's normal path, verified, and only then removed
// from recovery/. Replace journals the op like any acknowledged write,
// so crash-durability past this point is the standard journal story.
// Anything else is kept with a loud warning: range fragments without a
// fingerprint, unknown provenance, changed targets, and any
// upload/commit error. A failed redrive never fails the mount and
// never deletes quarantine data; the entry simply waits for the next
// mount or manual recovery.
func redriveRecoveryInventory(ctx context.Context, hub Hub, project, recoveryDir string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	inventory, err := readRecoveryInventory(recoveryDir)
	if err != nil {
		logging.Error(logger, "recovery redrive scan failed", "dir", recoveryDir, "err", err)
		return
	}
	for _, entry := range inventory {
		redriveRecoveryEntry(ctx, hub, project, entry, logger)
	}
}

// casStripeCount shards redrive compare-and-swap serialization across
// hash buckets keyed by (project, target). Two redrives of the same target
// serialize on one stripe; redrives of different targets proceed in
// parallel. This is deliberately NOT a mount-global lock: a single global
// mutex would serialize every redrive behind the slowest network write.
const casStripeCount = 64

// casStripes owns the redrive CAS buckets. Package-level (not per-mount)
// so concurrent mounts over one cache directory still serialize on the
// same target instead of each passing the fingerprint check and then
// overwriting each other.
var casStripes [casStripeCount]sync.Mutex

// casStripeFor resolves the serialization bucket for one redrive target.
func casStripeFor(project, target string) *sync.Mutex {
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(project))
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write([]byte(target))
	return &casStripes[sum.Sum64()%casStripeCount]
}

func redriveRecoveryEntry(ctx context.Context, hub Hub, project string, entry RecoveryEntry, logger *slog.Logger) {
	target, saved := entry.TargetPath, entry.SavedPath
	if target == "" || entry.Fingerprint == nil {
		return // manual recovery only; inventoried (not silent) by the caller
	}
	// Per-target CAS lock across the fingerprint check plus the write
	// plus the verification: without it two redrives of one target (or a
	// redrive racing a live writer) can both pass the stat check and then
	// land out of order, resurrecting stale bytes over newer ones.
	stripe := casStripeFor(project, target)
	stripe.Lock()
	defer stripe.Unlock()
	live, err := hub.StatPathContext(ctx, project, target)
	if err != nil || live == nil {
		logging.Warn(logger, "recovery redrive refused: target unreadable, keeping quarantine", "target", target, "saved", saved)
		return
	}
	fp := entry.Fingerprint
	if live.Size != fp.Size || live.Inode != fp.Inode || live.ModifiedAt != fp.ModifiedAt || live.ChangedAt != fp.ChangedAt {
		logging.Warn(logger, "recovery redrive refused: target changed since quarantine, keeping quarantine",
			"target", target, "saved", saved, "live_size", live.Size, "live_inode", live.Inode)
		return
	}
	// The fingerprint matched: the target is exactly what quarantine saw.
	// Redrive by temp kind. A full image replaces the file; recorded spans
	// patch it; a bare size change truncates it. Anything undescribed stays
	// manual: redriving bytes the sidecar cannot account for would invent
	// content.
	switch {
	case entry.FullImage:
		if _, err := hub.ReplaceFileContext(ctx, project, target, saved); err != nil {
			logging.Warn(logger, "recovery redrive replace failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	case len(entry.Ranges) > 0:
		if !redriveRanges(ctx, hub, project, entry, logger) {
			return
		}
	case entry.LogicalSize != entry.BaseSize:
		if _, err := hub.TruncateFileContext(ctx, project, target, entry.LogicalSize); err != nil {
			logging.Warn(logger, "recovery redrive truncate failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
		if _, err := hub.StatPathContext(ctx, project, target); err != nil {
			logging.Warn(logger, "recovery redrive truncate left no target, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	default:
		return // nothing described: manual recovery only
	}
	if entry.Pending != nil {
		if err := hub.ApplyMetadataPatchContext(ctx, project, target, *entry.Pending); err != nil {
			// Data landed but the metadata patch did not: keep the entry
			// (with its payload) so the next mount retries the patch
			// instead of declaring victory on half-applied state.
			logging.Warn(logger, "recovery redrive data landed but metadata patch failed, keeping quarantine", "target", target, "saved", saved, "err", err)
			return
		}
	}
	// Verify before deleting: the redrive must be observable, or the
	// quarantine data (the only copy) stays.
	after, err := hub.StatPathContext(ctx, project, target)
	if err != nil || after == nil {
		logging.Warn(logger, "recovery redrive verify failed, keeping quarantine", "target", target, "saved", saved, "err", err)
		return
	}
	if err := os.Remove(saved); err != nil && !os.IsNotExist(err) {
		logging.Warn(logger, "recovery redrive cleanup failed (data is committed; remove manually)", "target", target, "saved", saved, "err", err)
		return
	}
	if err := os.Remove(saved + ".json"); err != nil && !os.IsNotExist(err) {
		// The payload is gone but its sidecar survived: the next mount
		// inventories a manifest without data (kept, warned, never
		// redriven: the redrive requires the payload to exist).
		// Loud here so the orphan is removed, not wondered at.
		logging.Warn(logger, "recovery redrive sidecar cleanup failed (payload committed; remove sidecar manually)", "target", target, "saved", saved+".json", "err", err)
		return
	}
	logging.Warn(logger, "recovery redrive committed quarantined overlay", "target", target)
}

// redriveRanges replays recorded dirty spans from the saved temp through
// the patch verb: each span's bytes are read from the temp at the recorded
// offsets and patched over the same offsets. Reads use ReadAt per span so
// a huge temp never loads fully into memory for a small dirty set. Spans
// outside the temp file refuse the whole entry (a truncated temp must
// never redrive partial ranges). Reports whether the caller may proceed
// to verification.
func redriveRanges(ctx context.Context, hub Hub, project string, entry RecoveryEntry, logger *slog.Logger) bool {
	target, saved := entry.TargetPath, entry.SavedPath
	f, err := os.Open(saved)
	if err != nil {
		logging.Warn(logger, "recovery redrive refused: payload unreadable, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	defer func() { _ = f.Close() }()
	var size int64
	if info, err := f.Stat(); err == nil {
		size = info.Size()
	} else {
		logging.Warn(logger, "recovery redrive refused: payload unstatable, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	edits := make([]shfs.RangeEdit, 0, len(entry.Ranges))
	for _, span := range entry.Ranges {
		start, end := span[0], span[1]
		if start < 0 || end < start || end > size {
			logging.Warn(logger, "recovery redrive refused: span outside payload, keeping quarantine",
				"target", target, "saved", saved, "span", span)
			return false
		}
		edit := make([]byte, end-start)
		if _, err := f.ReadAt(edit, start); err != nil {
			logging.Warn(logger, "recovery redrive refused: span unreadable, keeping quarantine",
				"target", target, "saved", saved, "span", span, "err", err)
			return false
		}
		edits = append(edits, shfs.RangeEdit{Start: start, DeleteSize: end - start, Data: edit})
	}
	if _, err := hub.PatchFileRangesContext(ctx, project, target, edits); err != nil {
		logging.Warn(logger, "recovery redrive patch failed, keeping quarantine", "target", target, "saved", saved, "err", err)
		return false
	}
	return true
}

// logRecoveryInventory replays quarantined state at startup: operators see
// what survived the last crash instead of discovering it by accident.
func logRecoveryInventory(recoveryDir string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	inventory, err := readRecoveryInventory(recoveryDir)
	if err != nil {
		logging.Error(logger, "recovery inventory scan failed", "dir", recoveryDir, "err", err)
		return
	}
	if len(inventory) == 0 {
		return
	}
	var total int64
	targets := make([]string, 0, len(inventory))
	for _, e := range inventory {
		total += e.Size
		target := e.TargetPath
		if target == "" {
			target = e.OrigTemp
		}
		targets = append(targets, target)
	}
	logging.Warn(logger, "replaying quarantined overlays from previous run; manual recovery may be needed",
		"dir", recoveryDir, "files", len(inventory), "bytes", total, "targets", strings.Join(targets, ","))
}
