package storage

import (
	"fmt"
	"sort"

	"github.com/FarelRA/storhub/internal/logging"
)

// Degraded-mode policy: once a project's commits fail
// consecutively past MaxConsecutiveCommitFailures, the project stops
// admitting new mutations and fails them fast with a loud typed error.
// The latch is sticky: later commit successes clear the pressure streak
// but never re-admit, because silent recovery would mask a sick backend.
// Only an explicit ReEnableProject re-admits.
//
// The commit outcome hook lives in commit.go (owned by another workstream),
// so the latch arms lazily at mutation admission: the first mutation that
// observes a streak at or past the threshold latches the project and is
// itself refused. A streak that heals before any new mutation arrives
// never latches, which is correct: the backend demonstrably recovered.
//
// Recovery verbs bypass structurally: drain, shutdown drain, and rollback
// commit through commitProjectMetadata/commitRepoMetadata directly, and
// purge touches no tree, so none of them passes admission. RevertPath is
// deliberately NOT bypassed: it synthesizes new work through
// UpdateRepoMetadataContext, making it a mutation, not a rescue.
//
// The latch lives on the hub (StorHub.degradedLatched, guarded by
// degradedMu) so entries die with the hub: no process-wide registry can
// pin dead hubs, and lifecycle needs no cleanup beyond hub teardown.

// DegradedProjectError is the fail-fast refusal a degraded project answers
// to new mutations. It names the project and the streak that tripped the
// latch, and points at the rescue paths. Match with errors.As.
type DegradedProjectError struct {
	Project   string
	Streak    uint64
	Threshold int
}

func (e *DegradedProjectError) Error() string {
	return fmt.Sprintf("project %q is degraded after %d consecutive commit failures (threshold %d): refusing new mutations; recover with rollback, prune, or drain, or enable explicitly via ReEnableProject", e.Project, e.Streak, e.Threshold)
}

// degradedThreshold resolves the effective trip point: a non-positive knob
// (hand-built Config that skipped WithDefaults) takes the library default
// instead of degrading on the first failure or never.
func (h *StorHub) degradedThreshold() int {
	if h.config.MaxConsecutiveCommitFailures > 0 {
		return h.config.MaxConsecutiveCommitFailures
	}
	return DefaultConfig().MaxConsecutiveCommitFailures
}

// isProjectDegraded reports whether the latch is set (sticky: independent
// of the live pressure streak, which successes reset).
func (h *StorHub) isProjectDegraded(project string) bool {
	h.degradedMu.Lock()
	defer h.degradedMu.Unlock()
	return h.degradedLatched[project]
}

// markProjectDegraded sets the latch, logging the transition exactly once.
// The streak stays visible in PressureSnapshot; the latch is visible here.
func (h *StorHub) markProjectDegraded(project string) {
	h.degradedMu.Lock()
	if h.degradedLatched == nil {
		h.degradedLatched = make(map[string]bool)
	}
	already := h.degradedLatched[project]
	if !already {
		h.degradedLatched[project] = true
	}
	h.degradedMu.Unlock()
	if !already {
		logging.Warn(h.projectLogger(project), "project degraded: refusing new mutations until explicit re-enable",
			"streak", h.PressureFailureStreak(project),
			"threshold", h.degradedThreshold())
	}
}

// admitMutation is the mutation-admission gate: every verbs.go mutation
// entry passes here first. Healthy projects pass unless their live streak
// reached the threshold, in which case this call latches them and refuses.
// Latched projects are always refused, even with a zero streak. A project
// with a running prune is refused with *PruneConflictError so admitted
// work never interleaves with the prune's classify-to-delete windows;
// prune-owned commit tails run with the fence dropped, so they pass here.
func (h *StorHub) admitMutation(project string) error {
	threshold := h.degradedThreshold()
	if !h.isProjectDegraded(project) {
		if h.PressureFailureStreak(project) < uint64(threshold) {
			if h.pruneFenceRunning(project) {
				return &PruneConflictError{Project: project}
			}
			return nil
		}
		h.markProjectDegraded(project)
	} else if h.pruneFenceRunning(project) {
		return &PruneConflictError{Project: project}
	}
	return &DegradedProjectError{
		Project:   project,
		Streak:    h.PressureFailureStreak(project),
		Threshold: threshold,
	}
}

// ReEnableProject clears the degraded latch for one project, re-admitting
// mutations. It is idempotent (re-enabling a healthy project succeeds) and
// the ONLY path back to healthy: commit successes never clear the latch.
// Recovery verbs (drain, purge, rollback) need no re-enable; they bypass
// the gate structurally.
func (h *StorHub) ReEnableProject(project string) error {
	if err := validateProject(project); err != nil {
		return err
	}
	h.degradedMu.Lock()
	was := h.degradedLatched[project]
	delete(h.degradedLatched, project)
	h.degradedMu.Unlock()
	if was {
		logging.Info(h.projectLogger(project), "project re-enabled by explicit admin action; mutations admitted again")
	} else {
		logging.Debug(h.projectLogger(project), "project re-enable on a healthy project; no latch to clear")
	}
	return nil
}

// DegradedProjects lists every latched-degraded project on this hub,
// sorted for stable operator output. The live streaks behind the latch
// remain visible in PressureSnapshot.
func (h *StorHub) DegradedProjects() []string {
	h.degradedMu.Lock()
	out := make([]string, 0, len(h.degradedLatched))
	for name := range h.degradedLatched {
		out = append(out, name)
	}
	h.degradedMu.Unlock()
	sort.Strings(out)
	return out
}
