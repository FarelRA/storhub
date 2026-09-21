package storage

import (
	"context"
	"fmt"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// RevisionContext returns the project's current metadata revision: the
// content SHA of the latest committed metadata the cache holds. It serves
// the cached snapshot (loading fresh only on a cold miss), so the token a
// client receives always describes the tree the reads just served - the
// X-StorHub-Revision header rides every read, and a fresh remote load per
// call turned each GET into a full metadata reload. Snapshot coherence
// over remote exactness: an external writer may advance HEAD past the
// cached token, but that direction fails closed - enforceExpectedRevision
// still re-verifies against remote HEAD at apply time, so a stale token
// yields 409/412, never a silent overwrite.
func (h *StorHub) RevisionContext(ctx context.Context, project string) (string, error) {
	if err := validateProject(project); err != nil {
		return "", err
	}
	_, sha, err := h.loadRepoMetadataReadonly(ctx, project)
	if err != nil {
		return "", err
	}
	return sha, nil
}

// enforceExpectedRevision verifies an operation's declared revision against
// the remote HEAD immediately before the mutation applies. The residual
// window between this check and the async metadata commit is covered by the
// commit-level conflict detection (version-guarded recovery).
func (h *StorHub) enforceExpectedRevision(ctx context.Context, project string, opts []shfs.MutateOption) error {
	cfg := shfs.ApplyMutateOptions(opts)
	if cfg.ExpectedRevision() == "" {
		return nil
	}
	_, current, err := h.loadRepoMetadataFresh(ctx, project)
	if err != nil {
		return err
	}
	if err := shfs.CheckExpectedRevision(cfg.ExpectedRevision(), current); err != nil {
		return fmt.Errorf("%w: expected %s, remote at %s", err, cfg.ExpectedRevision(), current)
	}
	return nil
}

// revisionGateKey carries a compare-and-swap token from the synchronous
// pre-check (enforceExpectedRevision) into the mutation's critical section,
// so the token is re-verified atomically with the write. The key is
// unexported: only this package arms and consumes the gate.
type revisionGateKey struct{}

// withRevisionGate returns a context carrying the declared revision for
// the in-transaction gate (checkRevisionGateLocked). An empty revision
// leaves the context unchanged, so non-CAS callers are unaffected. Call
// it only after enforceExpectedRevision succeeded: the gate re-checks the
// same token against the committed state under lock, it never widens it.
func withRevisionGate(ctx context.Context, revision string) context.Context {
	if revision == "" {
		return ctx
	}
	return context.WithValue(ctx, revisionGateKey{}, revision)
}

// revisionGateFromContext returns the gated revision carried by ctx (""
// when none).
func revisionGateFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	revision, _ := ctx.Value(revisionGateKey{}).(string)
	return revision
}

// gateRevisionFromOpts folds opts and stashes the declared revision in
// ctx for the in-transaction gate. Every verb that enforces an expected
// revision must route its (already pre-checked) ctx through here before
// delegating to the transaction or direct-mutation path.
func gateRevisionFromOpts(ctx context.Context, opts []shfs.MutateOption) context.Context {
	return withRevisionGate(ctx, shfs.ApplyMutateOptions(opts).ExpectedRevision())
}

// checkRevisionGateLocked re-verifies a gated revision against the
// committed CAS token (pm.sha) and consumes it, all under pm.mu: the
// check and the consume share one critical section with the write that
// follows, so two mutations gated on the same token cannot both be
// admitted (no TOCTOU). A mismatch fails with ErrPreconditionFailed
// before any mutation runs, so a rejected CAS never partially applies.
//
// Tokens are single-use while the committed revision stands still: local
// mutations publish without advancing pm.sha (only a successful commit
// does), so equality alone cannot serialize local racers. The first
// admission arms the gate; a second admission on the same token observes
// the armed gate and fails. Once a commit advances pm.sha the armed token
// no longer equals it and is ignored (stale gates never false-reject).
// Caller must hold pm.mu for writing.
func (h *StorHub) checkRevisionGateLocked(pm *projectMetadata, expected string) error {
	if expected == "" {
		return nil
	}
	if err := shfs.CheckExpectedRevision(expected, pm.sha); err != nil {
		return fmt.Errorf("%w: expected %s, committed at %s", err, expected, pm.sha)
	}
	if pm.casArmed && pm.casToken == expected {
		return fmt.Errorf("%w: expected %s, token already consumed by a pending mutation", shfs.ErrPreconditionFailed, expected)
	}
	pm.casArmed = true
	pm.casToken = expected
	return nil
}
