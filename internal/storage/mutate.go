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
