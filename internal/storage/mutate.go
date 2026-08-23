package storage

import (
	"context"
	"fmt"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// RevisionContext returns the project's current remote metadata revision
// (the content SHA of the latest committed metadata.json). Clients pass it
// to WithExpectedRevision on a later mutation to assert nothing else
// advanced the project in between.
func (h *StorHub) RevisionContext(ctx context.Context, project string) (string, error) {
	if err := validateProject(project); err != nil {
		return "", err
	}
	_, sha, err := h.loadRepoMetadataFresh(ctx, project)
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
