package fs

import (
	"context"
	"errors"
)

// ErrPreconditionFailed reports that the repository's metadata revision no
// longer matches the revision a caller declared with WithExpectedRevision.
// Callers treat it as "re-read state and retry the decision"; REST maps it
// to HTTP 412 Precondition Failed.
var ErrPreconditionFailed = errors.New("storhub: metadata revision changed since expected revision")

// MutateOption decorates a mutating operation.
type MutateOption func(*mutateOptions)

type mutateOptions struct {
	expectedRevision string
}

// ExpectedRevision returns the revision declared via WithExpectedRevision
// ("" when none).
func (o mutateOptions) ExpectedRevision() string { return o.expectedRevision }

// ApplyMutateOptions folds options into a snapshot for inspection; exported
// for storage-layer enforcement helpers.
func ApplyMutateOptions(opts []MutateOption) mutateOptions {
	var o mutateOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// WithExpectedRevision declares the metadata revision this decision was
// based on (obtain one via RevisionContext). Before applying the mutation,
// the current remote revision is re-fetched; any divergence fails with
// ErrPreconditionFailed instead of silently landing last-writer-wins state.
// The empty string means "no expectation" and skips the check entirely, so
// existing callers are unaffected.
func WithExpectedRevision(revision string) MutateOption {
	return func(o *mutateOptions) {
		o.expectedRevision = revision
	}
}

// RevisionSource is implemented by backends that can report the current
// committed metadata revision (content SHA) of a project.
type RevisionSource interface {
	// RevisionContext returns the project's current remote metadata
	// revision, bypassing caches so the value reflects GitHub's HEAD.
	RevisionContext(ctx context.Context, project string) (string, error)
}

// CheckExpectedRevision compares expected against current and returns
// ErrPreconditionFailed on divergence. Shared by every enforce* helper so
// the failure text stays uniform across layers.
func CheckExpectedRevision(expected, current string) error {
	if expected == "" || expected == current {
		return nil
	}
	return ErrPreconditionFailed
}
