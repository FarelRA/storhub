package storage

import (
	"testing"
	"time"
)

// TestAdmissionDeleteEscape pins the size admission escape: growth on a
// capped project fails fast, while a removal with the escape flag is
// admitted so the project can always fold back under the ceiling.
func TestAdmissionDeleteEscape(t *testing.T) {
	t.Parallel()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectadmission"
	pm := &projectMetadata{meta: NewRepoMetadata(project), version: 7, sizeCapped: true}
	candidate := NewRepoMetadata(project)
	started := time.Unix(1_700_000_000, 0).UTC()
	over := int64(maxMetadataBytes) + 1

	if err := hub.admitCandidateSplit(project, pm, candidate, 1, over, pm.version, "storhub: grow", started, false); err == nil {
		t.Fatal("capped growth without escape must be rejected")
	}
	if err := hub.admitCandidateSplit(project, pm, candidate, over, over+1, pm.version, "storhub: delete", started, true); err != nil {
		t.Fatalf("delete escape must admit: %v", err)
	}
	if pm.sizeCapped {
		t.Fatal("admitted escape must clear the cap like any shrink")
	}
}
