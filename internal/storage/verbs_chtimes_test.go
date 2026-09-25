package storage

import (
	"context"
	"testing"
	"time"
)

// TestChtimesMatchesExplicit pins the timestamp contract: the explicit
// pointer form lands stamps verbatim (the epoch included) while the int64
// form resolves zero per field to now and can never store the epoch.
func TestChtimesMatchesExplicit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := newMockGitHub(t)
	hub := backend.newClient(t, smallTransferTestConfig())
	project := "projectchtimes"

	cloneSeedFile(t, hub, project, "stamped.txt", []byte("tick"))

	fixed := time.Unix(1_700_000_000, 123).UTC()
	if err := hub.ChtimesExplicitContext(ctx, project, "stamped.txt", &fixed, &fixed); err != nil {
		t.Fatalf("explicit: %v", err)
	}
	entry, err := hub.StatPathContext(ctx, project, "stamped.txt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if entry.AccessedAt != fixed.UnixNano() || entry.ModifiedAt != fixed.UnixNano() {
		t.Fatalf("explicit stamps not verbatim: atime %d mtime %d want %d",
			entry.AccessedAt, entry.ModifiedAt, fixed.UnixNano())
	}

	epoch := time.Unix(0, 0).UTC()
	if err := hub.ChtimesExplicitContext(ctx, project, "stamped.txt", &epoch, &epoch); err != nil {
		t.Fatalf("explicit epoch: %v", err)
	}
	entry, err = hub.StatPathContext(ctx, project, "stamped.txt")
	if err != nil {
		t.Fatalf("stat epoch: %v", err)
	}
	if entry.AccessedAt != 0 || entry.ModifiedAt != 0 {
		t.Fatalf("epoch must land verbatim: atime %d mtime %d",
			entry.AccessedAt, entry.ModifiedAt)
	}

	if err := hub.ChtimesContext(ctx, project, "stamped.txt", 0, 0); err != nil {
		t.Fatalf("legacy zeros: %v", err)
	}
	entry, err = hub.StatPathContext(ctx, project, "stamped.txt")
	if err != nil {
		t.Fatalf("stat legacy: %v", err)
	}
	if entry.AccessedAt == 0 || entry.ModifiedAt == 0 {
		t.Fatalf("legacy zero must resolve to now, never epoch: atime %d mtime %d",
			entry.AccessedAt, entry.ModifiedAt)
	}
}
