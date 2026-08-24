package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/FarelRA/storhub/storhub"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return fmt.Errorf("GITHUB_TOKEN environment variable not set")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		return err
	}
	workspace, err := os.MkdirTemp("", "storhub-revisions-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-revisions-%d", os.Getpid())
	// A half-finished demo is pure litter: always delete when the run
	// failed after creation. On success the env vars below decide retention.
	var created, failed bool
	defer func() {
		if !failed || !created {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := hub.DeleteProjectContext(cleanupCtx, project); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup: deleting %s: %v\n", project, err)
		}
	}()
	input := filepath.Join(workspace, "rev.txt")
	if err := os.WriteFile(input, []byte("revision demo\n"), 0o644); err != nil {
		return failedExit(&failed, err)
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/rev.txt", input); err != nil {
		return failedExit(&failed, err)
	}
	created = true
	if _, err := hub.PatchFileContext(ctx, project, "docs/rev.txt", 0, 0, []byte("v2: ")); err != nil {
		return failedExit(&failed, err)
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, project)
	if err != nil {
		return failedExit(&failed, err)
	}
	if len(revisions) > 1 {
		if err := hub.RollbackMetadataContext(ctx, project, revisions[1].CommitSHA); err != nil {
			return failedExit(&failed, err)
		}
	}
	purge, err := hub.PurgeUntrackedContext(ctx, project)
	if err != nil {
		return failedExit(&failed, err)
	}
	if err := hub.CleanupProjectContext(ctx, project); err != nil {
		return failedExit(&failed, err)
	}
	if tag := strings.TrimSpace(os.Getenv("STORHUB_DELETE_RELEASE_TAG")); tag != "" {
		if err := hub.DeleteReleaseContext(ctx, project, tag); err != nil {
			return failedExit(&failed, err)
		}
	}
	if os.Getenv("STORHUB_DELETE_PROJECT") == "1" {
		if err := hub.DeleteProjectContext(ctx, project); err != nil {
			return failedExit(&failed, err)
		}
	}
	fmt.Printf("revisions=%d purged-assets=%d purged-releases=%d\n", len(revisions), purge.DeletedAssets, purge.DeletedReleases)
	return nil
}

func failedExit(failed *bool, err error) error {
	*failed = true
	return err
}
