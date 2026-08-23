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

	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	workspace, err := os.MkdirTemp("", "storhub-revisions-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-revisions-%d", os.Getpid())
	input := filepath.Join(workspace, "rev.txt")
	if err := os.WriteFile(input, []byte("revision demo\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/rev.txt", input); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.PatchFileContext(ctx, project, "docs/rev.txt", 0, 0, []byte("v2: ")); err != nil {
		log.Fatal(err)
	}
	revisions, err := hub.ListMetadataRevisionsContext(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	if len(revisions) > 1 {
		if err := hub.RollbackMetadataContext(ctx, project, revisions[1].CommitSHA); err != nil {
			log.Fatal(err)
		}
	}
	purge, err := hub.PurgeUntrackedContext(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	if err := hub.CleanupProjectContext(ctx, project); err != nil {
		log.Fatal(err)
	}
	if tag := strings.TrimSpace(os.Getenv("STORHUB_DELETE_RELEASE_TAG")); tag != "" {
		if err := hub.DeleteReleaseContext(ctx, project, tag); err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("STORHUB_DELETE_PROJECT") == "1" {
		if err := hub.DeleteProjectContext(ctx, project); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("revisions=%d purged-assets=%d purged-releases=%d\n", len(revisions), purge.DeletedAssets, purge.DeletedReleases)
}
