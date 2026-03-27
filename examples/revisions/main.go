package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	hub, err := storhub.NewStorHub(token)
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
	_ = os.WriteFile(input, []byte("revision demo\n"), 0o644)
	meta, err := hub.UploadFile(project, "docs/rev.txt", input)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.PatchFile(project, meta.Name, 0, 0, []byte("v2: ")); err != nil {
		log.Fatal(err)
	}
	revisions, err := hub.ListMetadataRevisions(project)
	if err != nil {
		log.Fatal(err)
	}
	if len(revisions) > 1 {
		if err := hub.RollbackMetadata(project, revisions[1].CommitSHA); err != nil {
			log.Fatal(err)
		}
	}
	purge, err := hub.PurgeUntracked(project)
	if err != nil {
		log.Fatal(err)
	}
	if err := hub.CleanupProject(project); err != nil {
		log.Fatal(err)
	}
	if tag := strings.TrimSpace(os.Getenv("STORHUB_DELETE_RELEASE_TAG")); tag != "" {
		if err := hub.DeleteRelease(project, tag); err != nil {
			log.Fatal(err)
		}
	}
	if os.Getenv("STORHUB_DELETE_PROJECT") == "1" {
		if err := hub.DeleteProject(project); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("revisions=%d purged-assets=%d purged-releases=%d\n", len(revisions), purge.DeletedAssets, purge.DeletedReleases)
}
