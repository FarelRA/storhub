package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	// Ctrl+C cancels in-flight transfers instead of leaving half-uploaded
	// state behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	workspace, err := os.MkdirTemp("", "storhub-files-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-files-%d", os.Getpid())
	inputA := filepath.Join(workspace, "v1.txt")
	inputB := filepath.Join(workspace, "v2.txt")
	output := filepath.Join(workspace, "downloaded.txt")
	if err := os.WriteFile(inputA, []byte("hello from storhub\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(inputB, []byte("hello from storhub v2\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/readme.txt", inputA); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.ReplaceFileContext(ctx, project, "docs/readme.txt", inputB); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.PatchFileContext(ctx, project, "docs/readme.txt", 6, 4, []byte("there")); err != nil {
		log.Fatal(err)
	}
	files, err := hub.ListFilesContext(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	releases, err := hub.ListReleasesContext(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	if err := hub.DownloadFileContext(ctx, project, "docs/readme.txt", output); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("project=%s files=%d releases=%d\n", project, len(files), len(releases))
}
