package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	// Ctrl+C cancels in-flight transfers instead of leaving half-uploaded
	// state behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		return err
	}
	workspace, err := os.MkdirTemp("", "storhub-files-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(workspace); err != nil {
			fmt.Fprintln(os.Stderr, "workspace cleanup:", err)
		}
	}()
	project := fmt.Sprintf("storhub-files-%d", os.Getpid())
	// A demo repository is garbage once the demo ends — delete it even when
	// something below fails, so runs never litter the account.
	var created bool
	defer func() {
		if !created {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := hub.DeleteProjectContext(cleanupCtx, project); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup: deleting %s: %v\n", project, err)
		}
	}()
	inputA := filepath.Join(workspace, "v1.txt")
	inputB := filepath.Join(workspace, "v2.txt")
	output := filepath.Join(workspace, "downloaded.txt")
	if err := os.WriteFile(inputA, []byte("hello from storhub\n"), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(inputB, []byte("hello from storhub v2\n"), 0o644); err != nil {
		return err
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/readme.txt", inputA); err != nil {
		return err
	}
	created = true
	if _, err := hub.ReplaceFileContext(ctx, project, "docs/readme.txt", inputB); err != nil {
		return err
	}
	if _, err := hub.PatchFileContext(ctx, project, "docs/readme.txt", 6, 4, []byte("there")); err != nil {
		return err
	}
	files, err := hub.ListFilesContext(ctx, project)
	if err != nil {
		return err
	}
	releases, err := hub.ListReleasesContext(ctx, project)
	if err != nil {
		return err
	}
	if err := hub.DownloadFileContext(ctx, project, "docs/readme.txt", output); err != nil {
		return err
	}
	fmt.Printf("project=%s files=%d releases=%d\n", project, len(files), len(releases))
	return nil
}
