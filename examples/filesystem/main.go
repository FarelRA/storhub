package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	project := fmt.Sprintf("storhub-fs-%d", os.Getpid())
	// A demo repository is garbage once the demo ends - delete it even when
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
	if err := hub.MkdirContext(ctx, project, "docs"); err != nil {
		return err
	}
	created = true
	if err := hub.MkdirContext(ctx, project, "docs/specs"); err != nil {
		return err
	}
	if _, err := hub.CreateFileContext(ctx, project, "docs/specs/notes.txt"); err != nil {
		return err
	}
	if _, err := hub.WriteFileAtContext(ctx, project, "docs/specs/notes.txt", 0, []byte("hello")); err != nil {
		return err
	}
	if _, err := hub.AppendFileContext(ctx, project, "docs/specs/notes.txt", []byte(" world")); err != nil {
		return err
	}
	preview, err := hub.ReadFileAtContext(ctx, project, "docs/specs/notes.txt", 0, 64)
	if err != nil {
		return err
	}
	if _, err := hub.TruncateFileContext(ctx, project, "docs/specs/notes.txt", 5); err != nil {
		return err
	}
	if err := hub.RenameContext(ctx, project, "docs/specs/notes.txt", "docs/specs/notes-short.txt"); err != nil {
		return err
	}
	entries, err := hub.ReadDirContext(ctx, project, "docs/specs")
	if err != nil {
		return err
	}
	info, err := hub.StatPathContext(ctx, project, "docs/specs/notes-short.txt")
	if err != nil {
		return err
	}
	stats, err := hub.StatFSContext(ctx, project)
	if err != nil {
		return err
	}
	if err := hub.DeleteFileContext(ctx, project, "docs/specs/notes-short.txt"); err != nil {
		return err
	}
	if err := hub.RmdirContext(ctx, project, "docs/specs"); err != nil {
		return err
	}
	fmt.Printf("preview=%q entries=%d size=%d files=%d dirs=%d\n", string(preview), len(entries), info.Size, stats.Files, stats.Directories)
	return nil
}
