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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		return err
	}
	workspace, err := os.MkdirTemp("", "storhub-posix-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(workspace); err != nil {
			fmt.Fprintln(os.Stderr, "workspace cleanup:", err)
		}
	}()
	project := fmt.Sprintf("storhub-posix-%d", os.Getpid())
	// A demo repository is garbage once the demo ends — delete it even when
	// something below fails, so runs never litter the account. Errors unwind
	// through run() instead of exiting, which is what lets this defer fire.
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
	local := filepath.Join(workspace, "source.txt")
	if err := os.WriteFile(local, []byte("posix demo\n"), 0o644); err != nil {
		return err
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/source.txt", local); err != nil {
		return err
	}
	created = true
	if err := hub.ChmodContext(ctx, project, "docs/source.txt", 0o640); err != nil {
		return err
	}
	if err := hub.ChownContext(ctx, project, "docs/source.txt", 1000, 1000); err != nil {
		return err
	}
	if err := hub.ChtimesContext(ctx, project, "docs/source.txt", 1, 2); err != nil {
		return err
	}
	if err := hub.SetXAttrContext(ctx, project, "docs/source.txt", "user.demo", []byte("on")); err != nil {
		return err
	}
	value, err := hub.GetXAttrContext(ctx, project, "docs/source.txt", "user.demo")
	if err != nil {
		return err
	}
	attrs, err := hub.ListXAttrContext(ctx, project, "docs/source.txt")
	if err != nil {
		return err
	}
	if _, err := hub.SymlinkContext(ctx, project, "docs/source.txt", "docs/source.link"); err != nil {
		return err
	}
	target, err := hub.ReadlinkContext(ctx, project, "docs/source.link")
	if err != nil {
		return err
	}
	if _, err := hub.LinkContext(ctx, project, "docs/source.txt", "docs/source-copy.txt"); err != nil {
		return err
	}
	if err := hub.RemoveXAttrContext(ctx, project, "docs/source.txt", "user.demo"); err != nil {
		return err
	}
	fmt.Printf("xattr=%q attrs=%d symlink=%s\n", string(value), len(attrs), target)
	return nil
}
