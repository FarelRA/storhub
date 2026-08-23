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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	hub, err := storhub.NewStorHubWithContext(ctx, token, storhub.DefaultConfig())
	if err != nil {
		log.Fatal(err)
	}
	workspace, err := os.MkdirTemp("", "storhub-posix-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-posix-%d", os.Getpid())
	local := filepath.Join(workspace, "source.txt")
	if err := os.WriteFile(local, []byte("posix demo\n"), 0o644); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.UploadFileContext(ctx, project, "docs/source.txt", local); err != nil {
		log.Fatal(err)
	}
	must(hub.ChmodContext(ctx, project, "docs/source.txt", 0o640))
	must(hub.ChownContext(ctx, project, "docs/source.txt", 1000, 1000))
	must(hub.ChtimesContext(ctx, project, "docs/source.txt", 1, 2))
	must(hub.SetXAttrContext(ctx, project, "docs/source.txt", "user.demo", []byte("on")))
	value, err := hub.GetXAttrContext(ctx, project, "docs/source.txt", "user.demo")
	if err != nil {
		log.Fatal(err)
	}
	attrs, err := hub.ListXAttrContext(ctx, project, "docs/source.txt")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.SymlinkContext(ctx, project, "docs/source.txt", "docs/source.link"); err != nil {
		log.Fatal(err)
	}
	target, err := hub.ReadlinkContext(ctx, project, "docs/source.link")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.LinkContext(ctx, project, "docs/source.txt", "docs/source-copy.txt"); err != nil {
		log.Fatal(err)
	}
	must(hub.RemoveXAttrContext(ctx, project, "docs/source.txt", "user.demo"))
	fmt.Printf("xattr=%q attrs=%d symlink=%s\n", string(value), len(attrs), target)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
