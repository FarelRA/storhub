package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	project := fmt.Sprintf("storhub-fs-%d", os.Getpid())
	must(hub.MkdirContext(ctx, project, "docs"))
	must(hub.MkdirContext(ctx, project, "docs/specs"))
	if _, err := hub.CreateFileContext(ctx, project, "docs/specs/notes.txt"); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.WriteFileAtContext(ctx, project, "docs/specs/notes.txt", 0, []byte("hello")); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.AppendFileContext(ctx, project, "docs/specs/notes.txt", []byte(" world")); err != nil {
		log.Fatal(err)
	}
	preview, err := hub.ReadFileAtContext(ctx, project, "docs/specs/notes.txt", 0, 64)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.TruncateFileContext(ctx, project, "docs/specs/notes.txt", 5); err != nil {
		log.Fatal(err)
	}
	must(hub.RenameContext(ctx, project, "docs/specs/notes.txt", "docs/specs/notes-short.txt"))
	entries, err := hub.ReadDirContext(ctx, project, "docs/specs")
	if err != nil {
		log.Fatal(err)
	}
	info, err := hub.StatPathContext(ctx, project, "docs/specs/notes-short.txt")
	if err != nil {
		log.Fatal(err)
	}
	stats, err := hub.StatFSContext(ctx, project)
	if err != nil {
		log.Fatal(err)
	}
	must(hub.DeleteFileContext(ctx, project, "docs/specs/notes-short.txt"))
	must(hub.RmdirContext(ctx, project, "docs/specs"))
	fmt.Printf("preview=%q entries=%d size=%d files=%d dirs=%d\n", string(preview), len(entries), info.Size, stats.Files, stats.Directories)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
