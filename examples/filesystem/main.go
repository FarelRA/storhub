package main

import (
	"fmt"
	"log"
	"os"

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
	project := fmt.Sprintf("storhub-fs-%d", os.Getpid())
	must(hub.Mkdir(project, "docs"))
	must(hub.Mkdir(project, "docs/specs"))
	if _, err := hub.CreateFile(project, "docs/specs/notes.txt"); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.WriteFileAt(project, "docs/specs/notes.txt", 0, []byte("hello")); err != nil {
		log.Fatal(err)
	}
	if _, err := hub.AppendFile(project, "docs/specs/notes.txt", []byte(" world")); err != nil {
		log.Fatal(err)
	}
	preview, err := hub.ReadFileAt(project, "docs/specs/notes.txt", 0, 64)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.TruncateFile(project, "docs/specs/notes.txt", 5); err != nil {
		log.Fatal(err)
	}
	must(hub.Rename(project, "docs/specs/notes.txt", "docs/specs/notes-short.txt"))
	entries, err := hub.ReadDir(project, "docs/specs")
	if err != nil {
		log.Fatal(err)
	}
	info, err := hub.StatPath(project, "docs/specs/notes-short.txt")
	if err != nil {
		log.Fatal(err)
	}
	stats, err := hub.StatFS(project)
	if err != nil {
		log.Fatal(err)
	}
	must(hub.DeleteFile(project, "docs/specs/notes-short.txt"))
	must(hub.Rmdir(project, "docs/specs"))
	fmt.Printf("preview=%q entries=%d size=%d files=%d dirs=%d\n", string(preview), len(entries), info.Size, stats.Files, stats.Directories)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
