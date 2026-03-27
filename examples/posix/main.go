package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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
	workspace, err := os.MkdirTemp("", "storhub-posix-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-posix-%d", os.Getpid())
	local := filepath.Join(workspace, "source.txt")
	_ = os.WriteFile(local, []byte("posix demo\n"), 0o644)
	meta, err := hub.UploadFile(project, "docs/source.txt", local)
	if err != nil {
		log.Fatal(err)
	}
	must(hub.Chmod(project, meta.Name, 0o640))
	must(hub.Chown(project, meta.Name, 1000, 1000))
	must(hub.Chtimes(project, meta.Name, time.Unix(1, 0), time.Unix(2, 0)))
	must(hub.SetXAttr(project, meta.Name, "user.demo", []byte("on")))
	value, err := hub.GetXAttr(project, meta.Name, "user.demo")
	if err != nil {
		log.Fatal(err)
	}
	attrs, err := hub.ListXAttr(project, meta.Name)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.Symlink(project, meta.Name, "docs/source.link"); err != nil {
		log.Fatal(err)
	}
	target, err := hub.Readlink(project, "docs/source.link")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := hub.Link(project, meta.Name, "docs/source-copy.txt"); err != nil {
		log.Fatal(err)
	}
	must(hub.RemoveXAttr(project, meta.Name, "user.demo"))
	fmt.Printf("mode=%#o xattr=%q attrs=%d symlink=%s\n", meta.Mode, string(value), len(attrs), target)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
