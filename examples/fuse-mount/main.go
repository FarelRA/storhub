package main

import (
	"fmt"
	"log"
	"os"

	storfuse "github.com/FarelRA/storhub/fuse"
	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}
	mountPoint := os.Getenv("STORHUB_MOUNT_POINT")
	if mountPoint == "" {
		log.Fatal("STORHUB_MOUNT_POINT environment variable not set")
	}
	project := os.Getenv("STORHUB_PROJECT")
	if project == "" {
		log.Fatal("STORHUB_PROJECT environment variable not set")
	}
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatal(err)
	}
	opts := storfuse.DefaultOptions()
	fsys, err := storfuse.New(hub, project, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer fsys.Close()
	if err := fsys.Mount(mountPoint); err != nil {
		log.Fatal(err)
	}
	defer fsys.Unmount()
	fmt.Printf("mounted %s at %s\n", project, mountPoint)
	fsys.Wait()
}
