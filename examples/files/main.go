package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	workspace, err := os.MkdirTemp("", "storhub-files-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(workspace)
	project := fmt.Sprintf("storhub-files-%d", os.Getpid())
	inputA := filepath.Join(workspace, "v1.txt")
	inputB := filepath.Join(workspace, "v2.txt")
	output := filepath.Join(workspace, "downloaded.txt")
	_ = os.WriteFile(inputA, []byte("hello from storhub\n"), 0o644)
	_ = os.WriteFile(inputB, []byte("hello from storhub v2\n"), 0o644)
	meta, err := hub.UploadFile(project, "docs/readme.txt", inputA)
	if err != nil {
		log.Fatal(err)
	}
	meta, err = hub.ReplaceFile(project, meta.Name, inputB)
	if err != nil {
		log.Fatal(err)
	}
	meta, err = hub.PatchFile(project, meta.Name, 6, 4, []byte("there"))
	if err != nil {
		log.Fatal(err)
	}
	files, err := hub.ListFiles(project)
	if err != nil {
		log.Fatal(err)
	}
	releases, err := hub.ListReleases(project)
	if err != nil {
		log.Fatal(err)
	}
	if err := hub.DownloadFile(project, meta.Name, output); err != nil {
		log.Fatal(err)
	}
	if err := storhub.VerifyFileIntegrity(output, *meta, 32<<10); err != nil {
		log.Fatal(err)
	}
	combined, err := storhub.CombineChunkCRC32Cs(meta.Chunks)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("project=%s files=%d releases=%d combined-crc32c=%s\n", project, len(files), len(releases), combined)
}
