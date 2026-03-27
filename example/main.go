package main

import (
	"fmt"
	"log"
	"os"

	"github.com/storhub/storhub"
)

func main() {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		log.Fatal("GITHUB_TOKEN environment variable not set")
	}

	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatalf("Failed to initialize StorHub: %v", err)
	}

	fmt.Printf("StorHub initialized for %s\n", hub.Owner())
	fmt.Println()

	fmt.Println("=== Example 1: Upload File ===")

	sampleFile := "sample.txt"
	content := "Hello from StorHub!\nThis is a sample file for testing."
	if err := os.WriteFile(sampleFile, []byte(content), 0o644); err != nil {
		log.Fatalf("Failed to create sample file: %v", err)
	}
	defer os.Remove(sampleFile)

	project := "my-storage"
	fmt.Printf("Uploading %s to project %s...\n", sampleFile, project)

	fileMeta, err := hub.UploadFile(project, sampleFile, sampleFile)
	if err != nil {
		log.Fatalf("Failed to upload file: %v", err)
	}

	fmt.Printf("File uploaded successfully.\n")
	fmt.Printf("  Name: %s\n", fileMeta.Name)
	fmt.Printf("  Size: %d bytes\n", fileMeta.Size)
	fmt.Printf("  Release: %s\n", fileMeta.Release)
	fmt.Printf("  CRC32C: %s\n", fileMeta.CRC32C)
	if len(fileMeta.Chunks) > 0 {
		fmt.Printf("  Chunks: %d\n", len(fileMeta.Chunks))
	}
	fmt.Println()

	fmt.Println("=== Example 2: List Files ===")

	files, err := hub.ListFiles(project)
	if err != nil {
		log.Fatalf("Failed to list files: %v", err)
	}

	fmt.Printf("Total files in %s: %d\n", project, len(files))
	for i, file := range files {
		fmt.Printf("%d. %s (%d bytes) - Release: %s\n",
			i+1, file.Name, file.Size, file.Release)
		if len(file.Chunks) > 0 {
			fmt.Printf("   Chunked into %d parts\n", len(file.Chunks))
		}
	}
	fmt.Println()

	fmt.Println("=== Example 3: Download File ===")

	downloadPath := "downloaded_sample.txt"
	fmt.Printf("Downloading %s...\n", fileMeta.Name)

	err = hub.DownloadFile(project, fileMeta.Name, downloadPath)
	if err != nil {
		log.Fatalf("Failed to download file: %v", err)
	}

	fmt.Printf("File downloaded to %s\n", downloadPath)

	downloadedContent, err := os.ReadFile(downloadPath)
	if err != nil {
		log.Fatalf("Failed to read downloaded file: %v", err)
	}

	fmt.Printf("Content: %s\n", string(downloadedContent))
	os.Remove(downloadPath)

	fmt.Println()
	fmt.Println("=== Example 4: Immutable Patch ===")
	patchedMeta, err := hub.PatchFile(project, fileMeta.Name, 6, 4, []byte("patched"))
	if err != nil {
		log.Fatalf("Failed to patch file: %v", err)
	}
	patchedPath := "patched_sample.txt"
	if err := hub.DownloadFile(project, fileMeta.Name, patchedPath); err != nil {
		log.Fatalf("Failed to download patched file: %v", err)
	}
	patchedContent, err := os.ReadFile(patchedPath)
	if err != nil {
		log.Fatalf("Failed to read patched file: %v", err)
	}
	_ = os.Remove(patchedPath)
	fmt.Printf("Patched CRC32C: %s\n", patchedMeta.CRC32C)
	fmt.Printf("Patched Content: %s\n", string(patchedContent))

	fmt.Println()
	fmt.Println("=== Example 5: Hide And Purge ===")
	if err := hub.DeleteFile(project, fileMeta.Name); err != nil {
		log.Fatalf("Failed to hide file metadata: %v", err)
	}
	fmt.Println("File removed from metadata history head; assets still exist.")
	purge, err := hub.PurgeUntracked(project)
	if err != nil {
		log.Fatalf("Failed to purge untracked data: %v", err)
	}
	fmt.Printf("Purged %d releases and %d assets.\n", purge.DeletedReleases, purge.DeletedAssets)
	fmt.Println()

	fmt.Println("All examples completed successfully.")
}

// Example: Upload a large file (chunked automatically when it exceeds the configured chunk size).
func exampleLargeFile() {
	token := os.Getenv("GITHUB_TOKEN")
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatalf("Failed to initialize StorHub: %v", err)
	}

	largeFile := "/path/to/large/file.zip"
	project := "my-storage"

	fmt.Printf("Uploading large file: %s\n", largeFile)
	fileMeta, err := hub.UploadFile(project, "file.zip", largeFile)
	if err != nil {
		log.Fatalf("Failed to upload: %v", err)
	}

	if len(fileMeta.Chunks) > 1 {
		fmt.Printf("File was chunked into %d parts\n", len(fileMeta.Chunks))
		for i, chunk := range fileMeta.Chunks {
			fmt.Printf("  Chunk %d: %s (%d bytes)\n", i+1, chunk.Name, chunk.Size)
		}
	}
}

// Example: Upload multiple files.
func exampleBatchUpload() {
	token := os.Getenv("GITHUB_TOKEN")
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatalf("Failed to initialize StorHub: %v", err)
	}

	project := "my-storage"
	files := []string{"file1.txt", "file2.txt", "file3.txt"}

	fmt.Printf("Uploading %d files to %s...\n", len(files), project)

	for i, file := range files {
		fmt.Printf("[%d/%d] Uploading %s...\n", i+1, len(files), file)

		fileMeta, err := hub.UploadFile(project, file, file)
		if err != nil {
			fmt.Printf("  ✗ Failed: %v\n", err)
			continue
		}

		fmt.Printf("  Uploaded to release %s\n", fileMeta.Release)
	}

	fmt.Println("Batch upload completed.")
}
