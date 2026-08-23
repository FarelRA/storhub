// Command fuse-mount demonstrates mounting a StorHub project as a FUSE
// filesystem. Configuration comes from the environment:
//
//	GITHUB_TOKEN          GitHub personal access token (required)
//	STORHUB_MOUNT_POINT   directory to mount on (required)
//	STORHUB_PROJECT       project name (required)
//
// The example unmounts cleanly on SIGINT/SIGTERM, retrying briefly if files
// are still open.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	// Arm signals before mounting so an interrupt during setup cannot leave
	// a half-attached mount behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := fsys.Mount(mountPoint); err != nil {
		log.Fatal(err)
	}
	defer fsys.Unmount()
	fmt.Printf("mounted %s at %s\n", project, mountPoint)
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		fsys.Wait()
	}()
	select {
	case <-waitDone:
		return
	case <-ctx.Done():
		stop()
		unmountWithRetry(fsys, mountPoint)
		<-waitDone
	}
}

func unmountWithRetry(fsys *storfuse.Filesystem, mountPoint string) {
	delay := time.Second
	deadline := time.Now().Add(30 * time.Second)
	for attempt := 1; ; attempt++ {
		err := fsys.Unmount()
		if err == nil {
			return
		}
		log.Printf("unmount failed (%v); close programs using %s and wait, or press Ctrl+C again to quit", err, mountPoint)
		if time.Now().After(deadline) {
			log.Printf("giving up on unmount after %d attempts; %s may still be mounted", attempt, mountPoint)
			return
		}
		time.Sleep(delay)
		if delay < 8*time.Second {
			delay *= 2
		}
	}
}
