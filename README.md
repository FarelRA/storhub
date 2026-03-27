# StorHub

StorHub is a Go library and CLI for storing files in GitHub repositories while exposing a logical filesystem-style view over that content. It stores file data as GitHub release assets and keeps the logical catalog in `.storhub/metadata.json`.

## What StorHub Is

StorHub is designed for teams that want:

- immutable chunk-backed file storage on top of GitHub
- a structured logical filesystem view instead of raw release assets
- revision history and rollback for metadata changes
- optional FUSE mounting for POSIX-like access
- both a Go API and a CLI

StorHub is a good fit for documents, artifacts, build outputs, datasets, archives, and light mounted access.

StorHub is not intended to replace a local SSD filesystem or a database storage engine.

## Key Features

- Stores file content in GitHub release assets
- Uses `.storhub/metadata.json` as the logical source of truth
- Supports upload, replace, patch, append, truncate, download, and integrity verification
- Exposes filesystem-style operations such as create, rename, readdir, stat, and delete
- Tracks POSIX-like metadata including mode, uid, gid, timestamps, symlinks, hardlinks, and xattrs
- Supports metadata revision history, rollback, cleanup, and purge operations
- Provides a public FUSE facade for mounted access
- Includes a CLI for terminal-first workflows

## Installation

Requirements:

- Go 1.21+
- a GitHub token with repository access
- Linux with FUSE support and `fusermount3` if you want mounted access

Clone the repository:

```bash
git clone https://github.com/FarelRA/storhub.git
cd storhub
```

Use as a library:

```bash
go get github.com/FarelRA/storhub
```

Run the CLI directly from source:

```bash
go run ./cmd/storhub --help
```

## Quick Start

Minimal Go example:

```go
package main

import (
	"log"

	"github.com/FarelRA/storhub/storhub"
)

func main() {
	token := "your_github_token"
	hub, err := storhub.NewStorHub(token)
	if err != nil {
		log.Fatal(err)
	}

	meta, err := hub.UploadFile("demo-project", "docs/readme.txt", "./README.md")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("uploaded %s (%d bytes) in release %s", meta.Name, meta.Size, meta.Release)
}
```

Minimal CLI example:

```bash
GITHUB_TOKEN=your_token go run ./cmd/storhub upload demo-project docs/readme.txt ./README.md
GITHUB_TOKEN=your_token go run ./cmd/storhub ls demo-project docs
GITHUB_TOKEN=your_token go run ./cmd/storhub stat demo-project docs/readme.txt
```

## CLI Guide

Run help:

```bash
go run ./cmd/storhub --help
```

Common commands:

- storage: `upload`, `replace`, `download`, `patch`, `append`, `write`
- inspection: `ls`, `stat`, `cat`, `revisions`
- filesystem: `mkdir`, `mv`, `rm`
- recovery and cleanup: `rollback`
- web: `serve-rest`
- mount: `mount`

Typical workflow:

```bash
GITHUB_TOKEN=your_token go run ./cmd/storhub mkdir demo-project docs/specs
GITHUB_TOKEN=your_token go run ./cmd/storhub upload demo-project docs/specs/guide.txt ./guide.txt
GITHUB_TOKEN=your_token go run ./cmd/storhub patch demo-project docs/specs/guide.txt 0 0 "v2: "
GITHUB_TOKEN=your_token go run ./cmd/storhub revisions demo-project
```

For a shell-first walkthrough, see `examples/cli/demo.sh` and `examples/cli/README.md`.

REST serving from the CLI:

```bash
GITHUB_TOKEN=your_token go run ./cmd/storhub serve-rest --listen :8080
GITHUB_TOKEN=your_token go run ./cmd/storhub serve-rest --listen :8080 --auth-file ./rest-auth.json
```

Open `http://localhost:8080/ui` for the built-in Alpine.js file browser and console.

## API Guide

Public packages:

- `github.com/FarelRA/storhub/storhub`
- `github.com/FarelRA/storhub/fuse`
- `github.com/FarelRA/storhub/rest`

Constructors:

- `storhub.NewStorHub`
- `storhub.NewStorHubWithConfig`
- `storhub.NewStorHubWithContext`

Core storage APIs:

- `UploadFile`, `ReplaceFile`, `PatchFile`, `DownloadFile`
- `ListFiles`, `ListReleases`
- `VerifyFileIntegrity`, `CombineChunkCRC32Cs`

Filesystem-style APIs:

- `Mkdir`, `CreateFile`, `WriteFileAt`, `AppendFile`, `ReadFileAt`
- `Rename`, `TruncateFile`, `ReadDir`, `StatPath`, `StatFS`
- `DeleteFile`, `Rmdir`

POSIX-style APIs:

- `Chmod`, `Chown`, `Chtimes`
- `Symlink`, `Readlink`, `Link`
- `SetXAttr`, `GetXAttr`, `ListXAttr`, `RemoveXAttr`

Revision and maintenance APIs:

- `ListMetadataRevisions`
- `RollbackMetadata`
- `PurgeUntracked`
- `CleanupProject`
- `DeleteRelease`
- `DeleteProject`

FUSE APIs:

- `storhub.DefaultFUSEOptions`
- `(*StorHub).NewFUSE`
- `fuse.DefaultOptions`
- `fuse.New`

REST APIs:

- `github.com/FarelRA/storhub/rest`
- `rest.DefaultOptions`
- `rest.New`
- `rest.HashPassword`

Minimal REST server:

```go
package main

import (
	"log"
	"net/http"
	"os"

	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
)

func main() {
	hub, err := storhub.NewStorHub(os.Getenv("GITHUB_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}

	handler, err := shrest.New(hub, shrest.DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

The handler also serves a browser UI at `/` and `/ui`.

REST endpoint groups:

- `GET /api/v1/projects/{project}` - project stats
- `GET|HEAD|DELETE /api/v1/projects/{project}/nodes?path=...` - stat or remove files and empty directories
- `GET /api/v1/projects/{project}/children?path=...` - directory listing
- `GET|HEAD|PUT|PATCH /api/v1/projects/{project}/content?path=...` - streamed reads plus replace, append, write, patch, and truncate workflows
- `GET /api/v1/projects/{project}/xattrs?path=...` and `GET|PUT|DELETE /api/v1/projects/{project}/xattrs/value?...` - extended attribute inspection and mutation
- `POST /api/v1/projects/{project}/ops/...` - mkdir, create-file, rename, link, symlink, chmod, chown, utimes, rollback
- `POST /api/v1/projects/{project}/ops/share`, `GET /shares/{token}`, and `GET /shares/{token}/download` - shareable UI links with optional direct downloads
- `GET /api/v1/projects/{project}/revisions` - metadata revision history

Authenticated REST:

- login is `POST /api/v1/auth/login` with `username` and `password`
- successful login returns a bearer token with the resolved StorHub identity (`uid`, `primary_gid`, `groups`, `admin`)
- authenticated requests send `Authorization: Bearer <token>`
- authorization uses StorHub owner/group/mode metadata, so REST operations follow UNIX-style checks instead of a separate ACL model
- directory traversal requires execute/search permission on each ancestor directory
- create, unlink, rename, and rmdir operations are authorized from parent directory write+execute permission
- `chown`, rollback, and project deletion are restricted to admin identities

Minimal authenticated REST setup:

```go
package main

import (
	"log"
	"net/http"
	"os"

	shrest "github.com/FarelRA/storhub/rest"
	"github.com/FarelRA/storhub/storhub"
)

func main() {
	hub, err := storhub.NewStorHub(os.Getenv("GITHUB_TOKEN"))
	if err != nil {
		log.Fatal(err)
	}

	adminHash, err := shrest.HashPassword("change-me")
	if err != nil {
		log.Fatal(err)
	}

	opts := shrest.DefaultOptions()
	opts.Auth = &shrest.AuthOptions{
		TokenSigningKey: []byte(os.Getenv("STORHUB_REST_SIGNING_KEY")),
		Users: []shrest.User{{
			Username:     "admin",
			PasswordHash: adminHash,
			UID:          0,
			PrimaryGID:   0,
			Admin:        true,
		}},
	}

	handler, err := shrest.New(hub, opts)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

The REST handler uses HTTP preconditions where they help UNIX-like workflows:

- `ETag` is returned on node and content reads
- `If-Match` guards writes, patches, and deletes against stale metadata
- `If-None-Match: *` supports create-only full-file uploads
- `Range: bytes=...` supports partial reads for large files

## Examples

Full showcase:

```bash
GITHUB_TOKEN=your_token go run ./examples/showcase
```

Focused examples:

```bash
GITHUB_TOKEN=your_token go run ./examples/files
GITHUB_TOKEN=your_token ./examples/cli/demo.sh demo-project
GITHUB_TOKEN=your_token go run ./examples/rest
GITHUB_TOKEN=your_token STORHUB_REST_ADMIN_PASSWORD=change-me STORHUB_REST_SIGNING_KEY=signing-secret go run ./examples/rest-auth
GITHUB_TOKEN=your_token go run ./examples/filesystem
GITHUB_TOKEN=your_token go run ./examples/posix
GITHUB_TOKEN=your_token go run ./examples/revisions
GITHUB_TOKEN=your_token STORHUB_PROJECT=demo STORHUB_MOUNT_POINT=./mnt go run ./examples/fuse-mount
```

Example overview:

- `examples/showcase` - broad end-to-end walkthrough across the public API surface
- `examples/files` - storage upload/replace/patch/download flow
- `examples/cli` - shell-based CLI workflow
- `examples/rest` - unauthenticated REST server setup
- `examples/rest-auth` - authenticated REST server setup with bearer login
- `examples/filesystem` - filesystem-style API usage
- `examples/posix` - POSIX-like metadata usage
- `examples/revisions` - revision history, rollback, purge, and cleanup
- `examples/fuse-mount` - public FUSE facade and mount lifecycle

Each example directory includes its own `README.md` explaining what it teaches, why it exists, and how to run it.

## How StorHub Works

At a high level:

1. file content is chunked and stored as GitHub release assets
2. StorHub updates `.storhub/metadata.json` to describe the logical filesystem state
3. all path lookups, metadata inspection, links, timestamps, and revisions come from that metadata catalog
4. mounted FUSE access uses the same logical model underneath

This means the logical filesystem view is stable even though the underlying storage is built from immutable GitHub asset objects.

## Architecture

Public surface:

- `storhub/` - main library API
- `fuse/` - public FUSE facade
- `rest/` - public REST facade

Internal layout:

- `internal/config` - config defaults and transport tuning
- `internal/github` - real GitHub API client, transport, and request handling
- `internal/metadata` - metadata model, normalization, indexing, and validation
- `internal/chunking` - chunk planning and CRC32C integrity helpers
- `internal/storage` - high-level StorHub workflows and orchestration
- `internal/fs` - filesystem-style operations and path logic
- `internal/posix` - POSIX-like metadata operations
- `internal/fusefs` - concrete FUSE implementation
- `internal/rest` - concrete REST handlers, auth, and UNIX-style authorization
- `internal/cli` - CLI command parsing and rendering

Storage model:

- file data: GitHub release assets
- logical catalog: `.storhub/metadata.json`
- history: Git commit history of the metadata file
- rollback: restore an earlier metadata revision

Writeback model:

- small localized changes can use patch-style writeback
- append and truncate paths are optimized separately
- fragmented writes can switch to chunk-rewrite mode
- heavy rewrites can still fall back to full replacement when cheaper or simpler

## Testing

The test suite is grouped into three categories:

- `unit` - pure logic and package-local workflows
- `mock` - fake-backed integration tests without real GitHub traffic
- `smoke` - gated tests for mounted FUSE and real GitHub behavior

Direct commands:

```bash
go test ./storhub ./fuse ./rest ./cmd/storhub ./internal/config ./internal/chunking ./internal/fs ./internal/github ./internal/metadata ./internal/posix
go test ./internal/storage ./internal/fusefs ./internal/rest ./internal/cli ./examples/...
STORHUB_RUN_FUSE=1 go test ./internal/storage -run 'TestFUSEOptionalMountLifecycle$'
GITHUB_TOKEN=ghp_xxx STORHUB_RUN_LIVE=1 go test ./internal/storage -run 'TestLiveGitHub'
go test ./...
go test -race ./...
go vet ./...
```

Environment gates:

- `STORHUB_RUN_FUSE=1` enables mounted FUSE smoke tests
- `STORHUB_RUN_LIVE=1` enables live GitHub smoke tests
- `STORHUB_RUN_LIVE_LARGE=1` enables large live transfer smoke tests
- `GITHUB_TOKEN` is required for live GitHub smoke tests

## Limitations

- StorHub aims for practical POSIX-like behavior, not perfect full POSIX filesystem fidelity.
- It is not optimized for database files or very latency-sensitive small random writes.
- External out-of-band repository mutations can still create cache coherence challenges.
- Large metadata-heavy workloads can be slower than native local filesystems.
- FUSE support is best for convenience and integration, not as a replacement for a native disk filesystem.

## License

This project is licensed under the GNU General Public License v3.0. See `LICENSE`.
