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
- Supports upload, replace, patch, append, truncate, and download
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
- inspection: `ls`, `stat`, `cat`, `revisions` (all but `cat` accept `--json` for stable machine-readable output)
- filesystem: `mkdir`, `mv`, `rm`
- recovery and cleanup: `rollback`
- web: `serve-rest` (drains in-flight requests and flushes metadata on SIGINT/SIGTERM)
- mount: `mount`
- `cat` streams through a fixed 1 MiB window, so piping multi-GB files never buffers them whole; `revisions` prints nothing for empty history, like `ls(1)`

Typical workflow:

```bash
GITHUB_TOKEN=your_token go run ./cmd/storhub mkdir demo-project docs/specs
GITHUB_TOKEN=your_token go run ./cmd/storhub upload demo-project docs/specs/guide.txt ./guide.txt
GITHUB_TOKEN=your_token go run ./cmd/storhub patch demo-project docs/specs/guide.txt 0 0 "v2: "
GITHUB_TOKEN=your_token go run ./cmd/storhub revisions demo-project
```

`append`, `write`, and `patch` accept `-` as the data argument to read the
payload from stdin:

```bash
echo "more text" | GITHUB_TOKEN=your_token go run ./cmd/storhub append demo-project docs/specs/guide.txt -
```

Exit codes follow shell convention: `0` on success, `1` when a well-formed
command fails at runtime, and `2` when the command line itself is wrong
(unknown flags, missing arguments).

Environment variables: `GITHUB_TOKEN` (authentication),
`STORHUB_LOG_LEVEL` / `STORHUB_LOG_FORMAT` / `STORHUB_LOG_COLOR`
(default level is `info`), and `STORHUB_API_BASE_URL`.

For a shell-first walkthrough, see `examples/cli/demo.sh` and `examples/cli/README.md`.

REST serving from the CLI:

```bash
# With authentication (recommended):
GITHUB_TOKEN=your_token go run ./cmd/storhub serve-rest --listen :8080 --auth-file ./rest-auth.json

# Deliberately unauthenticated (insecure; requires the explicit flag):
GITHUB_TOKEN=your_token go run ./cmd/storhub serve-rest --listen :8080 --allow-anonymous
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

Filesystem-style APIs:

- `Mkdir`, `CreateFile`, `WriteFileAt`, `AppendFile`, `ReadFileAt`
- `Rename`, `TruncateFile`, `ReadDir`, `StatPath`, `StatFS`
- `DeleteFile`, `Rmdir`

POSIX-style APIs:

- `Chmod`, `Chown`, `Chtimes`
- `Symlink`, `Readlink`, `Link`
- `SetXAttr`, `GetXAttr`, `ListXAttr`, `RemoveXAttr`

Precondition (compare-and-swap) APIs:

- `(*StorHub).RevisionContext` - current remote metadata revision
- `fs.WithExpectedRevision(rev)` as a trailing option on `PatchFileContext`,
  `TruncateFileContext`, `AppendFileContext`, `WriteFileAtContext`,
  `DeleteFileContext`, `ReplaceFileContext`, and `ReplaceFileFromReaderContext`;
  the mutation fails with `fs.ErrPreconditionFailed` when remote HEAD moved

POSIX conformance notes:

- `Chown`/`ChownContext` accept `(uid_t)-1` (Go `^uint32(0)`) per field as
  POSIX "leave this owner unchanged"
- metadata-patch timestamps are authoritative: patching mtime to the epoch
  persists (an additive marker keeps legacy zero-fill repair for old entries);
  `Chtimes` keeps its omit-on-zero contract for library callers;
  `ChtimesExplicitContext(atime, mtime *time.Time)` expresses utimensat
  trinary semantics exactly (nil omits, non-nil sets - epoch included),
  and FUSE `utimens` routes through it so kernel-explicit zeros survive.
  Filenames are byte-honest: surrounding whitespace is significant
  everywhere (`" docs "` is one specific name), enforced by a
  conformance test pinning both normalizers together. Metadata is
  schema v4 with unambiguous timestamp keys (cr=created, ch=changed);
  v3 repositories migrate transparently on load
- FUSE advisory locks are dropped when a file's last open descriptor closes
  (POSIX last-close guarantee); per-fd close semantics depend on go-fuse
  surfacing `FUSE_RELEASE`'s lock owner, which v2.11 does not

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
- `GET|HEAD /api/v1/projects/{project}/children?path=...` - directory listing
- `GET|HEAD|PUT|PATCH /api/v1/projects/{project}/content?path=...` - streamed reads plus replace, append, write, patch, and truncate workflows. Conditional `If-Match` requests are re-verified immediately before mutation and fail with `412` on concurrent change; `append`/`write` bodies are applied atomically and capped (larger transfers belong in a full-file PUT, which answers `413` beyond the cap)
- `If-Match` accepts two token flavors: classic attribute ETags (freshness re-check) or the project's metadata revision published as `X-StorHub-Revision` on node/content reads. A current revision token upgrades the guard to true compare-and-swap - storage re-verifies against remote HEAD right before applying, so a stale revision fails `412` even when attributes coincide
- `GET /api/v1/projects/{project}/xattrs?path=...` and `GET|PUT|DELETE /api/v1/projects/{project}/xattrs/value?...` - extended attribute inspection and mutation
- `POST /api/v1/projects/{project}/ops/...` - mkdir, create-file, rename, link, symlink, chmod, chown, utimes, rollback
- `POST /api/v1/projects/{project}/ops/share` answers `201` with a `Location` header pointing at the created share resource, and `DELETE` of a share answers `204`, matching the API's other create/delete conventions; share lifetimes are clamped to the configured maximum (7 days by default). Share URLs carry a short opaque identifier (`/shares/{id}`, `/shares/{id}/download`) rather than the signed token, so links leak no credentials; previously issued token-shaped links keep working until expiry or revocation. The creation response alone returns the signed JWT for programmatic bearer use - listings never include it
- `GET /api/v1/projects/{project}/revisions` - metadata revision history

Authenticated REST:

- login is `POST /api/v1/auth/login` with `username` and `password`; unknown users are answered in constant work so login timing cannot enumerate accounts
- successful login returns a bearer token with the resolved StorHub identity (`uid`, `primary_gid`, `groups`, `admin`), which is enforced by every downstream POSIX permission check
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

- `ETag` is returned on node and content reads; `X-StorHub-Revision` publishes the project's metadata revision
- `If-Match` guards every mutating endpoint - file and directory deletes, replaces, appends, writes, patches, and truncates alike. A current-revision token strengthens the guard into true compare-and-swap enforced at apply time; tokens may be quoted per RFC 9110
- `If-None-Match: *` supports create-only full-file uploads
- `Range: bytes=...` supports partial reads for large files

## Examples

Every example deletes the GitHub repository it created once it finishes -
including on failures and Ctrl+C - so demo runs never litter your account.



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
- `internal/chunking` - chunk planning helpers
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

## Design Notes

- **Sequential transfers**: exactly one HTTP request is in flight at any
  time. Uploads and downloads are deterministic and retry-safe; the
  metadata commit loop and cache janitors are the only background work.
- **Chunk integrity**: uploads record a per-chunk SHA-256 digest; whole-
  chunk downloads verify it and fail loudly on bit rot. `purge` reclaims
  orphaned remote assets and prunes the chunk catalog in the same breath,
  so metadata cannot grow without bound.
- **Metadata schema v3**: strict version handling (no silent migrations of
  corrupt payloads), byte-valued extended attributes, offset-ordered chunk
  lists, persisted inode/chunk counters, and real zero values (UID/GID 0
  means root).
- **POSIX semantics**: `mv` replaces targets atomically, symlinks traverse
  (with ELOOP protection), directory link counts follow POSIX, permission
  checks are re-verified inside each mutation transaction, errno fidelity
  is preserved end to end, and an absent caller identity resolves to the
  local process user — never anonymous root.
- **Fail loudly**: invalid configurations are rejected at construction;
  operational failures are logged at error level and never silently
  swallowed; upstream GitHub errors reach clients as sanitized 502s;
  handler panics become clean 500s with server-side stack traces.
- **Bounded resources**: FUSE nodes evicted on kernel FORGET, share
  registries sweep expired entries, REST mutation bodies are capped
  (larger payloads belong to full-file PUT), and the UI ships a vendored
  Alpine.js with session-scoped token storage.
