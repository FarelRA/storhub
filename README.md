# StorHub

StorHub is a Go library for immutable GitHub-backed storage.

- File bytes live in GitHub release assets.
- Active catalog state lives in `.storhub/metadata.json` committed to the repo.
- Transfers stream and verify CRC32C end to end.
- Patching reuses unchanged asset ranges instead of reuploading whole files.
- Filesystem-like operations are available without FUSE wiring.

## What it supports

- Upload, replace, download, delete, purge, and project cleanup.
- Immutable patch operations with insert, delete, replace, grow, and shrink semantics.
- Directory-aware operations: `Mkdir`, `Rmdir`, `Rename`, `CreateFile`, `ReadFileAt`, `WriteFileAt`, `AppendFile`, `TruncateFile`, `ReadDir`, `StatPath`, and `StatFS`.
- Metadata revision listing and rollback.
- Private-by-default repository creation.

## Behavior notes

- `DeleteFile` removes the file from active metadata only.
- `PurgeUntracked` is destructive and can invalidate rollback to older metadata that still references purged assets.
- Missing projects return `project not found` instead of looking like empty storage.
- Client construction is lazy: authentication is resolved on first API use.

## Basic usage

```go
hub, err := storhub.NewStorHub(token)
if err != nil {
    return err
}

meta, err := hub.UploadFile("my-project", "docs/readme.txt", "/tmp/readme.txt")
if err != nil {
    return err
}

_, err = hub.PatchFile("my-project", "docs/readme.txt", 10, 3, []byte("updated"))
if err != nil {
    return err
}

err = hub.DownloadFile("my-project", meta.Name, "/tmp/out.txt")
if err != nil {
    return err
}
```

See `example/main.go` for a runnable example.
