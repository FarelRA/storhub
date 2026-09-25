# FUSE Mount Example

This example focuses on the public FUSE surface on the `storhub` package.

What it teaches:

- how to build a FUSE filesystem through `storhub.DefaultFUSEOptions`
  and `(*StorHub).NewFUSE` instead of internal code
- how to use default mount options
- how to mount, wait, and unmount a StorHub project

Why this example exists:

- mounting is a separate use case from direct API calls
- users who only care about FUSE should not have to read the full showcase first

How to run it:

```bash
GITHUB_TOKEN=your_token STORHUB_PROJECT=demo STORHUB_MOUNT_POINT=./mnt go run ./examples/fuse-mount
```

Public APIs highlighted:

- `storhub.DefaultFUSEOptions`
- `(*StorHub).NewFUSE`
- `(*storhub.FS).Mount`
- `(*storhub.FS).Wait`
- `(*storhub.FS).Unmount`
- `(*storhub.FS).Close`

Sync semantics: an fsync on a file renamed after it was opened reports
ENOENT. The data is safely staged and retried at the new name; the error
only says the old path no longer resolves.
