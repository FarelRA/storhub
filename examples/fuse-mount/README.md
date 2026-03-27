# FUSE Mount Example

This example focuses on the public `fuse` facade.

What it teaches:

- how to build a FUSE filesystem through the public package instead of internal code
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

- `fuse.DefaultOptions`
- `fuse.New`
- `(*storhub.StorHubFS).Mount`
- `(*storhub.StorHubFS).Wait`
- `(*storhub.StorHubFS).Unmount`
- `(*storhub.StorHubFS).Close`
