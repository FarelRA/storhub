# Filesystem Example

This example focuses on the filesystem-style API.

What it teaches:

- directory creation and removal
- creating logical files directly in StorHub
- random writes, append, read-at, truncate, and rename
- stat, readdir, and statfs inspection

Why this example exists:

- some users think in terms of paths and file operations, not upload/replace flows
- it shows how StorHub behaves like a logical filesystem even without mounting FUSE

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/filesystem
```

Public APIs highlighted:

- `(*StorHub).MkdirContext`
- `(*StorHub).CreateFileContext`
- `(*StorHub).WriteFileAtContext`
- `(*StorHub).AppendFileContext`
- `(*StorHub).ReadFileAtContext`
- `(*StorHub).TruncateFileContext`
- `(*StorHub).RenameContext`
- `(*StorHub).ReadDirContext`
- `(*StorHub).StatPathContext`
- `(*StorHub).StatFSContext`
- `(*StorHub).DeleteFileContext`
- `(*StorHub).RmdirContext`
