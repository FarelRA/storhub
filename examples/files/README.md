# Files Example

This example focuses on the core file-storage flow.

What it teaches:

- upload a local file into StorHub
- replace an existing logical file
- patch part of a file without rebuilding everything manually
- list files and releases
- download and verify file integrity

Why this example exists:

- many users want the storage layer first, before filesystem or POSIX features
- it shows the simplest path from local file to GitHub-backed immutable storage

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/files
```

Public APIs highlighted:

- `storhub.NewStorHub`
- `(*StorHub).UploadFile`
- `(*StorHub).ReplaceFile`
- `(*StorHub).PatchFile`
- `(*StorHub).ListFiles`
- `(*StorHub).ListReleases`
- `(*StorHub).DownloadFile`
- `storhub.VerifyFileIntegrity`
- `storhub.CombineChunkCRC32Cs`
