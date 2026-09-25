# Files Example

This example focuses on the core file-storage flow.

What it teaches:

- upload a local file into StorHub
- replace an existing logical file
- patch part of a file without rebuilding everything manually
- list files and releases
- download the stored file back

Why this example exists:

- many users want the storage layer first, before filesystem or POSIX features
- it shows the simplest path from local file to GitHub-backed immutable storage

How to run it:

```bash
GITHUB_TOKEN=your_token go run ./examples/files
```

Public APIs highlighted:

- `storhub.NewStorHubWithContext`
- `(*StorHub).UploadFileContext`
- `(*StorHub).ReplaceFileContext`
- `(*StorHub).PatchFileContext`
- `(*StorHub).ListFilesContext`
- `(*StorHub).ListReleasesContext`
- `(*StorHub).DownloadFileContext`
