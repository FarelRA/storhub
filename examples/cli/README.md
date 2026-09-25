# CLI Example

Design note: this directory intentionally has no `main.go`. The CLI
ships as `cmd/storhub`; this example drives that binary from
`demo.sh` via `go run ./cmd/storhub` so it always exercises the
current source tree. The full CLI surface is pinned by the
`internal/cli` tests and the `SurfaceCLI` conformance scenarios in
`internal/test`, not by this script.

This example shows how to use the `storhub` CLI from a shell script.

What it teaches:

- how to run the CLI with `go run ./cmd/storhub`
- how to set `GITHUB_TOKEN` once and reuse it across commands
- how common CLI flows map to the storage, filesystem, revision, and mount features

Why this example exists:

- some users want a terminal-first workflow instead of embedding the Go API
- it gives you a copyable shell session you can adapt into your own scripts or automation

How to use it:

```bash
chmod +x ./examples/cli/demo.sh
GITHUB_TOKEN=your_token ./examples/cli/demo.sh demo-project
```

What `demo.sh` runs:

- `mkdir` (including a nested path)
- `upload`, `replace`, `patch`, `download`
- `ls`, `stat`, `cat`
- `mv`, `rm`
- `project revisions`

Referenced but not executed by the script:

- `rest` and `serve`: see `examples/cli/rest-auth.json`, a starter
  auth file for `storhub rest --authfile` and `storhub serve --authfile`
- `mount`: echoed as a copyable command when `STORHUB_MOUNT_POINT` is set
- `project rollback`: covered by the `examples/revisions` Go example
  and the `internal/cli` tests, not by this script

Notes:

- the script expects one positional argument: the project name
- if `STORHUB_MOUNT_POINT` is set, it will also show the mount command to run
- the script uses `go run ./cmd/storhub` directly so it stays in sync with the current source tree
- `examples/cli/rest-auth.json` is a starter auth file for `storhub rest --authfile` and `storhub serve --authfile`
- the sample file stores the admin credential as plaintext `"password"`; the
  server hashes it at load and clears the field. The equivalent Go form is
  `PasswordHash: storhub.HashRESTPassword(password)` (JSON key
  `"password_hash"`), as used in `examples/rest-auth`. Both spellings load;
  the plaintext is cleared right after hashing, so it is never stored back
