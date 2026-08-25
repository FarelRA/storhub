# CLI Example

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

What it demonstrates:

- `upload`, `replace`, `patch`, `download`
- `ls`, `stat`, `cat`
- `mkdir`, `mv`, `rm`
- `revisions`, `rollback`
- `rest`
- `serve` (mount + REST together)
- optional `mount`

Notes:

- the script expects one positional argument: the project name
- if `STORHUB_MOUNT_POINT` is set, it will also show the mount command to run
- the script uses `go run ./cmd/storhub` directly so it stays in sync with the current source tree
- `examples/cli/rest-auth.json` is a starter auth file for `storhub rest --auth-file` and `storhub serve --auth-file`
