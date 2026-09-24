# Contributing to StorHub

## Branch protection on `main` (maintainer action, one time)

CI alone is not a merge gate: without branch protection, a merge can land
on `main` with zero `ci` check runs. The `ci` workflow has already proven
it bites (run 34910039786 failed all Go jobs on 4d9c77a), so it must be
required.

This cannot be set from a file in the repo. A maintainer with admin rights
must enable it once, via the UI (Settings, Branches, Branch protection
rules) or the API:

```bash
gh api -X PUT "repos/FarelRA/storhub/branches/main/protection" \
  -H "Accept: application/vnd.github+json" \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["lint", "web", "test-amd64", "race-amd64", "test-arm64", "race-arm64", "build-cross", "test-examples"]
  },
  "required_pull_request_reviews": {
    "required_approving_review_count": 1
  },
  "enforce_admins": true,
  "restrict_pushes": false,
  "restrict_deletions": true
}
JSON
```

Notes:

- `benchmark-budgets` runs in CI but is deliberately not in the required
  contexts above: budget enforcement is timing-sensitive and advisory, so a
  red budgets run deserves a look before merge but never blocks one on its
  own. Promoting it to required needs an explicit maintainer decision.
- `strict: true` requires branches to be up to date before merging.
- Dependabot PRs only run the `ci` workflow if the repository settings allow
  it: Settings, Code security and analysis, Dependabot, "Automatically
  approve dependencies that update version numbers" is unrelated; what
  matters is that `ci` triggers on `pull_request` (it does) and that the
  protection rule's required contexts are satisfied before merge. If
  Dependabot PRs show no checks at all, verify the workflow permissions for
  the Dependabot bot under Settings, Actions, General, Workflow
  permissions.
- A CI-side check for "protection absent" was deliberately not added: the
  default `GITHUB_TOKEN` is scoped `contents: read` and cannot read the
  protection endpoint, so such a check would fail spuriously or require a
  privileged PAT in CI, which is worse than the gap it polices.

## What CI does not run

- `STORHUB_RUN_FUSE=1` mounted FUSE smoke tests: CI runners lack a usable
  FUSE setup, so mount lifecycle is human-run only.
- `STORHUB_RUN_LIVE=1` / `STORHUB_RUN_LIVE_LARGE=1` live GitHub tests: they
  need a real token and create real repositories; human-run only.
- `govulncheck` runs in the `nightly` workflow, not in `ci`. A vulnerable
  dependency bump can therefore sit on `main` for up to about a day. This is
  a deliberate trade-off (private repo, no long-lived secret in CI); the
  nightly badge on the README is the signal to watch.

## Console embed workflow

`internal/rest/static/dist` is a committed build artifact that `go build`
and `docker build` ship directly. After ANY change under `web/`:

```bash
cd web
bun install
bun run build:embed
```

and commit the regenerated `dist` alongside the source change. The `web` CI
job rebuilds the embed from `web/` source but only asserts the fresh bundle
is non-empty (`index.html` plus at least one `_nuxt/*.js` chunk): the build
is not hermetic (`index.html` embeds a random buildId and chunk hashes drift
by arch and toolchain), so a byte-for-byte drift gate would false-fail. Only
the hashed `_nuxt/*` filenames are stable across rebuilds.

## Commit hygiene

- Keep subjects at or under 72 characters.
- No dash-as-punctuation in user-facing text (README, console strings,
  commit messages): recast with commas, colons, parentheses, or periods.
- Commit bodies should explain WHY; golden rule from the maintainers: a body
  that overclaims (for example "rebuilds the committed embed" on a commit
  that only touches `web/`) is worse than no body.

## Running the gates locally

```bash
go test -count=1 ./...
go test -race -count=1 ./...
gofmt -l .
go vet ./...
go mod tidy -diff
golangci-lint run
shellcheck examples/cli/demo.sh scripts/install.sh
cd web && bun install --frozen-lockfile && bun run lint && bun run test && bun run typecheck && bun run build:embed
# Same non-empty guard the `web` CI job asserts (never a byte-for-byte diff:
# the bundle is not hermetic, see above).
test -s internal/rest/static/dist/index.html
chunks=$(ls internal/rest/static/dist/_nuxt/*.js 2>/dev/null | wc -l | tr -d ' ')
test "${chunks:-0}" -ge 1
```
