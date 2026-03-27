#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: GITHUB_TOKEN=... $0 <project>" >&2
  exit 1
fi

if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "GITHUB_TOKEN is required" >&2
  exit 1
fi

PROJECT="$1"
SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"
CLI=(go run ./cmd/storhub)

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cat >"$TMP_DIR/readme-v1.txt" <<'EOF'
hello from storhub cli example
EOF

cat >"$TMP_DIR/readme-v2.txt" <<'EOF'
hello from storhub cli example version two
EOF

cd "$REPO_ROOT"

echo
echo "== Create directories =="
"${CLI[@]}" mkdir --token "$GITHUB_TOKEN" "$PROJECT" docs
"${CLI[@]}" mkdir --token "$GITHUB_TOKEN" "$PROJECT" docs/specs

echo
echo "== Upload and replace =="
"${CLI[@]}" upload --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt "$TMP_DIR/readme-v1.txt"
"${CLI[@]}" replace --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt "$TMP_DIR/readme-v2.txt"

echo
echo "== Patch and inspect =="
"${CLI[@]}" patch --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt 6 4 there
"${CLI[@]}" ls --token "$GITHUB_TOKEN" "$PROJECT" docs/specs
"${CLI[@]}" stat --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt
"${CLI[@]}" cat --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt

echo
echo "== Rename and download =="
"${CLI[@]}" mv --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/readme.txt docs/specs/guide.txt
"${CLI[@]}" download --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/guide.txt "$TMP_DIR/downloaded.txt"
cat "$TMP_DIR/downloaded.txt"

echo
echo "== Revisions =="
"${CLI[@]}" revisions --token "$GITHUB_TOKEN" "$PROJECT"

echo
echo "== Cleanup one file =="
"${CLI[@]}" rm --token "$GITHUB_TOKEN" "$PROJECT" docs/specs/guide.txt

if [ -n "${STORHUB_MOUNT_POINT:-}" ]; then
  echo
  echo "== Optional mount command =="
  echo "go run ./cmd/storhub mount --token \"$GITHUB_TOKEN\" \"$PROJECT\" \"$STORHUB_MOUNT_POINT\""
fi
