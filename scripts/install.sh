#!/usr/bin/env bash
# StorHub installer: fetches the latest release (or the rolling nightly)
# for this platform, verifies the SHA256 checksum, and installs the binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/FarelRA/storhub/main/scripts/install.sh | bash
#
# Environment / flags:
#   GITHUB_TOKEN         required for private repos; optional otherwise
#   STORHUB_INSTALL_DIR  override install destination (default /usr/local/bin)
#   --version vX.Y.Z     pin a specific release instead of nightly/stable

set -euo pipefail

REPO="FarelRA/storhub"
DEST="${STORHUB_INSTALL_DIR:-/usr/local/bin}"
PINNED=""

while [ $# -gt 0 ]; do
	case "$1" in
	--version)
		PINNED="${2:?--version requires a value}"
		shift 2
		;;
	*)
		echo "install.sh: unknown argument: $1" >&2
		exit 2
		;;
	esac
done

need() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "install.sh: missing dependency: $1" >&2
		exit 1
	}
}

need curl
need tar
if command -v sha256sum >/dev/null 2>&1; then
	digest() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
	digest() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
	echo "install.sh: need sha256sum or shasum to verify checksums" >&2
	exit 1
fi

case "$(uname -s)" in
Linux) OS="linux" ;;
Darwin) OS="darwin" ;;
*) echo "install.sh: unsupported OS: $(uname -s) (supported: linux, darwin)" >&2; exit 1 ;;
esac

case "$(uname -m)" in
x86_64 | amd64) ARCH="amd64" ;;
aarch64 | arm64) ARCH="arm64" ;;
armv6l) ARCH="armv6" ;;
armv7l | armhf) ARCH="armv7" ;;
i386 | i686) ARCH="386" ;;
*)
	echo "install.sh: unsupported architecture: $(uname -m)" >&2
	exit 1
	;;
esac

API="https://api.github.com/repos/${REPO}"
AUTH=()
if [ -n "${GITHUB_TOKEN:-}" ]; then
	AUTH=(-H "Authorization: Bearer ${GITHUB_TOKEN}")
fi

if [ -n "$PINNED" ]; then
	RELEASE_URL="$API/releases/tags/${PINNED}"
else
	# /releases/latest excludes prereleases; the rolling "nightly" is one,
	# so prefer it explicitly, then fall back to stable.
	NIGHTLY_ID="$(curl -fsSL "${AUTH[@]}" "$API/releases/tags/nightly" |
		sed -n 's/.*"id"[[:space:]]*:[[:space:]]*\([0-9]*\).*/\1/p' | head -n1 || true)"
	if [ -n "$NIGHTLY_ID" ]; then
		RELEASE_JSON="$(curl -fsSL "${AUTH[@]}" "$API/releases/tags/nightly")"
	else
		RELEASE_JSON="$(curl -fsSL "${AUTH[@]}" "$API/releases/latest")"
	fi
fi
if [ -z "${RELEASE_JSON:-}" ]; then
	RELEASE_JSON="$(curl -fsSL "${AUTH[@]}" "$RELEASE_URL")"
fi

TAG="$(printf '%s' "$RELEASE_JSON" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
[ -n "$TAG" ] || {
	echo "install.sh: could not determine release tag" >&2
	exit 1
}

# Asset names embed the goreleaser version, not the tag - and private
# repos require authenticated API downloads anyway. Resolve both files
# by scanning this release's asset list instead of guessing URLs.
# GitHub emits each asset as: "id":N,"node_id":"…","name":"…",… so
# compacting the document makes every (id, name) pair adjacent.
ASSETS_JSON="$(printf '%s' "$RELEASE_JSON" | tr -d '\n ')"

asset_id_for() {
	needle="$1"
	printf '%s' "$ASSETS_JSON" | grep -oE \
		"\"id\": ?[0-9]+,\"node_id\": ?\"[^\"]*\",\"name\": ?\"${needle}\"" |
		sed -n 's/^"id": *\([0-9]*\).*/\1/p' | head -n1
}

ASSET_ID="$(asset_id_for "storhub_[^\"]*_${OS}_${ARCH}\.tar\.gz")"
CHECKSUM_ID="$(asset_id_for "checksums\.txt")"
[ -n "$ASSET_ID" ] || {
	echo "install.sh: release ${TAG} has no storhub_*_${OS}_${ARCH}.tar.gz asset" >&2
	exit 1
}
[ -n "$CHECKSUM_ID" ] || {
	echo "install.sh: release ${TAG} has no checksums.txt asset" >&2
	exit 1
}

TMPDIR_DL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_DL"' EXIT

echo "==> downloading storhub for ${OS}/${ARCH} (${TAG})"
fetch_asset() {
	curl -fsSL --retry 3 "${AUTH[@]}" -H "Accept: application/octet-stream" \
		-o "$2" "$API/releases/assets/$1"
}
fetch_asset "$ASSET_ID" "${TMPDIR_DL}/storhub.tar.gz"
fetch_asset "$CHECKSUM_ID" "${TMPDIR_DL}/checksums.txt"

echo "==> verifying checksum"
EXPECTED="$(grep "_${OS}_${ARCH}\.tar\.gz\$" "${TMPDIR_DL}/checksums.txt" | head -n1 | cut -d' ' -f1)"
[ -n "$EXPECTED" ] || {
	echo "install.sh: checksums.txt has no entry for ${OS}/${ARCH}" >&2
	exit 1
}
ACTUAL="$(digest "${TMPDIR_DL}/storhub.tar.gz")"
if [ "$ACTUAL" != "$EXPECTED" ]; then
	echo "install.sh: checksum mismatch (expected ${EXPECTED}, got ${ACTUAL})" >&2
	exit 1
fi

tar -xzf "${TMPDIR_DL}/storhub.tar.gz" -C "$TMPDIR_DL"

install_binary() {
	local target="$1"
	if [ -w "$target" ] || [ "$(id -u)" -eq 0 ]; then
		install -m 0755 "${TMPDIR_DL}/storhub" "${target}/storhub"
	else
		sudo install -m 0755 "${TMPDIR_DL}/storhub" "${target}/storhub"
	fi
}

if [ -d "$DEST" ] && { [ -w "$DEST" ] || command -v sudo >/dev/null 2>&1; }; then
	install_binary "$DEST"
elif mkdir -p "$DEST" 2>/dev/null; then
	install_binary "$DEST"
else
	DEST="${HOME}/.local/bin"
	mkdir -p "$DEST"
	install_binary "$DEST"
	echo "==> installed to ${DEST}; make sure it is on your PATH"
fi

echo "==> storhub ${TAG} installed at ${DEST}/storhub"
"${DEST}/storhub" --version
