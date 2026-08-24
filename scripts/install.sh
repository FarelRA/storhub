#!/usr/bin/env bash
# StorHub installer: fetches the latest release for this platform,
# verifies the SHA256 checksum, and installs the binary.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/FarelRA/storhub/main/scripts/install.sh | bash
#
# Environment / flags:
#   STORHUB_INSTALL_DIR  override install destination (default /usr/local/bin)
#   --version vX.Y.Z     pin a specific release instead of latest

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
	SUMTOOL=(sha256sum -c)
elif command -v shasum >/dev/null 2>&1; then
	SUMTOOL=(shasum -a 256 -c)
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

API="https://api.github.com/repos/${REPO}/releases"
if [ -n "$PINNED" ]; then
	URL="$API/tags/${PINNED}"
else
	URL="$API/latest"
fi

echo "==> resolving release from ${URL}"
RELEASE_JSON="$(curl -fsSL "$URL")"
TAG="$(printf '%s' "$RELEASE_JSON" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
[ -n "$TAG" ] || {
	echo "install.sh: could not determine release tag" >&2
	exit 1
}

ASSET="storhub_${TAG}_${OS}_${ARCH}.tar.gz"
BASE="https://github.com/${REPO}/releases/download/${TAG}"
TMPDIR_DL="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_DL"' EXIT

echo "==> downloading ${ASSET} (${TAG})"
curl -fSL --retry 3 -o "${TMPDIR_DL}/${ASSET}" "${BASE}/${ASSET}"
curl -fSL --retry 3 -o "${TMPDIR_DL}/checksums.txt" "${BASE}/checksums.txt"

echo "==> verifying checksum"
(cd "$TMPDIR_DL" && grep " ${ASSET}\$" checksums.txt | "${SUMTOOL[@]}")

tar -xzf "${TMPDIR_DL}/${ASSET}" -C "$TMPDIR_DL"

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
