package metadata

import (
	"path"
	"strings"
)

// normalizeStoredPath canonicalizes user-supplied paths to storage keys:
// relative, slash-clean, and without a leading separator. It is idempotent.
// Escaping paths ("..") are returned unchanged for now; Validate rejects
// such keys at load/commit boundaries.
func normalizeStoredPath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.TrimLeft(trimmed, "/")
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return ""
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return strings.Trim(strings.TrimSpace(value), "/")
	}
	return cleaned
}

func parentPath(value string) string {
	value = normalizeStoredPath(value)
	if value == "" {
		return ""
	}
	parent := path.Dir(value)
	if parent == "." {
		return ""
	}
	return parent
}
