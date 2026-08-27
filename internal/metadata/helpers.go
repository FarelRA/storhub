package metadata

import (
	"path"
	"strings"
)

// normalizeStoredPath canonicalizes user-supplied paths to storage keys:
// relative, slash-clean, and without a leading separator. Surrounding
// whitespace is significant - leading/trailing spaces are legal filename
// characters on Unix and are preserved verbatim. It is idempotent and
// total: escaping paths ("..") are returned unchanged for now; Validate
// rejects such keys at load/commit boundaries. Semantics intentionally
// mirror fs.NormalizePath; TestPathNormalizerConformance pins the two
// implementations together so they cannot drift again.
func normalizeStoredPath(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	trimmed := strings.TrimLeft(value, "/")
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
