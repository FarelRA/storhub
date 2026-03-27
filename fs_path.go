package storhub

import (
	"fmt"
	"path"
	"strings"
)

func normalizeFSPath(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("path is required")
	}
	if strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("absolute paths are not supported: %s", value)
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes root: %s", value)
	}
	return cleaned, nil
}

func normalizeStoredPath(value string) string {
	cleaned, err := normalizeFSPath(value)
	if err != nil {
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

func isParentOrSame(parent, child string) bool {
	parent = normalizeStoredPath(parent)
	child = normalizeStoredPath(child)
	if parent == "" {
		return true
	}
	return child == parent || strings.HasPrefix(child, parent+"/")
}
