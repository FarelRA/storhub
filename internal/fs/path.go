package fs

import (
	"fmt"
	"path"
	"strings"
)

func NormalizePath(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", fmt.Errorf("path is required")
	}
	trimmed = strings.TrimLeft(trimmed, "/")
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
	cleaned, err := NormalizePath(value)
	if err != nil {
		return strings.Trim(strings.TrimSpace(value), "/")
	}
	return cleaned
}

func ParentPath(value string) string {
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

func IsParentOrSame(parent, child string) bool {
	parent = normalizeStoredPath(parent)
	child = normalizeStoredPath(child)
	if parent == "" {
		return true
	}
	return child == parent || strings.HasPrefix(child, parent+"/")
}

func RemapPath(oldBase, newBase, target string) string {
	if target == oldBase {
		return newBase
	}
	return newBase + strings.TrimPrefix(target, oldBase)
}
