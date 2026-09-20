package fs

import (
	"fmt"
	"path"
	"strings"
)

// ValidateAccessPathShape rejects the input spellings no repository can
// ever address, without consulting one: empty and whitespace-only paths.
// It mirrors NormalizePath's blank checks exactly. Traversal spellings are
// deliberately NOT rejected here: access resolution is physical and
// repo-aware (see ResolveAccessPath), so only the resolver can decide
// whether a ".." pops past the root.
func ValidateAccessPathShape(value string) error {
	if value == "" {
		return fmt.Errorf("path is required")
	}
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("whitespace-only path is not addressable: %q", value)
	}
	return nil
}

// NormalizePath canonicalizes a user-supplied path: relative to the project
// root, slash-clean, and free of traversal. Surrounding whitespace is
// significant - leading/trailing spaces are legal filename characters on
// Unix and are preserved verbatim. A name consisting only of whitespace is
// rejected here, not silently mapped to the root: the metadata store still
// keys whitespace-only names at the root (see normalizeStoredPath in
// internal/metadata), so accepting one would create a root-masquerading
// phantom entry.
//
// NormalizePath is KEY CANONICALIZATION: it maps a concrete path (no "..",
// no symlink components) to its canonical storage key. User paths that may
// contain ".", "..", or symlink components must go through the repo-aware
// access resolver (ResolveAccessPath) instead.
func NormalizePath(value string) (string, error) {
	if err := ValidateAccessPathShape(value); err != nil {
		return "", err
	}
	trimmed := strings.TrimLeft(value, "/")
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("path escapes root: %s", value)
	}
	return cleaned, nil
}

// normalizeStoredPath canonicalizes an already-stored path for map lookups.
// On normalization failure it keeps the value's literal spelling (minus
// surrounding slashes) but never trims significant whitespace: a traversal
// spelling like "../x" can only ever miss the exact-key repo maps (ENOENT),
// while collapsing it to "" would masquerade as the root directory.
func normalizeStoredPath(value string) string {
	cleaned, err := NormalizePath(value)
	if err != nil {
		return strings.Trim(value, "/")
	}
	return cleaned
}

// ParentPath returns the parent directory key of a stored path.
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

// IsParentOrSame reports whether child equals parent or lives under it.
func IsParentOrSame(parent, child string) bool {
	parent = normalizeStoredPath(parent)
	child = normalizeStoredPath(child)
	if parent == "" {
		return true
	}
	return child == parent || strings.HasPrefix(child, parent+"/")
}

// RemapPath rewrites target from under oldBase to under newBase.
func RemapPath(oldBase, newBase, target string) string {
	if target == oldBase {
		return newBase
	}
	return newBase + strings.TrimPrefix(target, oldBase)
}
