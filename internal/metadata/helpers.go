package metadata

import (
	"fmt"
	"path"
	"strings"
)

// normalizeStoredPathErr is the checked constructor: it canonicalizes
// user-supplied paths to storage keys exactly like normalizeStoredPath, but
// escaping paths fail with the offending value in context instead of passing
// through silently. Prefer it wherever a caller can act on the error;
// normalizeStoredPath stays total for the load/commit paths where Validate
// rejects escaping keys at the boundary.
func normalizeStoredPathErr(value string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", nil
	}
	trimmed := strings.TrimLeft(value, "/")
	cleaned := path.Clean(trimmed)
	if cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("metadata path escapes root: %q", value)
	}
	return cleaned, nil
}

// normalizeStoredPath canonicalizes user-supplied paths to storage keys:
// relative, slash-clean, and without a leading separator. Surrounding
// whitespace inside a non-blank value is significant - leading/trailing
// spaces are legal filename characters on Unix and are preserved verbatim -
// but a value that is entirely whitespace normalizes to "" (the empty key,
// which Validate then rejects), mirroring fs.NormalizePath's blank check.
// It is idempotent and total: escaping paths ("..") are returned unchanged
// for now; Validate rejects such keys at load/commit boundaries. Semantics
// intentionally mirror fs.NormalizePath; TestPathNormalizerConformance pins
// the two implementations together so they cannot drift again.
//
// Deprecated: load-path-only total normalizer. Prefer normalizeStoredPathErr
// (the checked primary) wherever a caller can act on the error; escaping
// paths pass through here silently and are rejected by Validate at the
// boundary instead. Cross-package callers cannot use either (both are
// unexported); in-package call sites migrate as their signatures allow
// (validateStoredPathKey already uses the Err form; the void entry-point
// family keeps this form until its signatures can carry errors).
func normalizeStoredPath(value string) string {
	cleaned, err := normalizeStoredPathErr(value)
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
