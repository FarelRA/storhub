package metadata

import (
	"fmt"
	"hash/crc32"
	"path"
	"strings"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func normalizeStoredPath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "/") {
		trimmed = strings.TrimPrefix(trimmed, "/")
	}
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

func sumCRC32C(data []byte) string {
	return fmt.Sprintf("%08x", crc32.Checksum(data, crc32cTable))
}
