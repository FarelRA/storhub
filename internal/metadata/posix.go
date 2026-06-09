package metadata

import (
	"os"
	"time"
)

type NodeKind string

const (
	NodeKindFile    NodeKind = "file"
	NodeKindSymlink NodeKind = "symlink"
)

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func defaultFileMode(kind NodeKind) uint32 {
	switch kind {
	case NodeKindSymlink:
		return 0o777
	default:
		return 0o644
	}
}

func defaultDirMode() uint32 {
	return 0o755
}

func defaultOwnerIDs() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}

func normalizeXAttrs(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	clone := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if k == "" {
			continue
		}
		clone[k] = v
	}
	if len(clone) == 0 {
		return nil
	}
	return clone
}

func chooseNonZeroTime(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func timeToUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
