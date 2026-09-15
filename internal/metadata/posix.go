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

func normalizeXAttrs(attrs XAttrMap) XAttrMap {
	if len(attrs) == 0 {
		return nil
	}
	clone := make(XAttrMap, len(attrs))
	for k, v := range attrs {
		if k == "" {
			continue
		}
		// Deep-copy the value too: sharing []byte backing arrays lets a
		// mutation of stored metadata reach the tree it was copied from.
		clone[k] = append([]byte(nil), v...)
	}
	if len(clone) == 0 {
		return nil
	}
	return clone
}

// nodeKindOf classifies a file entry for mode defaults: an entry carrying a
// symlink target is a link, everything else a regular file.
func nodeKindOf(f *FileMeta) NodeKind {
	if f != nil && f.Symlink != "" {
		return NodeKindSymlink
	}
	return NodeKindFile
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
