package storhub

import (
	"os"
	"time"
)

type NodeKind string

const (
	NodeKindFile    NodeKind = "file"
	NodeKindSymlink NodeKind = "symlink"
)

type RootMetadata struct {
	Inode      uint64            `json:"inode"`
	Mode       uint32            `json:"mode"`
	UID        uint32            `json:"uid"`
	GID        uint32            `json:"gid"`
	NLink      uint32            `json:"nlink"`
	CreatedAt  time.Time         `json:"created_at"`
	ModifiedAt time.Time         `json:"modified_at"`
	AccessedAt time.Time         `json:"accessed_at"`
	ChangedAt  time.Time         `json:"changed_at"`
	XAttrs     map[string]string `json:"xattrs,omitempty"`
}

func (r RootMetadata) Clone() RootMetadata {
	clone := r
	clone.XAttrs = cloneStringMap(r.XAttrs)
	return clone
}

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

func chooseNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
}
