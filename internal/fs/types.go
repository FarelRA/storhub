package fs

import (
	"time"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

type EntryInfo struct {
	Path          string            `json:"path"`
	Kind          metadata.NodeKind `json:"kind,omitempty"`
	IsDir         bool              `json:"is_dir"`
	IsSymlink     bool              `json:"is_symlink,omitempty"`
	Size          int64             `json:"size"`
	Inode         uint64            `json:"inode,omitempty"`
	Mode          uint32            `json:"mode,omitempty"`
	UID           uint32            `json:"uid,omitempty"`
	GID           uint32            `json:"gid,omitempty"`
	NLink         uint32            `json:"nlink,omitempty"`
	ModifiedAt    time.Time         `json:"modified_at"`
	CreatedAt     time.Time         `json:"created_at"`
	AccessedAt    time.Time         `json:"accessed_at,omitempty"`
	ChangedAt     time.Time         `json:"changed_at,omitempty"`
	SymlinkTarget string            `json:"symlink_target,omitempty"`
}

type MetadataPatch struct {
	HasMode  bool
	Mode     uint32
	HasOwner bool
	UID      uint32
	GID      uint32
	HasTimes bool
	ATime    time.Time
	MTime    time.Time
}

type DirEntry struct {
	Name      string            `json:"name"`
	Path      string            `json:"path"`
	Kind      metadata.NodeKind `json:"kind,omitempty"`
	IsDir     bool              `json:"is_dir"`
	IsSymlink bool              `json:"is_symlink,omitempty"`
	Size      int64             `json:"size"`
	Inode     uint64            `json:"inode,omitempty"`
	Mode      uint32            `json:"mode,omitempty"`
	NLink     uint32            `json:"nlink,omitempty"`
}

type FSStats struct {
	Files       int   `json:"files"`
	Directories int   `json:"directories"`
	Inodes      int   `json:"inodes"`
	Bytes       int64 `json:"bytes"`
	Releases    int   `json:"releases"`
	Assets      int   `json:"assets"`
}
