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
	ModifiedAt    int64             `json:"modified_at"`
	CreatedAt     int64             `json:"created_at"`
	AccessedAt    int64             `json:"accessed_at,omitempty"`
	ChangedAt     int64             `json:"changed_at,omitempty"`
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

// RangeEdit replaces the byte span [Start, Start+DeleteSize) with Data in
// one step. A batch of RangeEdits is applied to a file as a single
// operation: one release resolution, one asset per chunk of edited bytes,
// one playlist rebuild, one metadata mutation. Edits must be sorted by
// Start and never overlap; DeleteSize may be zero (pure insert) and Data
// may be empty (pure delete).
type RangeEdit struct {
	Start      int64
	DeleteSize int64
	Data       []byte
}

// End returns the exclusive end offset this edit replaces.
func (e RangeEdit) End() int64 { return e.Start + e.DeleteSize }

// Len reports how many bytes the edit inserts.
func (e RangeEdit) Len() int64 { return int64(len(e.Data)) }

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
