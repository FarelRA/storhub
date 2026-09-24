package fs

import (
	"time"

	metadata "github.com/FarelRA/storhub/internal/metadata"
)

// EntryInfo is the stat-style view of one path used by StatPath.
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

// KindLabel returns the single display vocabulary for an entry:
// "directory", "symlink", or "file". The IsDir/IsSymlink flags are the
// source of truth (the Kind wire field only spans file/symlink); a set dir
// bit wins, matching the historical renderer order. Every renderer must use
// this instead of re-spelling the triple.
func KindLabel(isDir, isSymlink bool) string {
	if isDir {
		return "directory"
	}
	if isSymlink {
		return "symlink"
	}
	return "file"
}

// KindLabel reports this entry's display vocabulary.
func (e EntryInfo) KindLabel() string {
	return KindLabel(e.IsDir, e.IsSymlink)
}

// KindLabel mirrors EntryInfo.KindLabel for directory listings.
func (e DirEntry) KindLabel() string {
	return KindLabel(e.IsDir, e.IsSymlink)
}

// IsDirectory reports the dir flag behind one name.
func (e EntryInfo) IsDirectory() bool { return e.IsDir }

// IsLink reports the symlink flag behind one name.
func (e EntryInfo) IsLink() bool { return e.IsSymlink }

// IsDirectory reports the dir flag behind one name.
func (e DirEntry) IsDirectory() bool { return e.IsDir }

// IsLink reports the symlink flag behind one name.
func (e DirEntry) IsLink() bool { return e.IsSymlink }

// MetadataPatch carries one metadata-only update (mode, owner, times).
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

// XAttrMode qualifies a SetXAttr request. XAttrCreate fails with EEXIST
// when the name is already present; XAttrReplace fails with ENODATA when
// it is absent. Both are decided inside the update transaction so the
// existence test and the write cannot race.
type XAttrMode uint32

const (
	// XAttrCreate fails SetXAttr with EEXIST when the name is present.
	XAttrCreate XAttrMode = 1 << 0
	// XAttrReplace fails SetXAttr with ENODATA when the name is absent.
	XAttrReplace XAttrMode = 1 << 1
)

// POSIX/Linux resource limits enforced at the fs/posix boundary. FUSE's
// VFS applies them before dispatching, but REST-originated calls bypass
// the kernel entirely, so the server must not.
const (
	// XAttrNameMax mirrors XATTR_NAME_MAX (255 bytes, NUL excluded).
	XAttrNameMax = 255
	// XAttrSizeMax mirrors XATTR_SIZE_MAX (64 KiB).
	XAttrSizeMax = 65536
	// PathMax mirrors PATH_MAX for symlink targets.
	PathMax = 4096
)

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

// DirEntry is one child row of a directory listing.
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
	UID       uint32            `json:"uid,omitempty"`
	GID       uint32            `json:"gid,omitempty"`
	// Timestamps complete the attribute view so a listing can answer
	// stat-style queries (FUSE READDIRPLUS) without a per-child re-stat.
	CreatedAt  int64 `json:"created_at,omitempty"`
	ModifiedAt int64 `json:"modified_at,omitempty"`
	AccessedAt int64 `json:"accessed_at,omitempty"`
	ChangedAt  int64 `json:"changed_at,omitempty"`
}

// FSStats keeps its short name: it is referenced as shfs.FSStats and
// storhub.FSStats across storage, REST, CLI, and FUSE facades, so the
// rename churn outweighs the stutter.
//
//revive:disable-next-line:exported
type FSStats struct {
	Files       int   `json:"files"`
	Directories int   `json:"directories"`
	Inodes      int   `json:"inodes"`
	Bytes       int64 `json:"bytes"`
	Releases    int   `json:"releases"`
	// Assets counts chunk records carrying a release tag (one per chunk
	// row), not distinct asset IDs: chunks sharing one asset ID count
	// once per row. The authoritative per-asset count is the release
	// catalog's AssetCount, which StatFS callers needing asset precision
	// should prefer.
	Assets int `json:"assets"`
}
