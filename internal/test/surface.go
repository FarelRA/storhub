package test

import (
	"errors"
	"time"
)

// CreateDisposition selects whether Open may create a missing file,
// like O_CREAT on open(2).
type CreateDisposition int

const (
	// CreateNever opens only existing files: a missing path in any mode
	// fails with ErrNotFound (POSIX ENOENT without O_CREAT).
	CreateNever CreateDisposition = iota + 1
	// CreateIfMissing creates a missing file like O_CREAT (never
	// exclusive: an existing file opens normally). Creation applies to
	// write modes only; OpenReadOnly and OpenPath never create. The
	// oracle creates with mode 0o644 subject to its umask; adapters
	// create through their usual write-mode paths.
	CreateIfMissing
)

// OpenMode selects the access mode of a Handle returned by Open.
type OpenMode int

const (
	// OpenReadOnly allows reads and denies writes.
	OpenReadOnly OpenMode = iota + 1
	// OpenWriteOnly allows writes and denies reads.
	OpenWriteOnly
	// OpenReadWrite allows both reads and writes.
	OpenReadWrite
	// OpenAppend is write-only with the cursor forced to the end on every cursor write.
	OpenAppend
	// OpenTruncate is write-only and empties an existing file on open.
	OpenTruncate
	// OpenPath opens a path without requiring read or write permission
	// and without creating anything, like O_PATH on Linux: the handle
	// cannot do I/O (reads and writes fail with ErrAccess). Only
	// surfaces that can express permission-free opens implement it;
	// scenarios using it are SurfaceFUSE-only (see Filter).
	OpenPath
)

// Permission bits honored by Chmod and Stat for the setuid/setgid scenarios.
const (
	// SetUIDBit is the set-user-ID bit (0o4000).
	SetUIDBit uint32 = 0o4000
	// SetGIDBit is the set-group-ID bit (0o2000).
	SetGIDBit uint32 = 0o2000
)

// Stat describes a regular file after symlink resolution.
type Stat struct {
	Size  int64
	Mode  uint32
	UID   uint32
	GID   uint32
	MTime int64 // Unix nanoseconds, set by Utimens or by the implementation clock.
}

// Handle is an open file description: reads see the handle's own prior
// writes, and each handle carries an independent cursor.
type Handle interface {
	// PRead reads at an offset like pread() without moving the cursor.
	PRead(offset int64, length int) ([]byte, error)
	// PWrite writes at an offset like pwrite() without moving the cursor and clears setuid/setgid.
	PWrite(offset int64, data []byte) (int, error)
	// Read reads from the cursor like read() and advances it, returning empty at EOF.
	Read(length int) ([]byte, error)
	// Write writes at the cursor like write(), at the end for append mode, and clears setuid/setgid.
	Write(data []byte) (int, error)
	// Truncate resizes the open file like ftruncate(), zero-filling growth.
	Truncate(size int64) error
	// Sync flushes the open file like fsync().
	Sync() error
	// Close releases the handle like close(); use after close fails with ErrClosed.
	Close() error
}

var (
	// ErrNotFound reports a missing path, like ENOENT.
	ErrNotFound = errors.New("posixconform: no such file or directory")
	// ErrExists reports a collision, like EEXIST.
	ErrExists = errors.New("posixconform: file exists")
	// ErrIsDir reports a directory where a file was required, like EISDIR.
	ErrIsDir = errors.New("posixconform: is a directory")
	// ErrNotDir reports a non-directory where a directory was required, like ENOTDIR.
	ErrNotDir = errors.New("posixconform: not a directory")
	// ErrNotEmpty reports a non-empty directory on Rmdir, like ENOTEMPTY.
	ErrNotEmpty = errors.New("posixconform: directory not empty")
	// ErrLoop reports a symlink loop, the ELOOP equivalent.
	ErrLoop = errors.New("posixconform: too many levels of symbolic links")
	// ErrUnsatisfiableRange reports an out-of-range read, the ERANGE/416 equivalent.
	ErrUnsatisfiableRange = errors.New("posixconform: range not satisfiable")
	// ErrClosed reports use of a closed handle, like EBADF.
	ErrClosed = errors.New("posixconform: handle is closed")
	// ErrStale reports a handle whose server-side description expired
	// or vanished: idle past its TTL, like a stateful-open lease
	// lapsing. Stateless fds never produce it; only session-backed
	// descriptions do.
	ErrStale = errors.New("posixconform: stale handle")
	// ErrAccess reports an operation denied by the handle open mode, like EBADF.
	ErrAccess = errors.New("posixconform: operation not permitted by open mode")
	// ErrInvalid reports a bad argument such as a negative offset, like EINVAL.
	ErrInvalid = errors.New("posixconform: invalid argument")
	// ErrUnsupported reports an operation the surface honestly cannot
	// perform, like EOPNOTSUPP. Scenarios accept it only where the
	// scenario documents both the unsupported and the emulated outcome.
	ErrUnsupported = errors.New("posixconform: operation not supported")
)

// SeekHandle is an optional Handle capability for SEEK_DATA and SEEK_HOLE.
// It stays optional (rather than growing Handle) so existing adapters keep
// compiling untouched: dense-byte surfaces report the hole at the file size
// and clamp data offsets into range, while surfaces that cannot express
// seeks simply do not implement it and FUSE-only scenarios never run there
// (see Filter). Offsets at or past the end fail SEEK_DATA with
// ErrUnsatisfiableRange (ENXIO); offsets past the end fail SEEK_HOLE the
// same way; negative offsets fail with ErrInvalid.
type SeekHandle interface {
	// SeekData returns the next data offset at or after off, like
	// lseek(SEEK_DATA).
	SeekData(off int64) (int64, error)
	// SeekHole returns the next hole offset at or after off, like
	// lseek(SEEK_HOLE).
	SeekHole(off int64) (int64, error)
}

// PunchHoler is an optional Surface capability for deallocating a byte
// range. It stays optional (rather than growing Surface) so existing
// adapters keep compiling untouched. Dense-byte surfaces may emulate a
// punch by zero-filling, which reads back identically on dense bytes;
// surfaces whose backend cannot express it fail with ErrUnsupported
// instead of faking success.
//
// Why dense backends cannot punch real holes: files here are
// content-addressed chunk runs, not block maps, so there are no sparse
// blocks to deallocate. A real hole (size preserved, storage released,
// reads observe zeros) has no representation in a chunk list that only
// models present bytes; expressing one would require rewriting chunk
// coverage the CAS store does not model. Zero-filling is therefore the
// honest emulation (observably identical on reads), and ErrUnsupported
// is the honest refusal.
type PunchHoler interface {
	// PunchHole deallocates [off, off+length) like
	// fallocate(PUNCH_HOLE|KEEP_SIZE), or zero-fills it on dense-byte
	// surfaces, or fails with ErrUnsupported.
	PunchHole(path string, off, length int64) error
}

// ScratchSession is an open file description bound to no path, like
// O_TMPFILE: reads, writes, truncate, and sync all work on staged
// bytes, Link stages a name (the name appears only at Close), and Close
// without a link discards. Link APPENDS to a pending name set: linking
// an already-pending name fails with ErrExists, and linking an occupied
// path fails with ErrExists the same way. Close publishes the staged
// image to ALL pending names, each with create semantics, after
// pre-validating every name first: any taken name fails the whole close
// with ErrExists, publishing nothing, and the handle stays open for
// Relink. Relink REPLACES the whole pending set with one path, the
// rescue for a close wedged on a taken name.
type ScratchSession interface {
	Handle
	// Link appends path to the pending name set like linkat on an
	// O_TMPFILE fd.
	Link(path string) error
	// Relink replaces the pending name set with path like a second
	// linkat after the first name was taken.
	Relink(path string) error
}

// SessionSurface is an optional Surface capability for opening pathless
// scratch descriptions with an idle TTL. It stays optional (rather than
// growing Surface) so existing adapters keep compiling untouched:
// kernel-fd surfaces cannot name a description after open without
// O_TMPFILE support, so they simply do not implement it and scratch
// scenarios never run there (see Filter). A non-positive ttl takes the
// surface default; idle past the TTL, every operation on the session
// fails with ErrStale, like a lapsed lease.
type SessionSurface interface {
	// OpenScratch opens a pathless read-write description.
	OpenScratch(ttl time.Duration) (ScratchSession, error)
}

// UmaskSurface is an optional Surface capability for configuring the
// creation mask applied to CreateFile (and to files created by Open
// with CreateIfMissing), like umask(2). It stays optional (rather than
// growing Surface) so surfaces that cannot express masks simply do not
// implement it and the umask scenario never runs there (see Filter).
// A zero mask disables masking. Chmod sets exact modes and is never
// masked.
type UmaskSurface interface {
	// SetUmask replaces the creation mask (only the low 9 bits apply).
	SetUmask(mask uint32)
}

// ErrPrecondition reports a stale CAS token from CompareAndWrite; match it with errors.As.
type ErrPrecondition struct {
	Expected uint64
	Actual   uint64
}

// Error implements the error interface.
func (e ErrPrecondition) Error() string {
	return "posixconform: precondition failed: version mismatch"
}

// Surface is the semantic file-operation contract every adapter (FUSE mount,
// REST API, CLI) must implement. Paths are absolute POSIX paths. Methods that
// resolve symlinks report ErrLoop on a loop; Unlink, Rename, Readlink, Mkdir,
// and Rmdir act on the link or directory name itself.
type Surface interface {
	// CreateFile creates a file like O_CREAT; exclusive adds O_EXCL so a duplicate fails with ErrExists.
	// The requested mode is masked by the surface umask (see
	// UmaskSurface; zero when unconfigured).
	CreateFile(path string, perm uint32, exclusive bool) error
	// Open returns a handle with an independent cursor. Creation intent
	// travels in disp, like O_CREAT on open(2): CreateNever opens only
	// existing files (a missing path in any mode fails with ErrNotFound,
	// POSIX ENOENT without O_CREAT), while CreateIfMissing creates a
	// missing file in write modes before opening (never exclusive: an
	// existing file opens normally; OpenReadOnly and OpenPath never
	// create). The FUSE, REST and CLI adapters honor disp through their
	// write-mode create paths, so the oracle and the adapters now spell
	// creation the same way.
	Open(path string, mode OpenMode, disp CreateDisposition) (Handle, error)
	// Stat reports size, mode, ownership, and mtime of the resolved path like stat().
	Stat(path string) (Stat, error)
	// Truncate resizes by path like truncate(), zero-filling any extension.
	Truncate(path string, size int64) error
	// Chmod replaces permission bits including setuid/setgid like chmod().
	Chmod(path string, mode uint32) error
	// Chown replaces owner and group and clears setuid/setgid like chown by a non-privileged user.
	Chown(path string, uid, gid uint32) error
	// Utimens sets mtime explicitly like utimensat().
	Utimens(path string, mtime int64) error
	// Unlink removes the name like unlink(); open handles keep working while the name is gone.
	Unlink(path string) error
	// Rename moves a name like rename(); noReplace adds RENAME_NOREPLACE so clobbering fails with ErrExists.
	Rename(oldPath, newPath string, noReplace bool) error
	// Mkdir creates a directory like mkdir(), failing with ErrExists when present.
	// The perm lands masked to 0o7777. Backends that carry no directory
	// mode record nothing for it; portable checks assert presence, not bits.
	Mkdir(path string, perm uint32) error
	// Rmdir removes an empty directory like rmdir(), failing with ErrNotEmpty when occupied.
	Rmdir(path string) error
	// Symlink creates a symlink like symlink(); dangling targets are allowed.
	Symlink(target, linkPath string) error
	// Readlink returns the link target like readlink() without resolving it.
	Readlink(linkPath string) (string, error)
	// ReadRange returns an offset slice like a ranged GET; offsets at or past EOF fail with ErrUnsatisfiableRange.
	ReadRange(path string, offset, length int64) ([]byte, error)
	// Append atomically adds bytes at the end like O_APPEND writes on an existing file.
	Append(path string, data []byte) error
	// Sync flushes durability like fsync() and reports success with nil.
	Sync(path string) error
	// CompareAndWrite writes only when token matches the current revision like a CAS store, else ErrPrecondition.
	CompareAndWrite(path string, offset int64, data []byte, token uint64) error
	// Revision returns the current CAS token, advanced by every data mutation.
	Revision(path string) (uint64, error)
}
