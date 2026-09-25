package fs

import (
	"errors"
	"fmt"
	"syscall"
)

// ErrNotFound and peers are the sentinel errors wrapped with the
// offending path by the constructors below. They are the single form for
// failures raised inside this package: every verb returns a typed sentinel
// (never a bare string), and the FUSE/REST edges translate sentinels to
// errno/status. Sentinels that replace a POSIX errno dual-wrap it with a
// second %w so errors.Is matches both faces until the edges match the
// sentinel directly.
var (
	ErrAlreadyExists  = errors.New("already exists")
	ErrNotEmpty       = errors.New("not empty")
	ErrIsDirectory    = errors.New("is a directory")
	ErrNotDirectory   = errors.New("not a directory")
	ErrNotFound       = errors.New("not found")
	ErrInvalidSymlink = errors.New("invalid symlink")
	ErrXAttrNotFound  = errors.New("xattr not found")
	ErrCorrupted      = errors.New("corrupted")
	// ErrBusy reports a node the kernel holds busy (rmdir of the root).
	ErrBusy = errors.New("busy")
	// ErrTooLarge reports growth past maxZeroExtendBytes.
	ErrTooLarge = errors.New("too large")
	// ErrInvalidArgument reports a malformed numeric argument such as a
	// negative offset, size, or span. Edges read it as EINVAL (400).
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrEscapesRoot reports a path that pops past the project root.
	ErrEscapesRoot = errors.New("path escapes root")
	// ErrRequiredPath reports a missing path where one is mandatory.
	ErrRequiredPath = errors.New("path is required")
)

// AlreadyExists reports that a path already exists.
func AlreadyExists(path string) error { return fmt.Errorf("%w: %s", ErrAlreadyExists, path) }

// NotEmpty reports that a directory is not empty.
func NotEmpty(path string) error { return fmt.Errorf("%w: %s", ErrNotEmpty, path) }

// IsDirectory reports that a path is a directory.
func IsDirectory(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrIsDirectory, path, syscall.EISDIR)
}

// NotDirectory reports that a path is not a directory.
func NotDirectory(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrNotDirectory, path, syscall.ENOTDIR)
}

// NotFound reports that a path was not found.
func NotFound(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrNotFound, path, syscall.ENOENT)
}

// InvalidSymlink reports that a symlink cannot be resolved.
func InvalidSymlink(path string) error { return fmt.Errorf("%w: %s", ErrInvalidSymlink, path) }

// XAttrNotFound reports that an extended attribute is absent.
func XAttrNotFound(path string) error { return fmt.Errorf("%w: %s", ErrXAttrNotFound, path) }

// Corrupted reports that stored data failed integrity checks.
func Corrupted(path string) error { return fmt.Errorf("%w: %s", ErrCorrupted, path) }

// Busy reports that a node is busy (rmdir of the project root).
func Busy(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrBusy, path, syscall.EBUSY)
}

// TooLarge reports growth past the zero-extend cap.
func TooLarge(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrTooLarge, path, syscall.EFBIG)
}

// InvalidArgument reports a malformed numeric argument. The detail keeps
// the historical message so string-matching callers keep working.
func InvalidArgument(detail string) error {
	return fmt.Errorf("%w: %s: %w", ErrInvalidArgument, detail, syscall.EINVAL)
}

// EscapesRoot reports a path that pops past the project root.
func EscapesRoot(path string) error {
	return fmt.Errorf("%w: %s: %w", ErrEscapesRoot, path, syscall.EINVAL)
}

// RequiredPath reports a missing path where one is mandatory. The detail
// keeps the historical per-verb message.
func RequiredPath(detail string) error {
	return fmt.Errorf("%w: %s: %w", ErrRequiredPath, detail, syscall.EINVAL)
}
