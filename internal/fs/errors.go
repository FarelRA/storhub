package fs

import (
	"errors"
	"fmt"
)

// ErrNotFound and peers are the sentinel errors wrapped with the
// offending path by the constructors below.
var (
	ErrAlreadyExists  = errors.New("already exists")
	ErrNotEmpty       = errors.New("not empty")
	ErrIsDirectory    = errors.New("is a directory")
	ErrNotDirectory   = errors.New("not a directory")
	ErrNotFound       = errors.New("not found")
	ErrInvalidSymlink = errors.New("invalid symlink")
	ErrXAttrNotFound  = errors.New("xattr not found")
	ErrCorrupted      = errors.New("corrupted")
)

// AlreadyExists reports that a path already exists.
func AlreadyExists(path string) error { return fmt.Errorf("%w: %s", ErrAlreadyExists, path) }

// NotEmpty reports that a directory is not empty.
func NotEmpty(path string) error { return fmt.Errorf("%w: %s", ErrNotEmpty, path) }

// IsDirectory reports that a path is a directory.
func IsDirectory(path string) error { return fmt.Errorf("%w: %s", ErrIsDirectory, path) }

// NotDirectory reports that a path is not a directory.
func NotDirectory(path string) error { return fmt.Errorf("%w: %s", ErrNotDirectory, path) }

// NotFound reports that a path was not found.
func NotFound(path string) error { return fmt.Errorf("%w: %s", ErrNotFound, path) }

// InvalidSymlink reports that a symlink cannot be resolved.
func InvalidSymlink(path string) error { return fmt.Errorf("%w: %s", ErrInvalidSymlink, path) }

// XAttrNotFound reports that an extended attribute is absent.
func XAttrNotFound(path string) error { return fmt.Errorf("%w: %s", ErrXAttrNotFound, path) }

// Corrupted reports that stored data failed integrity checks.
func Corrupted(path string) error { return fmt.Errorf("%w: %s", ErrCorrupted, path) }
