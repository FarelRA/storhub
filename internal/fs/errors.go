package fs

import (
	"errors"
	"fmt"
)

var (
	ErrAlreadyExists  = errors.New("already exists")
	ErrNotEmpty       = errors.New("not empty")
	ErrIsDirectory    = errors.New("is a directory")
	ErrNotDirectory   = errors.New("not a directory")
	ErrNotFound       = errors.New("not found")
	ErrInvalidSymlink = errors.New("invalid symlink")
	ErrXAttrNotFound  = errors.New("xattr not found")
)

func AlreadyExists(path string) error  { return fmt.Errorf("%w: %s", ErrAlreadyExists, path) }
func NotEmpty(path string) error       { return fmt.Errorf("%w: %s", ErrNotEmpty, path) }
func IsDirectory(path string) error    { return fmt.Errorf("%w: %s", ErrIsDirectory, path) }
func NotDirectory(path string) error   { return fmt.Errorf("%w: %s", ErrNotDirectory, path) }
func NotFound(path string) error       { return fmt.Errorf("%w: %s", ErrNotFound, path) }
func InvalidSymlink(path string) error { return fmt.Errorf("%w: %s", ErrInvalidSymlink, path) }
func XAttrNotFound(path string) error  { return fmt.Errorf("%w: %s", ErrXAttrNotFound, path) }
