//go:build !linux

package fusefs

import "syscall"

// errCorruptedErrno falls back to EIO on platforms without EUCLEAN: the
// operation failed on corrupted metadata either way, and EIO is the
// portable "I/O error" superset callers already handle.
const errCorruptedErrno = syscall.EIO
