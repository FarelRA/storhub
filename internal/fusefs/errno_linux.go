//go:build linux

package fusefs

import "syscall"

// errCorruptedErrno maps shfs.ErrCorrupted to EUCLEAN ("Structure needs
// cleaning"), the closest POSIX signal that on-disk metadata is inconsistent.
// Linux-only: the errno does not exist on darwin/bsd syscall tables.
const errCorruptedErrno = syscall.EUCLEAN
