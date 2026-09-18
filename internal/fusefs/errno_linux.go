//go:build linux

package fusefs

import "syscall"

// errCorruptedErrno maps shfs.ErrCorrupted to EUCLEAN ("Structure needs
// cleaning"), the closest POSIX signal that on-disk metadata is inconsistent.
// Linux-only: the errno does not exist on darwin/bsd syscall tables.
const errCorruptedErrno = syscall.EUCLEAN

// syncWriteFlags selects handles whose writes must be durable before the
// Write returns. O_DSYNC exists on linux; other platforms spell it
// differently or not at all (see errno_other.go).
const syncWriteFlags = uint32(syscall.O_SYNC | syscall.O_DSYNC)

// dsyncOnlyFlag is the O_DSYNC bit alone (without the O_SYNC bit), for
// tests proving an O_DSYNC-only open takes the synchronous path.
const dsyncOnlyFlag = uint32(syscall.O_DSYNC)
