//go:build !linux

package fusefs

import "syscall"

// errCorruptedErrno falls back to EIO on platforms without EUCLEAN: the
// operation failed on corrupted metadata either way, and EIO is the
// portable "I/O error" superset callers already handle.
const errCorruptedErrno = syscall.EIO

// syncWriteFlags selects handles whose writes must be durable before the
// Write returns. O_DSYNC has no portable spelling (absent on windows), so
// off-linux only O_SYNC forces the synchronous commit plus drain path;
// O_DSYNC-only opens there degrade to buffered durability.
const syncWriteFlags = uint32(syscall.O_SYNC)

// dsyncOnlyFlag is zero off-linux: no distinct O_DSYNC spelling exists,
// so the O_DSYNC-only test skips there.
const dsyncOnlyFlag = 0
