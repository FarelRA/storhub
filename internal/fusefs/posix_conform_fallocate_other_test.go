//go:build !linux

package fusefs

import "syscall"

// pcFallocatePunchHole has no implementation off Linux: report ENOSYS so
// the punch-hole scenario fails loudly there instead of silently
// degrading.
func pcFallocatePunchHole(fd int64, off, length int64) error {
	return syscall.ENOSYS
}
