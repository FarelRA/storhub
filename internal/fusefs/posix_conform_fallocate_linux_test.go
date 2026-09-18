//go:build linux

package fusefs

import "golang.org/x/sys/unix"

// pcFallocatePunchHole issues a KEEP_SIZE hole punch on an open file
// descriptor. Linux-only syscall; the portable fallback reports ENOSYS so
// darwin vet still compiles and the scenario fails loudly off-Linux.
func pcFallocatePunchHole(fd int64, off, length int64) error {
	return unix.Fallocate(int(fd), unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, off, length)
}
