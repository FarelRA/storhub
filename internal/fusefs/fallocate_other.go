//go:build !linux

package fusefs

import (
	"os"
	"syscall"
)

// reserveSpace reports ENOSYS where the platform has no fallocate; FUSE
// callers (aria2c et al.) fall back to truncate-based allocation.
func reserveSpace(_ *os.File, _ uint32, _, _ int64) error {
	return syscall.ENOSYS
}
