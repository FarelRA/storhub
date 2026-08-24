//go:build linux

package fusefs

import (
	"os"
	"syscall"
)

// reserveSpace issues the platform fallocate on the overlay temp file so
// blocks are reserved before data flows, giving early ENOSPC instead of a
// mid-download failure.
func reserveSpace(f *os.File, mode uint32, off, size int64) error {
	return syscall.Fallocate(int(f.Fd()), mode, off, size)
}
