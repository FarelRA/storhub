//go:build linux

package fusefs

import "golang.org/x/sys/unix"

// pcRenameNoReplace performs an atomic no-replace rename on Linux via
// renameat2. x/sys is already a module dependency.
func pcRenameNoReplace(oldFull, newFull string) error {
	return unix.Renameat2(unix.AT_FDCWD, oldFull, unix.AT_FDCWD, newFull, unix.RENAME_NOREPLACE)
}
