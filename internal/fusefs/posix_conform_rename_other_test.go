//go:build !linux

package fusefs

import "syscall"

// pcRenameNoReplace has no atomic implementation off Linux (darwin has no
// renameat2): report ENOSYS so the no-replace scenario fails loudly there
// instead of silently degrading to a racy check-then-rename.
func pcRenameNoReplace(oldFull, newFull string) error {
	return syscall.ENOSYS
}
