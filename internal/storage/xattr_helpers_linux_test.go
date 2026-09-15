//go:build linux

package storage

import "syscall"

// testSetUserXattr / testGetUserXattr wrap the Linux-only xattr syscalls used
// by the mounted-FUSE smoke test. Kept build-tagged so the package still
// compiles under `GOOS=darwin go vet`; the non-linux twin
// reports unsupported and the test skips the assertion.
func testSetUserXattr(path, name string, value []byte) error {
	return syscall.Setxattr(path, name, value, 0)
}

func testGetUserXattr(path, name string) ([]byte, error) {
	buf := make([]byte, 16)
	n, err := syscall.Getxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
