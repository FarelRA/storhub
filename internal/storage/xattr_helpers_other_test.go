//go:build !linux

package storage

import "errors"

// errXattrUnsupported lets the mounted-FUSE smoke test skip its xattr
// assertion on platforms without the Linux user.* xattr syscalls, while still
// compiling under `GOOS=darwin go vet`.
var errXattrUnsupported = errors.New("user xattr syscalls are linux-only")

func testSetUserXattr(path, name string, value []byte) error {
	return errXattrUnsupported
}

func testGetUserXattr(path, name string) ([]byte, error) {
	return nil, errXattrUnsupported
}
