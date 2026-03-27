// Package storhub provides immutable GitHub-backed file storage.
//
// StorHub stores file data in GitHub release assets and stores the active
// filesystem/catalog state in `.storhub/metadata.json` inside the target
// repository. Files are chunked, transferred as streams, and verified with
// CRC32C checksums. The package supports immutable patching, rollback through
// metadata history, and filesystem-style operations such as mkdir, rename,
// read-at, write-at, append, and truncate.
package storhub
