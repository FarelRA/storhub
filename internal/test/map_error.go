package test

// Central error translation for the three conformance adapters.
//
// Boundary: the CLI (fs + storage sentinels), REST (HTTP statuses), and
// FUSE (syscall errnos) adapters each reported backend failures in their
// own words, which made one added errno three edits. The shared
// backend-to-sentinel knowledge lives here; each adapter keeps a thin
// delegate that only adds its own context (the "(cli: ...)" and "what:"
// prefixes, the storage session lines). No error VALUE changes: every
// mapping below mirrors the adapters line for line, so errors.Is
// outcomes are identical before and after.
//
// Leaf-only: this file imports internal/fs (already an edge the package
// holds) but never internal/storage. Storage test binaries import this
// package, so a storage edge here would cycle the test binary. The two
// storage-scoped lines (session linked/stale) stay in the CLI delegate.

import (
	"errors"
	"net/http"
	"strings"
	"syscall"

	shfs "github.com/FarelRA/storhub/internal/fs"
)

// testSentinels is every test sentinel a backend failure can already
// carry. Errors matching one pass through untouched, never rewrapped.
var testSentinels = []error{
	ErrNotFound, ErrExists, ErrIsDir,
	ErrNotDir, ErrNotEmpty, ErrLoop,
	ErrUnsatisfiableRange, ErrClosed,
	ErrAccess, ErrInvalid, ErrStale,
}

// SentinelFor maps a backend failure onto a posixconform sentinel,
// returning nil when err carries no known backend failure. It covers
// the fs sentinels the fakes report and the syscall errnos the mount
// reports. Test sentinels never reach it (Translate filters them first),
// and storage-scoped sentinels never reach it either (the CLI delegate
// owns those two lines).
func SentinelFor(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, shfs.ErrNotFound):
		return ErrNotFound
	case errors.Is(err, shfs.ErrAlreadyExists):
		return ErrExists
	case errors.Is(err, shfs.ErrIsDirectory):
		return ErrIsDir
	case errors.Is(err, shfs.ErrNotDirectory):
		return ErrNotDir
	case errors.Is(err, shfs.ErrNotEmpty):
		return ErrNotEmpty
	case errors.Is(err, syscall.ENOENT):
		return ErrNotFound
	case errors.Is(err, syscall.EEXIST):
		return ErrExists
	case errors.Is(err, syscall.EISDIR):
		return ErrIsDir
	case errors.Is(err, syscall.ENOTDIR):
		return ErrNotDir
	case errors.Is(err, syscall.ENOTEMPTY):
		return ErrNotEmpty
	case errors.Is(err, syscall.ELOOP):
		return ErrLoop
	case errors.Is(err, syscall.EINVAL):
		return ErrInvalid
	case errors.Is(err, syscall.EBADF):
		return ErrClosed
	}
	return nil
}

// Translate maps err onto a test sentinel for errors.Is matching.
// Errors already carrying a test sentinel, and unrecognized errors,
// return untouched. Backend-sourced mappings pass through wrap for the
// adapter to annotate (nil wrap returns the bare sentinel); a nil err
// returns nil.
func Translate(err error, wrap func(mapped error) error) error {
	if err == nil {
		return nil
	}
	for _, sentinel := range testSentinels {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	if mapped := SentinelFor(err); mapped != nil {
		if wrap != nil {
			return wrap(mapped)
		}
		return mapped
	}
	return err
}

// SentinelForStatus maps a REST status onto a posixconform sentinel,
// using detail (the lowercased code plus message, which disambiguates
// 409) for the conflict branches. It returns nil for success statuses,
// for precondition failures (the caller builds those, since they carry
// an Actual token), and for unknown statuses (the caller reports those
// loudly with the code attached).
func SentinelForStatus(status int, detail string) error {
	lower := strings.ToLower(detail)
	switch status {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusGone:
		return ErrStale
	case http.StatusConflict:
		switch {
		case strings.Contains(lower, "not empty"):
			return ErrNotEmpty
		case strings.Contains(lower, "is a directory"):
			return ErrIsDir
		case strings.Contains(lower, "not a directory"):
			return ErrNotDir
		default:
			return ErrExists
		}
	case http.StatusBadRequest:
		return ErrInvalid
	case http.StatusRequestedRangeNotSatisfiable:
		return ErrUnsatisfiableRange
	}
	return nil
}
