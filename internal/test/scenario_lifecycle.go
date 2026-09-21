package test

import (
	"errors"
	"fmt"
)

var scenarioLifecycle = []Scenario{
	{
		Name:     "unlink-while-open",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-unlink-while-open"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("keep")); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			if err := s.Unlink(p); err != nil {
				return fmt.Errorf("unlink: %v", err)
			}
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat after unlink: want ErrNotFound, got %v", err)
			}
			got, err := h.PRead(0, 4)
			if err != nil {
				return fmt.Errorf("pread on unlinked handle: %v", err)
			}
			if string(got) != "keep" {
				return fmt.Errorf("unlinked handle read: want %q, got %q", "keep", got)
			}
			if _, err := h.Write([]byte("more")); err != nil {
				return fmt.Errorf("write on unlinked handle: %v", err)
			}
			got, err = h.PRead(0, 8)
			if err != nil {
				return fmt.Errorf("second pread: %v", err)
			}
			if string(got) != "keepmore" {
				return fmt.Errorf("grown content: want %q, got %q", "keepmore", got)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "rename-while-open",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-rename-open-src"
			q := "/pc-rename-open-dst"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("v1")); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			if err := s.Rename(p, q, false); err != nil {
				return fmt.Errorf("rename: %v", err)
			}
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat old name: want ErrNotFound, got %v", err)
			}
			st, err := s.Stat(q)
			if err != nil {
				return fmt.Errorf("stat new name: %v", err)
			}
			if st.Size != 2 {
				return fmt.Errorf("new name size: want 2, got %d", st.Size)
			}
			got, err := h.PRead(0, 2)
			if err != nil {
				return fmt.Errorf("pread on renamed handle: %v", err)
			}
			if string(got) != "v1" {
				return fmt.Errorf("renamed handle read: want %q, got %q", "v1", got)
			}
			if _, err := h.Write([]byte("v2")); err != nil {
				return fmt.Errorf("write after rename: %v", err)
			}
			got, err = s.ReadRange(q, 0, 4)
			if err != nil {
				return fmt.Errorf("ranged read new name: %v", err)
			}
			if string(got) != "v1v2" {
				return fmt.Errorf("new name content: want %q, got %q", "v1v2", got)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "close-after-unlink-discards",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-close-unlinked"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("keep")); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			if err := s.Unlink(p); err != nil {
				return fmt.Errorf("unlink: %v", err)
			}
			if _, err := h.Write([]byte("more")); err != nil {
				return fmt.Errorf("write on unlinked handle: %v", err)
			}
			// POSIX close on an unlinked description drops the data:
			// the name must stay gone after close.
			if err := h.Close(); err != nil {
				return fmt.Errorf("close on unlinked handle: %v", err)
			}
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat after close: want ErrNotFound, got %v", err)
			}
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("recreate: %v", err)
			}
			if err := s.Truncate(p, 3); err != nil {
				return fmt.Errorf("size recreate: %v", err)
			}
			got, err := s.ReadRange(p, 0, 3)
			if err != nil {
				return fmt.Errorf("read recreated: %v", err)
			}
			if string(got) != "\x00\x00\x00" {
				return fmt.Errorf("close must not publish staged bytes, got %q", got)
			}
			return nil
		},
	},
	{
		Name:     "close-after-rename-publishes-to-survivor",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-close-renamed-src"
			q := "/pc-close-renamed-dst"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("v1")); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			if err := s.Rename(p, q, false); err != nil {
				return fmt.Errorf("rename: %v", err)
			}
			if _, err := h.Write([]byte("v2")); err != nil {
				return fmt.Errorf("write after rename: %v", err)
			}
			// Close follows the surviving name: the staged tail lands
			// on q, and the old name stays gone.
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat old name: want ErrNotFound, got %v", err)
			}
			got, err := s.ReadRange(q, 0, 4)
			if err != nil {
				return fmt.Errorf("read survivor: %v", err)
			}
			if string(got) != "v1v2" {
				return fmt.Errorf("survivor content: want %q, got %q", "v1v2", got)
			}
			return nil
		},
	},
	{
		Name:     "sync-after-unlink-retains",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-sync-unlinked"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("keep")); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			if err := s.Unlink(p); err != nil {
				return fmt.Errorf("unlink: %v", err)
			}
			// fsync on an unlinked description succeeds and the staged
			// bytes stay readable; nothing publishes.
			if err := h.Sync(); err != nil {
				return fmt.Errorf("sync on unlinked handle: %v", err)
			}
			got, err := h.PRead(0, 4)
			if err != nil {
				return fmt.Errorf("pread after sync: %v", err)
			}
			if string(got) != "keep" {
				return fmt.Errorf("retained content: want %q, got %q", "keep", got)
			}
			if err := h.Truncate(2); err != nil {
				return fmt.Errorf("truncate unlinked handle: %v", err)
			}
			got, err = h.PRead(0, 2)
			if err != nil {
				return fmt.Errorf("pread after truncate: %v", err)
			}
			if string(got) != "ke" {
				return fmt.Errorf("truncated content: want %q, got %q", "ke", got)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat after close: want ErrNotFound, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "unlink-recreate-write-isolation",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-unlink-recreate"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			hOld, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open old: %v", err)
			}
			if _, err := hOld.Write([]byte("orig")); err != nil {
				return fmt.Errorf("write orig: %v", err)
			}
			if err := s.Unlink(p); err != nil {
				return fmt.Errorf("unlink: %v", err)
			}
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("recreate: %v", err)
			}
			hNew, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open new: %v", err)
			}
			if _, err := hNew.Write([]byte("new!")); err != nil {
				return fmt.Errorf("write new: %v", err)
			}
			if err := hNew.Close(); err != nil {
				return fmt.Errorf("close new: %v", err)
			}
			// The old description still addresses the unlinked inode:
			// its reads see orig, and its writes must not touch the
			// new file at the recycled name.
			got, err := hOld.PRead(0, 4)
			if err != nil {
				return fmt.Errorf("pread old handle: %v", err)
			}
			if string(got) != "orig" {
				return fmt.Errorf("old handle read: want %q, got %q", "orig", got)
			}
			if _, err := hOld.PWrite(0, []byte("XXXX")); err != nil {
				return fmt.Errorf("pwrite old handle: %v", err)
			}
			got, err = hOld.PRead(0, 4)
			if err != nil {
				return fmt.Errorf("reread old handle: %v", err)
			}
			if string(got) != "XXXX" {
				return fmt.Errorf("old handle content: want %q, got %q", "XXXX", got)
			}
			got, err = s.ReadRange(p, 0, 4)
			if err != nil {
				return fmt.Errorf("read recycled name: %v", err)
			}
			if string(got) != "new!" {
				return fmt.Errorf("recycled name content: want %q, got %q", "new!", got)
			}
			// Closing the old description discards its staged state
			// without clobbering the new file.
			if err := hOld.Close(); err != nil {
				return fmt.Errorf("close old: %v", err)
			}
			got, err = s.ReadRange(p, 0, 4)
			if err != nil {
				return fmt.Errorf("read after old close: %v", err)
			}
			if string(got) != "new!" {
				return fmt.Errorf("post-close content: want %q, got %q", "new!", got)
			}
			return nil
		},
	},
	{
		Name:     "handle-lifecycle-after-close",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-handle-lifecycle"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("first close: %v", err)
			}
			if err := h.Close(); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("second close: want ErrClosed, got %v", err)
			}
			if _, err := h.Read(1); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("read after close: want ErrClosed, got %v", err)
			}
			if _, err := h.PRead(0, 1); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("pread after close: want ErrClosed, got %v", err)
			}
			if _, err := h.Write([]byte("x")); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("write after close: want ErrClosed, got %v", err)
			}
			if err := h.Sync(); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("sync after close: want ErrClosed, got %v", err)
			}
			if err := h.Truncate(0); !errors.Is(err, ErrClosed) {
				return fmt.Errorf("truncate after close: want ErrClosed, got %v", err)
			}
			return nil
		},
	},
}
