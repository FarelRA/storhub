package test

import (
	"bytes"
	"errors"
	"fmt"
)

var scenarioDirOpen = []Scenario{
	{
		Name:     "mkdir-rmdir-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			d := "/pc-mkdir-dir"
			if err := s.Mkdir(d, 0o755); err != nil {
				return fmt.Errorf("mkdir: %v", err)
			}
			if err := s.Mkdir(d, 0o755); !errors.Is(err, ErrExists) {
				return fmt.Errorf("second mkdir: want ErrExists, got %v", err)
			}
			f := d + "/f"
			if err := s.CreateFile(f, 0o644, false); err != nil {
				return fmt.Errorf("create inside dir: %v", err)
			}
			if err := s.Unlink(f); err != nil {
				return fmt.Errorf("unlink inside dir: %v", err)
			}
			if err := s.Rmdir(d); err != nil {
				return fmt.Errorf("rmdir: %v", err)
			}
			if err := s.Rmdir(d); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("second rmdir: want ErrNotFound, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "rmdir-nonempty-fails",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			d := "/pc-rmdir-nonempty"
			if err := s.Mkdir(d, 0o755); err != nil {
				return fmt.Errorf("mkdir: %v", err)
			}
			f := d + "/f"
			if err := s.CreateFile(f, 0o644, false); err != nil {
				return fmt.Errorf("create inside dir: %v", err)
			}
			if err := s.Rmdir(d); !errors.Is(err, ErrNotEmpty) {
				return fmt.Errorf("rmdir non-empty: want ErrNotEmpty, got %v", err)
			}
			if err := s.Unlink(f); err != nil {
				return fmt.Errorf("unlink: %v", err)
			}
			if err := s.Rmdir(d); err != nil {
				return fmt.Errorf("rmdir after emptying: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "stat-size-reflects-uncommitted-writes",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-stat-uncommitted"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenWriteOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write(bytes.Repeat([]byte("z"), 100)); err != nil {
				return fmt.Errorf("write: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Size != 100 {
				return fmt.Errorf("stat size before close: want 100, got %d", st.Size)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "open-truncate-clears-file",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-open-truncate"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("hello")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			h, err := s.Open(p, OpenTruncate, CreateNever)
			if err != nil {
				return fmt.Errorf("open truncate: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Size != 0 {
				return fmt.Errorf("size after truncate-open: want 0, got %d", st.Size)
			}
			r, err := s.Open(p, OpenReadOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open read-only: %v", err)
			}
			got, err := r.Read(8)
			if err != nil {
				return fmt.Errorf("read: %v", err)
			}
			if len(got) != 0 {
				return fmt.Errorf("content after truncate-open: want empty, got %q", got)
			}
			if err := r.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "open-mode-enforcement",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-open-mode"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("x")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			ro, err := s.Open(p, OpenReadOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open read-only: %v", err)
			}
			if _, err := ro.Write([]byte("y")); !errors.Is(err, ErrAccess) {
				_ = ro.Close()
				return fmt.Errorf("write on read-only handle: want ErrAccess, got %v", err)
			}
			if _, err := ro.PWrite(0, []byte("y")); !errors.Is(err, ErrAccess) {
				_ = ro.Close()
				return fmt.Errorf("pwrite on read-only handle: want ErrAccess, got %v", err)
			}
			if err := ro.Truncate(0); !errors.Is(err, ErrAccess) {
				_ = ro.Close()
				return fmt.Errorf("truncate on read-only handle: want ErrAccess, got %v", err)
			}
			if err := ro.Close(); err != nil {
				return fmt.Errorf("close ro: %v", err)
			}
			wo, err := s.Open(p, OpenWriteOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open write-only: %v", err)
			}
			if _, err := wo.Read(1); !errors.Is(err, ErrAccess) {
				_ = wo.Close()
				return fmt.Errorf("read on write-only handle: want ErrAccess, got %v", err)
			}
			if _, err := wo.PRead(0, 1); !errors.Is(err, ErrAccess) {
				_ = wo.Close()
				return fmt.Errorf("pread on write-only handle: want ErrAccess, got %v", err)
			}
			if err := wo.Close(); err != nil {
				return fmt.Errorf("close wo: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "open-path-no-perm-required",
		Surfaces: SurfaceFUSE,
		Run: func(s Surface) error {
			p := "/pc-open-path"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("data")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			// Mode 007 leaves the owner with zero permission bits while
			// surviving the metadata round-trip (mode 0 is the unset
			// sentinel and normalizes back to 644, so 000 cannot express
			// "no permission" here). The opener is the file owner, so a
			// read open is genuinely denied and only a permission-free
			// O_PATH-style open may succeed.
			if err := s.Chmod(p, 0o007); err != nil {
				return fmt.Errorf("chmod: %v", err)
			}
			// An O_PATH-style open requires no permission on the file.
			h, err := s.Open(p, OpenPath, CreateNever)
			if err != nil {
				return fmt.Errorf("O_PATH open without permission: %v", err)
			}
			// ...but carries no I/O rights either.
			if _, err := h.Read(1); !errors.Is(err, ErrAccess) {
				_ = h.Close()
				return fmt.Errorf("read on O_PATH handle: want ErrAccess, got %v", err)
			}
			if _, err := h.PRead(0, 1); !errors.Is(err, ErrAccess) {
				_ = h.Close()
				return fmt.Errorf("pread on O_PATH handle: want ErrAccess, got %v", err)
			}
			if _, err := h.Write([]byte("x")); !errors.Is(err, ErrAccess) {
				_ = h.Close()
				return fmt.Errorf("write on O_PATH handle: want ErrAccess, got %v", err)
			}
			if _, err := h.PWrite(0, []byte("x")); !errors.Is(err, ErrAccess) {
				_ = h.Close()
				return fmt.Errorf("pwrite on O_PATH handle: want ErrAccess, got %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			// Restore access and prove the bytes survived untouched.
			if err := s.Chmod(p, 0o644); err != nil {
				return fmt.Errorf("restore chmod: %v", err)
			}
			got, err := s.ReadRange(p, 0, 4)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "data" {
				return fmt.Errorf("content: want %q, got %q", "data", got)
			}
			// A missing path still reports not-found, never bare success.
			if _, err := s.Open("/pc-open-path-missing", OpenPath, CreateNever); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("O_PATH open missing: want ErrNotFound, got %v", err)
			}
			return nil
		},
	},
}
