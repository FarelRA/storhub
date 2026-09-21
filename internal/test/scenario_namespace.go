package test

import (
	"errors"
	"fmt"
)

var scenarioNamespace = []Scenario{
	{
		Name:     "rename-noreplace-onto-existing-fails",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			src := "/pc-rename-noreplace-src"
			dst := "/pc-rename-noreplace-dst"
			if err := s.CreateFile(src, 0o644, false); err != nil {
				return fmt.Errorf("create src: %v", err)
			}
			if err := s.CreateFile(dst, 0o644, false); err != nil {
				return fmt.Errorf("create dst: %v", err)
			}
			if err := s.Append(src, []byte("src")); err != nil {
				return fmt.Errorf("append src: %v", err)
			}
			if err := s.Append(dst, []byte("dst")); err != nil {
				return fmt.Errorf("append dst: %v", err)
			}
			if err := s.Rename(src, dst, true); !errors.Is(err, ErrExists) {
				return fmt.Errorf("noreplace rename: want ErrExists, got %v", err)
			}
			got, err := s.ReadRange(dst, 0, 3)
			if err != nil {
				return fmt.Errorf("ranged read dst: %v", err)
			}
			if string(got) != "dst" {
				return fmt.Errorf("dst clobbered: want %q, got %q", "dst", got)
			}
			if _, err := s.Stat(src); err != nil {
				return fmt.Errorf("src missing after failed rename: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "rename-replace-succeeds",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			src := "/pc-rename-replace-src"
			dst := "/pc-rename-replace-dst"
			if err := s.CreateFile(src, 0o644, false); err != nil {
				return fmt.Errorf("create src: %v", err)
			}
			if err := s.CreateFile(dst, 0o644, false); err != nil {
				return fmt.Errorf("create dst: %v", err)
			}
			if err := s.Append(src, []byte("new")); err != nil {
				return fmt.Errorf("append src: %v", err)
			}
			if err := s.Append(dst, []byte("old")); err != nil {
				return fmt.Errorf("append dst: %v", err)
			}
			if err := s.Rename(src, dst, false); err != nil {
				return fmt.Errorf("rename: %v", err)
			}
			if _, err := s.Stat(src); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat old name: want ErrNotFound, got %v", err)
			}
			got, err := s.ReadRange(dst, 0, 3)
			if err != nil {
				return fmt.Errorf("ranged read dst: %v", err)
			}
			if string(got) != "new" {
				return fmt.Errorf("dst content: want %q, got %q", "new", got)
			}
			return nil
		},
	},
	{
		Name:     "symlink-loop-eloop",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			a := "/pc-loop-a"
			b := "/pc-loop-b"
			// Relative targets: an absolute target escapes a real
			// mount (the kernel resolves it against the host root,
			// correctly yielding ENOENT), so loop detection on a
			// mount is only testable with contained targets. The
			// property under test — ELOOP on a cycle — is identical.
			if err := s.Symlink("pc-loop-b", a); err != nil {
				return fmt.Errorf("symlink a: %v", err)
			}
			if err := s.Symlink("pc-loop-a", b); err != nil {
				return fmt.Errorf("symlink b: %v", err)
			}
			if _, err := s.Stat(a); !errors.Is(err, ErrLoop) {
				return fmt.Errorf("stat loop: want ErrLoop, got %v", err)
			}
			if _, err := s.Open(a, OpenReadOnly, CreateNever); !errors.Is(err, ErrLoop) {
				return fmt.Errorf("open loop: want ErrLoop, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "symlink-readlink-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			target := "pc-symlink-target"
			link := "/pc-symlink-link"
			if err := s.CreateFile("/"+target, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append("/"+target, []byte("payload")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			// Relative target: stays inside a real mount (absolute
			// targets escape to the host root by design). Readlink
			// still returns the stored string verbatim.
			if err := s.Symlink(target, link); err != nil {
				return fmt.Errorf("symlink: %v", err)
			}
			got, err := s.Readlink(link)
			if err != nil {
				return fmt.Errorf("readlink: %v", err)
			}
			if got != target {
				return fmt.Errorf("readlink: want %q, got %q", target, got)
			}
			st, err := s.Stat(link)
			if err != nil {
				return fmt.Errorf("stat through link: %v", err)
			}
			if st.Size != 7 {
				return fmt.Errorf("stat through link size: want 7, got %d", st.Size)
			}
			data, err := s.ReadRange(link, 0, 7)
			if err != nil {
				return fmt.Errorf("read through link: %v", err)
			}
			if string(data) != "payload" {
				return fmt.Errorf("link content: want %q, got %q", "payload", data)
			}
			if _, err := s.Readlink("/" + target); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("readlink on regular file: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
}
