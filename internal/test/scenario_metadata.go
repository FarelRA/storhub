package test

import (
	"bytes"
	"fmt"
)

var scenarioMetadata = []Scenario{
	{
		Name:     "truncate-extend-zeroes",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-truncate-extend"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("abc")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			if err := s.Truncate(p, 6); err != nil {
				return fmt.Errorf("truncate: %v", err)
			}
			got, err := s.ReadRange(p, 0, 6)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			want := []byte{'a', 'b', 'c', 0, 0, 0}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("extended tail: want %q, got %q", want, got)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Size != 6 {
				return fmt.Errorf("stat size: want 6, got %d", st.Size)
			}
			return nil
		},
	},
	{
		Name:     "truncate-shrink",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-truncate-shrink"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("12345678")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			if err := s.Truncate(p, 3); err != nil {
				return fmt.Errorf("truncate: %v", err)
			}
			got, err := s.ReadRange(p, 0, 3)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "123" {
				return fmt.Errorf("shrunk content: want %q, got %q", "123", got)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Size != 3 {
				return fmt.Errorf("stat size: want 3, got %d", st.Size)
			}
			return nil
		},
	},
	{
		Name:     "chmod-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-chmod-roundtrip"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			for _, mode := range []uint32{0o640, 0o755} {
				if err := s.Chmod(p, mode); err != nil {
					return fmt.Errorf("chmod %o: %v", mode, err)
				}
				st, err := s.Stat(p)
				if err != nil {
					return fmt.Errorf("stat: %v", err)
				}
				if st.Mode&0o7777 != mode {
					return fmt.Errorf("mode: want %o, got %o", mode, st.Mode&0o7777)
				}
			}
			return nil
		},
	},
	{
		Name:     "chown-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-chown-roundtrip"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			// Chown to the current owner: the only chown a non-privileged
			// caller is guaranteed on every enforcing surface (owner may
			// always reassert its own uid/gid). Cross-owner chown is
			// root-only and correctly EPERM elsewhere, so it cannot live
			// in a portable scenario. Ownership-changing chown is pinned
			// by storage-layer unit tests with mock identities instead.
			self, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if err := s.Chown(p, self.UID, self.GID); err != nil {
				return fmt.Errorf("chown: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.UID != self.UID || st.GID != self.GID {
				return fmt.Errorf("ownership: want %d/%d, got %d/%d", self.UID, self.GID, st.UID, st.GID)
			}
			if st.Mode&0o7777 != 0o644 {
				return fmt.Errorf("chown changed permission bits: want 644, got %o", st.Mode&0o7777)
			}
			return nil
		},
	},
	{
		Name:     "utimens-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-utimens-roundtrip"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			const mtime = int64(1700000000123456789)
			if err := s.Utimens(p, mtime); err != nil {
				return fmt.Errorf("utimens: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.MTime != mtime {
				return fmt.Errorf("mtime: want %d, got %d", mtime, st.MTime)
			}
			return nil
		},
	},
	{
		Name:     "setuid-cleared-on-write",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-setuid-clear-write"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Chmod(p, 0o4755); err != nil {
				return fmt.Errorf("chmod: %v", err)
			}
			h, err := s.Open(p, OpenWriteOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("x")); err != nil {
				_ = h.Close()
				return fmt.Errorf("write: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Mode&SetUIDBit != 0 {
				return fmt.Errorf("setuid survived data write: mode %o", st.Mode&0o7777)
			}
			if st.Mode&0o777 != 0o755 {
				return fmt.Errorf("base bits changed: want 755, got %o", st.Mode&0o777)
			}
			return nil
		},
	},
	{
		Name:     "setgid-cleared-on-write",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-setgid-clear-write"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Chmod(p, 0o2755); err != nil {
				return fmt.Errorf("chmod: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.PWrite(0, []byte("y")); err != nil {
				_ = h.Close()
				return fmt.Errorf("pwrite: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Mode&SetGIDBit != 0 {
				return fmt.Errorf("setgid survived data write: mode %o", st.Mode&0o7777)
			}
			if st.Mode&0o777 != 0o755 {
				return fmt.Errorf("base bits changed: want 755, got %o", st.Mode&0o777)
			}
			return nil
		},
	},
	{
		Name:     "setuid-setgid-cleared-on-chown",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-setid-clear-chown"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Chmod(p, 0o6750); err != nil {
				return fmt.Errorf("chmod: %v", err)
			}
			// Same portable-chown rule as chown-roundtrip: reassert the
			// current owner. A non-privileged chown that succeeds must
			// clear setuid/setgid; cross-owner chown is EPERM by design
			// on enforcing surfaces and lives in unit tests instead.
			self, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if err := s.Chown(p, self.UID, self.GID); err != nil {
				return fmt.Errorf("chown: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Mode&(SetUIDBit|SetGIDBit) != 0 {
				return fmt.Errorf("setid bits survived chown: mode %o", st.Mode&0o7777)
			}
			if st.Mode&0o777 != 0o750 {
				return fmt.Errorf("base bits changed: want 750, got %o", st.Mode&0o777)
			}
			if st.UID != self.UID || st.GID != self.GID {
				return fmt.Errorf("ownership: want %d/%d, got %d/%d", self.UID, self.GID, st.UID, st.GID)
			}
			return nil
		},
	},
}
