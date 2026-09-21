package test

import (
	"bytes"
	"errors"
	"fmt"
)

var scenarioSeek = []Scenario{
	{
		Name:     "seek-data-hole-basic",
		Surfaces: SurfaceFUSE,
		Run: func(s Surface) error {
			p := "/pc-seek-basic"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("0123456789")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			h, err := s.Open(p, OpenReadOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			defer func() { _ = h.Close() }()
			seeker, ok := h.(SeekHandle)
			if !ok {
				return fmt.Errorf("seek: %w: handle cannot SEEK_DATA/SEEK_HOLE", ErrUnsupported)
			}
			if got, err := seeker.SeekData(0); err != nil || got != 0 {
				return fmt.Errorf("seek data 0: want 0, got %d, err %v", got, err)
			}
			if got, err := seeker.SeekData(4); err != nil || got != 4 {
				return fmt.Errorf("seek data 4: want 4, got %d, err %v", got, err)
			}
			if _, err := seeker.SeekData(10); !errors.Is(err, ErrUnsatisfiableRange) {
				return fmt.Errorf("seek data at EOF: want ErrUnsatisfiableRange, got %v", err)
			}
			if _, err := seeker.SeekData(99); !errors.Is(err, ErrUnsatisfiableRange) {
				return fmt.Errorf("seek data past EOF: want ErrUnsatisfiableRange, got %v", err)
			}
			if _, err := seeker.SeekData(-1); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("seek data negative: want ErrInvalid, got %v", err)
			}
			if got, err := seeker.SeekHole(0); err != nil || got != 10 {
				return fmt.Errorf("seek hole 0: want 10, got %d, err %v", got, err)
			}
			if got, err := seeker.SeekHole(7); err != nil || got != 10 {
				return fmt.Errorf("seek hole 7: want 10, got %d, err %v", got, err)
			}
			if got, err := seeker.SeekHole(10); err != nil || got != 10 {
				return fmt.Errorf("seek hole at EOF: want 10, got %d, err %v", got, err)
			}
			if _, err := seeker.SeekHole(11); !errors.Is(err, ErrUnsatisfiableRange) {
				return fmt.Errorf("seek hole past EOF: want ErrUnsatisfiableRange, got %v", err)
			}
			if _, err := seeker.SeekHole(-1); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("seek hole negative: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "fallocate-punch-hole-unsupported",
		Surfaces: SurfaceFUSE,
		Run: func(s Surface) error {
			p := "/pc-punch-hole"
			const original = "0123456789ABCDEF"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte(original)); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			puncher, ok := s.(PunchHoler)
			if !ok {
				return fmt.Errorf("punch hole: %w: surface cannot punch holes", ErrUnsupported)
			}
			switch err := puncher.PunchHole(p, 4, 8); {
			case err == nil:
				// Zero-fill emulation: the punched span reads back as
				// zeros (observably equal to a real hole punch on dense
				// bytes) while the edges stay intact.
				got, rerr := s.ReadRange(p, 0, 16)
				if rerr != nil {
					return fmt.Errorf("ranged read after punch: %v", rerr)
				}
				want := append([]byte("0123"), bytes.Repeat([]byte{0}, 8)...)
				want = append(want, "CDEF"...)
				if !bytes.Equal(got, want) {
					return fmt.Errorf("punched content: want %q, got %q", want, got)
				}
			case errors.Is(err, ErrUnsupported):
				// Honest EOPNOTSUPP: the content must be unchanged.
				got, rerr := s.ReadRange(p, 0, 16)
				if rerr != nil {
					return fmt.Errorf("ranged read after refused punch: %v", rerr)
				}
				if string(got) != original {
					return fmt.Errorf("refused punch changed content: want %q, got %q", original, got)
				}
			default:
				return fmt.Errorf("punch hole: want nil or ErrUnsupported, got %v", err)
			}
			// Negative args are invalid on every surface.
			if err := puncher.PunchHole(p, -1, 4); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("punch negative offset: want ErrInvalid, got %v", err)
			}
			if err := puncher.PunchHole(p, 0, -4); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("punch negative length: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "open-create-disposition",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-open-create-disp"
			// Without creation intent, a write-mode open of a missing
			// path reports ErrNotFound (POSIX ENOENT without O_CREAT).
			if _, err := s.Open(p, OpenWriteOnly, CreateNever); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("write open without create: want ErrNotFound, got %v", err)
			}
			if _, err := s.Open(p, OpenReadWrite, CreateNever); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("readwrite open without create: want ErrNotFound, got %v", err)
			}
			// With creation intent, the same open creates (never
			// exclusive: no truncation, no failure on existing files).
			w, err := s.Open(p, OpenWriteOnly, CreateIfMissing)
			if err != nil {
				return fmt.Errorf("write open with create: %v", err)
			}
			if _, err := w.Write([]byte("created")); err != nil {
				_ = w.Close()
				return fmt.Errorf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			got, err := s.ReadRange(p, 0, 7)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "created" {
				return fmt.Errorf("created content: want %q, got %q", "created", got)
			}
			// CreateIfMissing over an existing file opens normally.
			r, err := s.Open(p, OpenReadOnly, CreateIfMissing)
			if err != nil {
				return fmt.Errorf("reopen with create: %v", err)
			}
			got, err = r.Read(7)
			if err != nil {
				_ = r.Close()
				return fmt.Errorf("read: %v", err)
			}
			if string(got) != "created" {
				_ = r.Close()
				return fmt.Errorf("reopened content: want %q, got %q", "created", got)
			}
			if err := r.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			// Read-only opens never create, whatever the disposition.
			if _, err := s.Open("/pc-open-create-disp-ro", OpenReadOnly, CreateIfMissing); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("read-only open with create: want ErrNotFound, got %v", err)
			}
			if _, err := s.Stat("/pc-open-create-disp-ro"); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("read-only open must not create: %v", err)
			}
			// A bad disposition fails loudly.
			if _, err := s.Open(p, OpenReadOnly, CreateDisposition(0)); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("bad disposition: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "umask-masks-create-mode",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			us, ok := s.(UmaskSurface)
			if !ok {
				return fmt.Errorf("surface lacks umask primitive")
			}
			us.SetUmask(0o022)
			defer us.SetUmask(0)
			p := "/pc-umask-masked"
			if err := s.CreateFile(p, 0o777, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Mode&0o777 != 0o755 {
				return fmt.Errorf("masked mode: want 755, got %o", st.Mode&0o777)
			}
			// Chmod sets exact modes and is never masked.
			if err := s.Chmod(p, 0o777); err != nil {
				return fmt.Errorf("chmod: %v", err)
			}
			st, err = s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat after chmod: %v", err)
			}
			if st.Mode&0o777 != 0o777 {
				return fmt.Errorf("chmod under mask: want 777, got %o", st.Mode&0o777)
			}
			// Files created by Open under CreateIfMissing observe the
			// mask too (created 0o644, which 022 leaves alone).
			q := "/pc-umask-open-created"
			h, err := s.Open(q, OpenWriteOnly, CreateIfMissing)
			if err != nil {
				return fmt.Errorf("open with create: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			st, err = s.Stat(q)
			if err != nil {
				return fmt.Errorf("stat open-created: %v", err)
			}
			if st.Mode&0o777 != 0o644 {
				return fmt.Errorf("open-created mode: want 644, got %o", st.Mode&0o777)
			}
			return nil
		},
	},
}
