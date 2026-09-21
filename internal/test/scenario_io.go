package test

import (
	"errors"
	"fmt"
)

var scenarioIO = []Scenario{
	{
		Name:     "ranged-read-basic",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-ranged-basic"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("0123456789")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			got, err := s.ReadRange(p, 2, 4)
			if err != nil {
				return fmt.Errorf("mid read: %v", err)
			}
			if string(got) != "2345" {
				return fmt.Errorf("mid read: want %q, got %q", "2345", got)
			}
			got, err = s.ReadRange(p, 8, 100)
			if err != nil {
				return fmt.Errorf("clamped read: %v", err)
			}
			if string(got) != "89" {
				return fmt.Errorf("clamped read: want %q, got %q", "89", got)
			}
			got, err = s.ReadRange(p, 0, 10)
			if err != nil {
				return fmt.Errorf("full read: %v", err)
			}
			if string(got) != "0123456789" {
				return fmt.Errorf("full read: want %q, got %q", "0123456789", got)
			}
			return nil
		},
	},
	{
		Name:     "ranged-read-unsatisfiable",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-ranged-unsat"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("0123456789")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			if _, err := s.ReadRange(p, 10, 1); !errors.Is(err, ErrUnsatisfiableRange) {
				return fmt.Errorf("read at EOF: want ErrUnsatisfiableRange, got %v", err)
			}
			if _, err := s.ReadRange(p, 99, 1); !errors.Is(err, ErrUnsatisfiableRange) {
				return fmt.Errorf("read past EOF: want ErrUnsatisfiableRange, got %v", err)
			}
			if _, err := s.ReadRange(p, -1, 2); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("negative offset: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "cas-write-success",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-cas-success"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("aaaa")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			rev1, err := s.Revision(p)
			if err != nil {
				return fmt.Errorf("revision: %v", err)
			}
			if err := s.CompareAndWrite(p, 0, []byte("bbbb"), rev1); err != nil {
				return fmt.Errorf("cas write: %v", err)
			}
			got, err := s.ReadRange(p, 0, 4)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "bbbb" {
				return fmt.Errorf("cas content: want %q, got %q", "bbbb", got)
			}
			rev2, err := s.Revision(p)
			if err != nil {
				return fmt.Errorf("second revision: %v", err)
			}
			if rev2 == rev1 {
				return fmt.Errorf("revision did not advance after cas write")
			}
			return nil
		},
	},
	{
		Name:     "cas-write-stale-token-fails",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-cas-stale"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(p, []byte("one!")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
			stale, err := s.Revision(p)
			if err != nil {
				return fmt.Errorf("revision: %v", err)
			}
			if err := s.Append(p, []byte("XXX")); err != nil {
				return fmt.Errorf("intervening append: %v", err)
			}
			current, err := s.Revision(p)
			if err != nil {
				return fmt.Errorf("second revision: %v", err)
			}
			err = s.CompareAndWrite(p, 0, []byte("stale"), stale)
			var pre ErrPrecondition
			if !errors.As(err, &pre) {
				return fmt.Errorf("stale cas: want ErrPrecondition, got %v", err)
			}
			if pre.Actual != current {
				return fmt.Errorf("precondition actual: want %d, got %d", current, pre.Actual)
			}
			got, err := s.ReadRange(p, 0, 7)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "one!XXX" {
				return fmt.Errorf("stale write applied: want %q, got %q", "one!XXX", got)
			}
			return nil
		},
	},
	{
		Name:     "sync-durability-marker",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-sync-marker"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenWriteOnly, CreateNever)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			if _, err := h.Write([]byte("durable")); err != nil {
				_ = h.Close()
				return fmt.Errorf("write: %v", err)
			}
			if err := h.Sync(); err != nil {
				_ = h.Close()
				return fmt.Errorf("handle sync: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			if err := s.Sync(p); err != nil {
				return fmt.Errorf("sync: want nil, got %v", err)
			}
			got, err := s.ReadRange(p, 0, 7)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "durable" {
				return fmt.Errorf("post-sync content: want %q, got %q", "durable", got)
			}
			return nil
		},
	},
}
