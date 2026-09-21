package test

import (
	"errors"
	"fmt"
)

var scenarioScratchMulti = []Scenario{
	{
		// Multi-name scratch rows run on the oracle only until the CLI
		// and REST fake mirrors land (see the table notes): the session
		// fakes live outside the owned files, so adapter suites skip
		// these rows until the prod worker wires test.PendingNames in.
		Name:     "scratch-link-two-names-both-publish",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			h, err := ss.OpenScratch(0)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("shared")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			a := "/pc-scratch-multi-a"
			b := "/pc-scratch-multi-b"
			if err := h.Link(a); err != nil {
				return fmt.Errorf("link a: %v", err)
			}
			if err := h.Link(b); err != nil {
				return fmt.Errorf("link b: %v", err)
			}
			// Linking stages creation: both names stay absent until
			// close commits them.
			if _, err := s.Stat(a); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat a before close: want ErrNotFound, got %v", err)
			}
			if _, err := s.Stat(b); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat b before close: want ErrNotFound, got %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			for _, p := range []string{a, b} {
				got, err := s.ReadRange(p, 0, 6)
				if err != nil {
					return fmt.Errorf("read published %s: %v", p, err)
				}
				if string(got) != "shared" {
					return fmt.Errorf("published %s: want %q, got %q", p, "shared", got)
				}
			}
			return nil
		},
	},
	{
		Name:     "scratch-link-duplicate-name-fails",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			h, err := ss.OpenScratch(0)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("shared")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			a := "/pc-scratch-dup-a"
			b := "/pc-scratch-dup-b"
			if err := h.Link(a); err != nil {
				return fmt.Errorf("link a: %v", err)
			}
			// Linking an already-pending name fails, and the handle
			// stays usable for a fresh name.
			if err := h.Link(a); !errors.Is(err, ErrExists) {
				return fmt.Errorf("duplicate link: want ErrExists, got %v", err)
			}
			if err := h.Link(b); err != nil {
				return fmt.Errorf("link b after duplicate: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			for _, p := range []string{a, b} {
				got, err := s.ReadRange(p, 0, 6)
				if err != nil {
					return fmt.Errorf("read published %s: %v", p, err)
				}
				if string(got) != "shared" {
					return fmt.Errorf("published %s: want %q, got %q", p, "shared", got)
				}
			}
			return nil
		},
	},
	{
		Name:     "scratch-relink-replaces-pending-set",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			h, err := ss.OpenScratch(0)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("data")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			if err := h.Link("/pc-scratch-replaced-a"); err != nil {
				return fmt.Errorf("link a: %v", err)
			}
			if err := h.Link("/pc-scratch-replaced-b"); err != nil {
				return fmt.Errorf("link b: %v", err)
			}
			// Relink replaces the whole pending set with one path.
			c := "/pc-scratch-replaced-c"
			if err := h.Relink(c); err != nil {
				return fmt.Errorf("relink: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			got, err := s.ReadRange(c, 0, 4)
			if err != nil {
				return fmt.Errorf("read rescued: %v", err)
			}
			if string(got) != "data" {
				return fmt.Errorf("rescued content: want %q, got %q", "data", got)
			}
			for _, p := range []string{"/pc-scratch-replaced-a", "/pc-scratch-replaced-b"} {
				if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
					return fmt.Errorf("stat replaced %s: want ErrNotFound, got %v", p, err)
				}
			}
			return nil
		},
	},
	{
		Name:     "scratch-multi-close-atomic-on-taken",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			h, err := ss.OpenScratch(0)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("shared")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			a := "/pc-scratch-atomic-a"
			b := "/pc-scratch-atomic-b"
			if err := h.Link(a); err != nil {
				return fmt.Errorf("link a: %v", err)
			}
			if err := h.Link(b); err != nil {
				return fmt.Errorf("link b: %v", err)
			}
			// A concurrent writer takes one name before close.
			if err := s.CreateFile(b, 0o644, false); err != nil {
				return fmt.Errorf("take name: %v", err)
			}
			// Every name is pre-validated: the close fails WITHOUT
			// publishing to the free name, and the handle stays open.
			if err := h.Close(); !errors.Is(err, ErrExists) {
				return fmt.Errorf("close on taken target: want ErrExists, got %v", err)
			}
			if _, err := s.Stat(a); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("atomic close published the free name %s: %v", a, err)
			}
			st, err := s.Stat(b)
			if err != nil {
				return fmt.Errorf("stat taken name: %v", err)
			}
			if st.Size != 0 {
				return fmt.Errorf("taken name must stay untouched, size %d", st.Size)
			}
			// Relink rescues the still-open description.
			c := "/pc-scratch-atomic-c"
			if err := h.Relink(c); err != nil {
				return fmt.Errorf("relink: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close after relink: %v", err)
			}
			got, err := s.ReadRange(c, 0, 6)
			if err != nil {
				return fmt.Errorf("read rescued: %v", err)
			}
			if string(got) != "shared" {
				return fmt.Errorf("rescued content: want %q, got %q", "shared", got)
			}
			return nil
		},
	},
}
