package test

import (
	"errors"
	"fmt"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
)

var scenarioScratch = []Scenario{
	{
		// Session primitives have no fd spelling, so these rows run
		// where stateful descriptions exist (REST, CLI, oracle) and
		// skip FUSE mounts: a kernel fd cannot be named after open
		// without O_TMPFILE, which the mount does not implement.
		Name:     "scratch-link-close-publishes",
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
			if _, err := h.Write([]byte("staged")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			got, err := h.PRead(0, 6)
			if err != nil {
				return fmt.Errorf("pread scratch: %v", err)
			}
			if string(got) != "staged" {
				return fmt.Errorf("scratch read: want %q, got %q", "staged", got)
			}
			p := "/pc-scratch-linked"
			if err := h.Link(p); err != nil {
				return fmt.Errorf("link: %v", err)
			}
			// Linking stages the creation: the name stays absent
			// until close commits it.
			if _, err := s.Stat(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("stat before close: want ErrNotFound, got %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			got, err = s.ReadRange(p, 0, 6)
			if err != nil {
				return fmt.Errorf("read published: %v", err)
			}
			if string(got) != "staged" {
				return fmt.Errorf("published content: want %q, got %q", "staged", got)
			}
			return nil
		},
	},
	{
		Name:     "scratch-unlink-before-commit-still-publishes",
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
			if _, err := h.Write([]byte("staged")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			p := "/pc-scratch-unlinked"
			if err := h.Link(p); err != nil {
				return fmt.Errorf("link: %v", err)
			}
			// The link staged a creation but published nothing, so
			// unlinking the staged-only name reports NotFound and
			// changes no state: the later close still publishes.
			if err := s.Unlink(p); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("unlink staged name: want ErrNotFound, got %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			got, err := s.ReadRange(p, 0, 6)
			if err != nil {
				return fmt.Errorf("read published: %v", err)
			}
			if string(got) != "staged" {
				return fmt.Errorf("published content: want %q, got %q", "staged", got)
			}
			return nil
		},
	},
	{
		Name:     "scratch-link-existing-fails",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			p := "/pc-scratch-taken"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := ss.OpenScratch(0)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("staged")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			if err := h.Link(p); !errors.Is(err, ErrExists) {
				return fmt.Errorf("link onto existing: want ErrExists, got %v", err)
			}
			q := "/pc-scratch-free"
			if err := h.Link(q); err != nil {
				return fmt.Errorf("link free name: %v", err)
			}
			if err := h.Link(q); !errors.Is(err, ErrExists) {
				return fmt.Errorf("second link: want ErrExists, got %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			got, err := s.ReadRange(q, 0, 6)
			if err != nil {
				return fmt.Errorf("read published: %v", err)
			}
			if string(got) != "staged" {
				return fmt.Errorf("published content: want %q, got %q", "staged", got)
			}
			return nil
		},
	},
	{
		Name:     "scratch-relink-after-taken-target",
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
			p := "/pc-relink-taken"
			if err := h.Link(p); err != nil {
				return fmt.Errorf("link: %v", err)
			}
			// A concurrent writer takes the linked name before close.
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("take name: %v", err)
			}
			// The wedged close fails loudly WITHOUT consuming the
			// description: relink can still rescue it.
			if err := h.Close(); !errors.Is(err, ErrExists) {
				return fmt.Errorf("close on taken target: want ErrExists, got %v", err)
			}
			q := "/pc-relink-rescued"
			if err := h.Relink(q); err != nil {
				return fmt.Errorf("relink: %v", err)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close after relink: %v", err)
			}
			got, err := s.ReadRange(q, 0, 4)
			if err != nil {
				return fmt.Errorf("read rescued: %v", err)
			}
			if string(got) != "data" {
				return fmt.Errorf("rescued content: want %q, got %q", "data", got)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat taken name: %v", err)
			}
			if st.Size != 0 {
				return fmt.Errorf("taken name must stay untouched, size %d", st.Size)
			}
			return nil
		},
	},
	{
		Name:     "scratch-ttl-expiry-stales-handle",
		Surfaces: SurfaceREST | SurfaceCLI,
		Run: func(s Surface) error {
			ss, ok := s.(SessionSurface)
			if !ok {
				return fmt.Errorf("surface lacks session primitives")
			}
			h, err := ss.OpenScratch(100 * time.Millisecond)
			if err != nil {
				return fmt.Errorf("open scratch: %v", err)
			}
			if _, err := h.Write([]byte("data")); err != nil {
				return fmt.Errorf("write scratch: %v", err)
			}
			// Idle past the TTL lapses the description lease. Poll until
			// the first op reports stale (deadline 1 PatienceUnit,
			// step 1 TickUnit): expiry is checked lazily on use, so
			// fast hosts converge early and slow hosts still meet
			// the deadline without a fixed 1s wall sleep.
			deadline := time.Now().Add(storcfg.PatienceUnit)
			for {
				_, err := h.PRead(0, 4)
				if errors.Is(err, ErrStale) {
					break
				}
				if err != nil {
					return fmt.Errorf("pread while awaiting expiry: want ErrStale or nil, got %v", err)
				}
				if !time.Now().Before(deadline) {
					return fmt.Errorf("pread after expiry: want ErrStale, handle never lapsed")
				}
				time.Sleep(storcfg.TickUnit)
			}
			if _, err := h.PRead(0, 4); !errors.Is(err, ErrStale) {
				return fmt.Errorf("pread after expiry: want ErrStale, got %v", err)
			}
			if _, err := h.Write([]byte("x")); !errors.Is(err, ErrStale) {
				return fmt.Errorf("write after expiry: want ErrStale, got %v", err)
			}
			if err := h.Close(); !errors.Is(err, ErrStale) {
				return fmt.Errorf("close after expiry: want ErrStale, got %v", err)
			}
			return nil
		},
	},
}
