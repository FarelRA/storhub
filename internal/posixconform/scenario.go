package posixconform

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Surface selector bitmask for Scenario.Surfaces.
const (
	// SurfaceFUSE selects the FUSE mount surface.
	SurfaceFUSE uint32 = 1 << iota
	// SurfaceREST selects the REST API surface.
	SurfaceREST
	// SurfaceCLI selects the CLI surface.
	SurfaceCLI
)

// SurfaceAll selects every known surface.
const SurfaceAll = SurfaceFUSE | SurfaceREST | SurfaceCLI

// Scenario is one deterministic conformance check against a Surface.
// Each Run must use its own unique paths and avoid sleeps and wall-clock reads.
type Scenario struct {
	Name     string
	Surfaces uint32 // bitmask: SurfaceFUSE, SurfaceREST, SurfaceCLI, SurfaceAll
	Run      func(Surface) error
}

// Table is the full conformance suite.
var Table = []Scenario{
	{
		Name:     "basic-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-basic-roundtrip"
			if _, err := s.Open(p, OpenReadOnly); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("open missing read-only: want ErrNotFound, got %v", err)
			}
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			w, err := s.Open(p, OpenWriteOnly)
			if err != nil {
				return fmt.Errorf("open write-only: %v", err)
			}
			if _, err := w.Write([]byte("hello")); err != nil {
				_ = w.Close()
				return fmt.Errorf("write: %v", err)
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			r, err := s.Open(p, OpenReadOnly)
			if err != nil {
				return fmt.Errorf("open read-only: %v", err)
			}
			got, err := r.Read(5)
			if err != nil {
				return fmt.Errorf("read: %v", err)
			}
			if string(got) != "hello" {
				return fmt.Errorf("read: want %q, got %q", "hello", got)
			}
			st, err := s.Stat(p)
			if err != nil {
				return fmt.Errorf("stat: %v", err)
			}
			if st.Size != 5 {
				return fmt.Errorf("stat size: want 5, got %d", st.Size)
			}
			if err := r.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "create-nonexclusive-idempotent",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-create-idempotent"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("first create: %v", err)
			}
			h, err := s.Open(p, OpenWriteOnly)
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
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("second non-exclusive create: want nil, got %v", err)
			}
			if err := s.CreateFile(p, 0o644, true); !errors.Is(err, ErrExists) {
				return fmt.Errorf("exclusive create over existing: want ErrExists, got %v", err)
			}
			got, err := s.ReadRange(p, 0, 1)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if string(got) != "x" {
				return fmt.Errorf("repeat create truncated file: want %q, got %q", "x", got)
			}
			return nil
		},
	},
	{
		Name:     "create-exclusive-race",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-exclusive-race"
			const n = 16
			var okCount, existsCount atomic.Int32
			var wg sync.WaitGroup
			errCh := make(chan error, n)
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					switch err := s.CreateFile(p, 0o644, true); {
					case err == nil:
						okCount.Add(1)
					case errors.Is(err, ErrExists):
						existsCount.Add(1)
					default:
						errCh <- err
					}
				}()
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				return fmt.Errorf("exclusive create returned unexpected error: %v", err)
			}
			if okCount.Load() != 1 {
				return fmt.Errorf("exclusive race: want exactly 1 success, got %d", okCount.Load())
			}
			if existsCount.Load() != n-1 {
				return fmt.Errorf("exclusive race: want %d ErrExists, got %d", n-1, existsCount.Load())
			}
			return nil
		},
	},
	{
		Name:     "concurrent-nonoverlapping-pwrite",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-concurrent-pwrite"
			const workers = 8
			const stride = 16
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Truncate(p, workers*stride); err != nil {
				return fmt.Errorf("truncate: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite)
			if err != nil {
				return fmt.Errorf("open: %v", err)
			}
			want := make([]byte, 0, workers*stride)
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				chunk := bytes.Repeat([]byte{byte('a' + i)}, stride)
				want = append(want, chunk...)
				wg.Add(1)
				go func(off int64, data []byte) {
					defer wg.Done()
					if _, err := h.PWrite(off, data); err != nil {
						errCh <- err
					}
				}(int64(i*stride), chunk)
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				return fmt.Errorf("concurrent pwrite failed: %v", err)
			}
			got, err := s.ReadRange(p, 0, workers*stride)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("byte-exact mismatch: want %q, got %q", want, got)
			}
			if err := h.Close(); err != nil {
				return fmt.Errorf("close: %v", err)
			}
			return nil
		},
	},
	{
		Name:     "concurrent-append",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-concurrent-append"
			const workers = 8
			const perWorker = 32
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			for i := 0; i < workers; i++ {
				wg.Add(1)
				go func(b byte) {
					defer wg.Done()
					if err := s.Append(p, bytes.Repeat([]byte{b}, perWorker)); err != nil {
						errCh <- err
					}
				}(byte('A' + i))
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				return fmt.Errorf("concurrent append failed: %v", err)
			}
			got, err := s.ReadRange(p, 0, workers*perWorker)
			if err != nil {
				return fmt.Errorf("ranged read: %v", err)
			}
			if len(got) != workers*perWorker {
				return fmt.Errorf("append size: want %d, got %d", workers*perWorker, len(got))
			}
			for i := 0; i < workers; i++ {
				if c := bytes.Count(got, []byte{byte('A' + i)}); c != perWorker {
					return fmt.Errorf("byte %q: want %d copies, got %d", byte('A'+i), perWorker, c)
				}
			}
			return nil
		},
	},
	{
		Name:     "cross-handle-read-your-writes",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-cross-handle-ryw"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h1, err := s.Open(p, OpenReadWrite)
			if err != nil {
				return fmt.Errorf("open h1: %v", err)
			}

			h2, err := s.Open(p, OpenReadWrite)
			if err != nil {
				return fmt.Errorf("open h2: %v", err)
			}

			want := []byte("data-should-match")
			if _, err := h1.Write(want); err != nil {
				return fmt.Errorf("h1 write: %v", err)
			}
			got, err := h2.PRead(0, len(want))
			if err != nil {
				return fmt.Errorf("h2 pread: %v", err)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("h2 pread: want %q, got %q", want, got)
			}
			got, err = h2.Read(len(want))
			if err != nil {
				return fmt.Errorf("h2 cursor read: %v", err)
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("h2 cursor read: want %q, got %q", want, got)
			}
			if err := h1.Close(); err != nil {
				return fmt.Errorf("close h1: %v", err)
			}
			if err := h2.Close(); err != nil {
				return fmt.Errorf("close h2: %v", err)
			}
			return nil
		},
	},
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
			h, err := s.Open(p, OpenWriteOnly)
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
			h, err := s.Open(p, OpenReadWrite)
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
	{
		Name:     "unlink-while-open",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-unlink-while-open"
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			h, err := s.Open(p, OpenReadWrite)
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
			h, err := s.Open(p, OpenReadWrite)
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
		Name:     "rename-no-replace-onto-existing-fails",
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
				return fmt.Errorf("no-replace rename: want ErrExists, got %v", err)
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
			if err := s.Symlink(b, a); err != nil {
				return fmt.Errorf("symlink a: %v", err)
			}
			if err := s.Symlink(a, b); err != nil {
				return fmt.Errorf("symlink b: %v", err)
			}
			if _, err := s.Stat(a); !errors.Is(err, ErrLoop) {
				return fmt.Errorf("stat loop: want ErrLoop, got %v", err)
			}
			if _, err := s.Open(a, OpenReadOnly); !errors.Is(err, ErrLoop) {
				return fmt.Errorf("open loop: want ErrLoop, got %v", err)
			}
			return nil
		},
	},
	{
		Name:     "symlink-readlink-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			target := "/pc-symlink-target"
			link := "/pc-symlink-link"
			if err := s.CreateFile(target, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			if err := s.Append(target, []byte("payload")); err != nil {
				return fmt.Errorf("append: %v", err)
			}
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
			if _, err := s.Readlink(target); !errors.Is(err, ErrInvalid) {
				return fmt.Errorf("readlink on regular file: want ErrInvalid, got %v", err)
			}
			return nil
		},
	},
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
			h, err := s.Open(p, OpenWriteOnly)
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
			h, err := s.Open(p, OpenWriteOnly)
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
			h, err := s.Open(p, OpenTruncate)
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
			r, err := s.Open(p, OpenReadOnly)
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
			ro, err := s.Open(p, OpenReadOnly)
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
			wo, err := s.Open(p, OpenWriteOnly)
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
}
