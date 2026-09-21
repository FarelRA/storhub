package test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	storcfg "github.com/FarelRA/storhub/internal/config"
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

// Table notes (contract divergences documented, not faked).
//
//   - Open carries creation intent in disp, like O_CREAT on open(2):
//     portable scenarios create with CreateFile first, then open with
//     CreateNever, and a write-mode open of a missing path with
//     CreateNever expects ErrNotFound (POSIX ENOENT without O_CREAT),
//     which the MemSurface oracle enforces. The FUSE, REST and CLI
//     adapters honor disp through their write-mode create paths (a
//     missing file under CreateIfMissing is created with mode 0o644
//     before opening; OpenReadOnly and OpenPath never create), so
//     creation is one spelled contract, not a divergence.
//   - Chown pins the non-privileged rule only: any successful chown in
//     the table must clear setuid/setgid (cross-owner chown is EPERM on
//     enforcing surfaces and cannot be portable, so scenarios reassert
//     the current owner). The admin-keeps-bits exemption lives outside
//     the interface (MemSurface.ChownAdmin) with its real proof in the
//     storage-layer unit tests, which own caller identity.
//   - umask is expressed through the optional UmaskSurface capability:
//     SetUmask configures the creation mask that CreateFile (and files
//     created by Open under CreateIfMissing) must observe, pinned by
//     the umask-masks-create-mode scenario. The fixed 022 default plus
//     the --umask mount/serve override stays pinned at the product
//     layer by TestMountUmaskFlagMasksCreatedModes
//     (internal/cli/app_test.go) and TestCallerContextCarriesDefaultUmask
//     (internal/fusefs/fuse_dac_test.go): the conformance mask starts
//     at zero and each scenario restores it, so product defaults never
//     leak into the table.
//   - Scratch multi-name rows (link-two-names-both-publish and
//     siblings) currently run on the oracle only (Surfaces 0, which the
//     oracle covers by running the full table unfiltered): the CLI and
//     REST session fakes live outside the owned files, so their mirrors
//     are prod-worker hunks (reported with this change) that flip these
//     rows to SurfaceREST | SurfaceCLI once landed.
//
// Table is the full conformance suite.
var Table = []Scenario{
	{
		Name:     "basic-roundtrip",
		Surfaces: SurfaceAll,
		Run: func(s Surface) error {
			p := "/pc-basic-roundtrip"
			if _, err := s.Open(p, OpenReadOnly, CreateNever); !errors.Is(err, ErrNotFound) {
				return fmt.Errorf("open missing read-only: want ErrNotFound, got %v", err)
			}
			if err := s.CreateFile(p, 0o644, false); err != nil {
				return fmt.Errorf("create: %v", err)
			}
			w, err := s.Open(p, OpenWriteOnly, CreateNever)
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
			r, err := s.Open(p, OpenReadOnly, CreateNever)
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
			h, err := s.Open(p, OpenReadWrite, CreateNever)
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
			h1, err := s.Open(p, OpenReadWrite, CreateNever)
			if err != nil {
				return fmt.Errorf("open h1: %v", err)
			}

			h2, err := s.Open(p, OpenReadWrite, CreateNever)
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
