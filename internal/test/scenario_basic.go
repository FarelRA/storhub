package test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var scenarioBasic = []Scenario{
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
}
