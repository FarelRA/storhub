package test

import (
	"testing"
	"time"
)

func TestClampTTL(t *testing.T) {
	if got := ClampTTL(0, 0, 0); got != DefaultTTL {
		t.Fatalf("zero request = %v, want default %v", got, DefaultTTL)
	}
	if got := ClampTTL(-time.Second, time.Minute, time.Hour); got != time.Minute {
		t.Fatalf("negative request = %v, want default", got)
	}
	if got := ClampTTL(2*time.Hour, time.Minute, time.Hour); got != time.Hour {
		t.Fatalf("over-max request = %v, want cap", got)
	}
	if got := ClampTTL(5*time.Minute, time.Minute, time.Hour); got != 5*time.Minute {
		t.Fatalf("in-range request = %v, want passthrough", got)
	}
}

func TestExpired(t *testing.T) {
	now := time.Now()
	if Expired(now.Add(time.Hour), now) {
		t.Fatal("future deadline must not read expired")
	}
	if !Expired(now.Add(-time.Second), now) {
		t.Fatal("past deadline must read expired")
	}
	if !Expired(now, now) {
		t.Fatal("deadline equal to now must read expired (Before is strict)")
	}
}

func TestPendingNamesListReturnsCopy(t *testing.T) {
	var p PendingNames
	if err := p.Add("/a"); err != nil {
		t.Fatalf("add: %v", err)
	}
	got := p.List()
	got[0] = "/mutated"
	if again := p.List(); len(again) != 1 || again[0] != "/a" {
		t.Fatalf("List must return a copy, stage now %q", again)
	}
}

func TestMemScratchCloseHonorsUmask(t *testing.T) {
	m := NewMemSurface()
	m.SetUmask(0o077)
	h, err := m.OpenScratch(0)
	if err != nil {
		t.Fatalf("open scratch: %v", err)
	}
	if _, err := h.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := h.Link("/pc-scratch-umask"); err != nil {
		t.Fatalf("link: %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	st, err := m.Stat("/pc-scratch-umask")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Mode&0o777 != 0o600 {
		t.Fatalf("scratch close must mask 0o644 with the umask, mode = %o", st.Mode)
	}
}
