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
