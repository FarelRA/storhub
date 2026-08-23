package fuse

import (
	"testing"

	public "github.com/FarelRA/storhub/storhub"
)

func TestDefaultOptionsAndNewValidation(t *testing.T) {
	if DefaultOptions().OverlayBufferSize == 0 {
		t.Fatal("expected non-zero fuse defaults")
	}
	hub, err := public.NewStorHub("token")
	if err != nil {
		t.Fatalf("new storhub: %v", err)
	}
	if _, err := New(hub, "bad/name", DefaultOptions()); err == nil {
		t.Fatal("expected invalid project error")
	}
}
