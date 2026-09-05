package rest

import (
	"testing"

	public "github.com/FarelRA/storhub/storhub"
)

func TestDefaultOptionsAndNewValidation(t *testing.T) {
	if DefaultOptions().BasePath == "" {
		t.Fatal("expected non-empty rest defaults")
	}
	hub, err := public.NewStorHub("token")
	if err != nil {
		t.Fatalf("new storhub: %v", err)
	}
	// DefaultOptions serve wide-open by design of this facade test; the
	// explicit opt-in documents intent.
	opts0 := DefaultOptions()
	opts0.AllowAnonymous = true
	handler, err := New(hub, opts0)
	if err != nil {
		t.Fatalf("new rest: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil rest handler")
	}
	hash, err := HashPassword("secret")
	if err != nil || hash == "" {
		t.Fatalf("hash password: %q %v", hash, err)
	}
	opts := DefaultOptions()
	opts.Auth = &AuthOptions{Users: []User{{Username: "u", PasswordHash: hash, UID: 1, PrimaryGID: 1}}, TokenSigningKey: []byte("0123456789abcdef0123456789abcdeg")}
	if _, err := New(hub, opts); err != nil {
		t.Fatalf("new authenticated rest: %v", err)
	}
	if _, err := New(nil, DefaultOptions()); err == nil {
		t.Fatal("expected nil hub error")
	}
}
