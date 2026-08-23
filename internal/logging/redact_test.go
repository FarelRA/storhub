package logging

import "testing"

func TestRedactQueryValuesMasksUnsafeKeys(t *testing.T) {
	got := RedactQueryValues("path=/docs/a.txt&token=secret.jwt&op=append")
	want := "op=append&path=%2Fdocs%2Fa.txt&token=REDACTED"
	if got != want {
		t.Fatalf("unexpected redaction: got %q want %q", got, want)
	}
}

func TestRedactQueryValuesMasksUnparseableQuery(t *testing.T) {
	// A bare % sequence cannot decode; the whole value is untrustworthy.
	if got := RedactQueryValues("token=abc%zz"); got != redactedPlaceholder {
		t.Fatalf("expected wholesale redaction, got %q", got)
	}
	if got := RedactQueryValues(""); got != "" {
		t.Fatalf("empty query must stay empty, got %q", got)
	}
}

func TestRedactSensitivePathMasksShareIdentifiers(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/shares/eyJhbGciOiJIUzI1NiJ9.tok.sig/download", "/shares/REDACTED/download"},
		{"/api/v1/projects/demo/shares/rec-123", "/api/v1/projects/demo/shares/REDACTED"},
		{"/projects/demo/files?path=/a.txt", "/projects/demo/files?path=/a.txt"},
	}
	for _, tc := range cases {
		if got := RedactSensitivePath(tc.in); got != tc.want {
			t.Fatalf("RedactSensitivePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRedactRequestURI(t *testing.T) {
	got := RedactRequestURI("/shares/sig-token-xyz/download?path=/a.txt&sig=zzz")
	want := "/shares/REDACTED/download?path=%2Fa.txt&sig=REDACTED"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := RedactRequestURI("/api/v1/projects/p/files"); got != "/api/v1/projects/p/files" {
		t.Fatalf("plain path changed: %q", got)
	}
}
