package logging

import "testing"

func TestRedactQueryValuesMasksUnsafeKeys(t *testing.T) {
	t.Parallel()
	got := RedactQueryValues("path=/docs/a.txt&token=secret.jwt&op=append")
	want := "op=append&path=%2Fdocs%2Fa.txt&token=REDACTED"
	if got != want {
		t.Fatalf("unexpected redaction: got %q want %q", got, want)
	}
}

func TestRedactQueryValuesMasksUnparseableQuery(t *testing.T) {
	t.Parallel()
	// A bare % sequence cannot decode; the whole value is untrustworthy.
	if got := RedactQueryValues("token=abc%zz"); got != redactedPlaceholder {
		t.Fatalf("expected wholesale redaction, got %q", got)
	}
	if got := RedactQueryValues(""); got != "" {
		t.Fatalf("empty query must stay empty, got %q", got)
	}
}

func TestRedactSensitivePathMasksShareIdentifiers(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	got := RedactRequestURI("/shares/sig-token-xyz/download?path=/a.txt&sig=zzz")
	want := "/shares/REDACTED/download?path=%2Fa.txt&sig=REDACTED"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := RedactRequestURI("/api/v1/projects/p/files"); got != "/api/v1/projects/p/files" {
		t.Fatalf("plain path changed: %q", got)
	}
}

func TestRedactEndpointComposesPathAndQuery(t *testing.T) {
	t.Parallel()
	got := RedactEndpoint("/repos/o/r/contents/a.txt?ref=main&token=secret")
	want := "/repos/o/r/contents/a.txt?ref=REDACTED&token=REDACTED"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := RedactEndpoint("/repos/o/r/contents/a.txt"); got != "/repos/o/r/contents/a.txt" {
		t.Fatalf("queryless endpoint changed: %q", got)
	}
	got = RedactEndpoint("/shares/sigcap/download?path=/a.txt&sig=zzz")
	want = "/shares/REDACTED/download?path=%2Fa.txt&sig=REDACTED"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRedactSignedURLStripsQuery(t *testing.T) {
	t.Parallel()
	got := RedactSignedURL("https://cdn.example/r/1/a.bin?sig=zzz&exp=99")
	want := "https://cdn.example/r/1/a.bin"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if got := RedactSignedURL("https://cdn.example/r/1/a.bin"); got != "https://cdn.example/r/1/a.bin" {
		t.Fatalf("queryless url changed: %q", got)
	}
	if got := RedactSignedURL("http://x/%zz"); got != redactedPlaceholder {
		t.Fatalf("unparseable url must redact wholesale, got %q", got)
	}
}
