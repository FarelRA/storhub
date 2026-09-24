package logging

import (
	"net/http"
	"testing"
)

// fakeWriter captures statuses forwarded by the recorder.
type fakeWriter struct {
	header   http.Header
	statuses []int
}

func (f *fakeWriter) Header() http.Header {
	if f.header == nil {
		f.header = http.Header{}
	}
	return f.header
}

func (f *fakeWriter) Write(p []byte) (int, error) { return len(p), nil }

func (f *fakeWriter) WriteHeader(status int) { f.statuses = append(f.statuses, status) }

// TestHTTPRecorderKeepsFirstStatus pins net/http semantics: only the first
// WriteHeader counts, so a double-WriteHeader handler logs the status the
// client actually saw. The old code overwrote the capture with the second.
func TestHTTPRecorderKeepsFirstStatus(t *testing.T) {
	t.Parallel()
	fake := &fakeWriter{}
	r := NewHTTPRecorder(fake)
	r.WriteHeader(http.StatusNotFound)
	r.WriteHeader(http.StatusInternalServerError)
	if got := r.Status(); got != http.StatusNotFound {
		t.Fatalf("Status() = %d, want %d", got, http.StatusNotFound)
	}
	if len(fake.statuses) != 1 || fake.statuses[0] != http.StatusNotFound {
		t.Fatalf("forwarded statuses = %v, want single %d", fake.statuses, http.StatusNotFound)
	}
	if _, err := r.Write([]byte("body")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := r.Bytes(); got != 4 {
		t.Fatalf("Bytes() = %d, want 4", got)
	}
}
