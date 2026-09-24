package github

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FarelRA/storhub/internal/logging"
)

// debugLogClient returns a client whose debug records land in buf, plus
// the buffer itself for assertions.
func debugLogClient(server *httptest.Server, buf *bytes.Buffer) *Client {
	cfg := retryTaxonomyConfig(server, nil)
	cfg.Logger = logging.NewLogger(logging.Options{
		Level:  logging.LevelDebug,
		Format: logging.FormatText,
		Output: buf,
	})
	return NewClient("t", cfg)
}

// TestW8UploadEmitsStartLine pins the logging-contract start line for
// asset uploads: every op emits Debug "<op> start" plus a terminal line.
func TestW8UploadEmitsStartLine(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":11}`))
	}))
	defer server.Close()
	c := debugLogClient(server, &buf)
	if _, err := c.UploadAsset(context.Background(), server.URL+"/upload", "chunk.bin", strings.NewReader("payload"), 7); err != nil {
		t.Fatalf("upload: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "upload asset start") {
		t.Fatalf("missing Debug start line for upload, logs:\n%s", out)
	}
	if !strings.Contains(out, "upload asset complete") {
		t.Fatalf("missing Debug complete line for upload, logs:\n%s", out)
	}
}

// TestW8DownloadEmitsStartLine pins the logging-contract start line for
// asset downloads.
func TestW8DownloadEmitsStartLine(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/p/releases/assets/7", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/cdn/7", http.StatusFound)
	})
	mux.HandleFunc("/cdn/7", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("hello"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	c := debugLogClient(server, &buf)
	body, _, err := c.DownloadAssetStream(context.Background(), "o", "p", 7, 0, 4)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	_, _ = io.ReadAll(body)
	_ = body.Close()
	out := buf.String()
	if !strings.Contains(out, "download asset start") {
		t.Fatalf("missing Debug start line for download, logs:\n%s", out)
	}
	if !strings.Contains(out, "http request start") {
		t.Fatalf("missing Debug start line for the underlying request, logs:\n%s", out)
	}
}

// TestW8RedactEndpointMasksQuery pins the redaction contract the
// retry-exhausted error relies on: query values never reach logs or
// returned errors verbatim.
func TestW8RedactEndpointMasksQuery(t *testing.T) {
	t.Parallel()
	endpoint := "https://api.github.com/repos/o/p/releases?per_page=100&token=secret"
	got := redactEndpoint(endpoint)
	if strings.Contains(got, "secret") || strings.Contains(got, "per_page=100") {
		t.Fatalf("endpoint not redacted: %q", got)
	}
	if !strings.HasPrefix(got, "https://api.github.com/repos/o/p/releases?") {
		t.Fatalf("redacted endpoint lost its path: %q", got)
	}
}
