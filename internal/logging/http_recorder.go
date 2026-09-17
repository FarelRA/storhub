package logging

import "net/http"

// HTTPRecorder is the single shared HTTP status/byte capture wrapper for
// the REST handler and any other HTTP surface. It records the status code
// and bytes written so request logging can report them, while preserving
// streaming semantics via Flush/Unwrap.
//
// Home for the former rest.statusWriter and cli.statusRecorder duplicates:
// one owner for one job.
type HTTPRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

// NewHTTPRecorder wraps w for status/byte capture. The default status is
// 200 until WriteHeader says otherwise, matching net/http's implicit-200
// behavior.
func NewHTTPRecorder(w http.ResponseWriter) *HTTPRecorder {
	return &HTTPRecorder{ResponseWriter: w, status: http.StatusOK}
}

// WriteHeader captures the status code.
func (r *HTTPRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// Write captures byte counts, defaulting an unwritten status to 200.
func (r *HTTPRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += n
	return n, err
}

// Status reports the captured status code (200 when never written).
func (r *HTTPRecorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// Bytes reports the total bytes written through the recorder.
func (r *HTTPRecorder) Bytes() int { return r.bytes }

// Flush forwards flushing to the wrapped writer when it supports it, so
// wrapping never silently disables streaming (SSE, chunked downloads).
func (r *HTTPRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the wrapped writer to http.ResponseController.
func (r *HTTPRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
