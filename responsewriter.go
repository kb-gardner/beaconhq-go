package beaconhq

import (
	"bufio"
	"net"
	"net/http"
)

// StatusRecorder wraps an http.ResponseWriter to capture the status code and
// whether anything was written. It transparently forwards the optional
// http.Hijacker, http.Flusher and io.ReaderFrom interfaces so it is safe to use
// in front of arbitrary handlers (WebSocket upgrades, SSE, etc.).
//
// It is exported so the net/http-based middlewares (e.g. the chi adapter) can
// reuse it without redefining the boilerplate.
type StatusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// NewStatusRecorder wraps w. Until WriteHeader/Write is called, Status() reports
// http.StatusOK (200), matching net/http's implicit default.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// WriteHeader records the status code and forwards it once.
func (r *StatusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

// Write forwards the body, marking an implicit 200 if WriteHeader was not called.
func (r *StatusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.ResponseWriter.Write(b)
}

// Status returns the captured status code (200 if none was written).
func (r *StatusRecorder) Status() int { return r.status }

// Hijack implements http.Hijacker when the underlying writer supports it.
func (r *StatusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := r.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

// Flush implements http.Flusher when the underlying writer supports it.
func (r *StatusRecorder) Flush() {
	if fl, ok := r.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}
