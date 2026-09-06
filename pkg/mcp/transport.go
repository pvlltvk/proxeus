package mcp

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
)

// handlerTransport is an http.RoundTripper that answers a request by calling an
// http.Handler in this process instead of going over the network. It lets the
// tools use the Prometheus API client against proxeus's own API handler without
// a loopback hop, so the calling context (deadline, cancellation) reaches the
// query engine unchanged.
//
// The handler runs on the caller's goroutine, so a cancellation while it is
// running is seen by the handler itself rather than by RoundTrip.
type handlerTransport struct {
	handler http.Handler
}

func (t handlerTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	// A RoundTripper may not modify the request it is given, and handlers do
	// modify it (ParseForm and friends).
	req = req.Clone(req.Context())
	// Nothing compresses in-process: without this the handler would gzip a
	// body the client is not going to decode.
	req.Header.Del("Accept-Encoding")
	if req.Body == nil {
		// Server requests always have a body; handlers rely on it.
		req.Body = http.NoBody
	}

	// Over HTTP, a panic in the Prometheus handler would be recovered by
	// net/http's own per-connection recover. In-process, the MCP SDK calls
	// tool handlers on a goroutine of its own that nothing recovers panics
	// on, so without this a panic here would crash proxeus instead of just
	// failing the one tool call.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mcp: prometheus handler panicked: %v\n%s", r, debug.Stack())
		}
	}()

	rec := &responseRecorder{header: make(http.Header), code: http.StatusOK}
	t.handler.ServeHTTP(rec, req)

	return &http.Response{
		Status:        fmt.Sprintf("%d %s", rec.code, http.StatusText(rec.code)),
		StatusCode:    rec.code,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        rec.header,
		Body:          io.NopCloser(bytes.NewReader(rec.body.Bytes())),
		ContentLength: int64(rec.body.Len()),
		Request:       req,
	}, nil
}

// responseRecorder buffers what the handler writes.
type responseRecorder struct {
	header      http.Header
	body        bytes.Buffer
	code        int
	wroteHeader bool
}

func (r *responseRecorder) Header() http.Header { return r.header }

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.code = code
	r.wroteHeader = true
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.body.Write(b)
}
