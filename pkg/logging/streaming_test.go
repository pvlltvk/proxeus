package logging

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The access-log wrapper must stay transparent to streaming handlers.
// ApacheLogRecord embeds the http.ResponseWriter *interface*, which promotes
// only Header/Write/WriteHeader — so any capability a streaming handler probes
// for has to be reimplemented explicitly. Prometheus' SSE endpoint
// (/api/v1/notifications/live) type-asserts for http.Flusher and 500s without
// it, and httputil.ReverseProxy reaches Flush via http.ResponseController when
// it streams a text/event-stream response.
func TestApacheLogRecordSupportsStreaming(t *testing.T) {
	t.Run("http.Flusher type assertion", func(t *testing.T) {
		rec := &ApacheLogRecord{ResponseWriter: httptest.NewRecorder()}
		if _, ok := interface{}(rec).(http.Flusher); !ok {
			t.Fatal("ApacheLogRecord does not implement http.Flusher; streaming handlers will 500")
		}
	})

	t.Run("http.ResponseController can flush", func(t *testing.T) {
		rec := &ApacheLogRecord{ResponseWriter: httptest.NewRecorder()}
		if err := http.NewResponseController(rec).Flush(); err != nil {
			t.Fatalf("ResponseController.Flush through the wrapper: %v", err)
		}
	})
}

// End-to-end: a handler that streams events through the access-log wrapper must
// reach the client incrementally rather than being buffered until the handler
// returns.
func TestApacheLoggingHandlerStreamsIncrementally(t *testing.T) {
	release := make(chan struct{})
	handler := NewApacheLoggingHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "data: first\n\n")
		flusher.Flush()
		// Block until the test has confirmed the first event arrived, proving
		// it was not merely flushed at handler exit.
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
		flusher.Flush()
	}), func(*ApacheLogRecord) {})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	defer close(release)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	type readResult struct {
		line string
		err  error
	}
	lines := make(chan readResult, 1)
	go func() {
		line, err := bufio.NewReader(resp.Body).ReadString('\n')
		lines <- readResult{line, err}
	}()

	select {
	case got := <-lines:
		if got.err != nil {
			t.Fatalf("reading first event: %v", got.err)
		}
		if strings.TrimSpace(got.line) != "data: first" {
			t.Fatalf("first event = %q, want %q", strings.TrimSpace(got.line), "data: first")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the first event: the wrapper buffered the stream")
	}
}

// A handler that aborts with http.ErrAbortHandler — how httputil.ReverseProxy
// signals a client disconnect, and the normal way a long-lived SSE stream ends
// — must not be converted into a 500 plus a stack trace by the wrapper's
// recover(). Panics that are real bugs must still be captured.
func TestApacheLoggingHandlerPanicHandling(t *testing.T) {
	t.Run("ErrAbortHandler is not logged as a 500", func(t *testing.T) {
		var logged *ApacheLogRecord
		handler := NewApacheLoggingHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic(http.ErrAbortHandler)
		}), func(rec *ApacheLogRecord) { logged = rec })

		// net/http recovers ErrAbortHandler silently at the server level, so
		// call ServeHTTP directly and assert the wrapper re-panicked.
		defer func() {
			rec := recover()
			e, ok := rec.(error)
			if !ok || !errors.Is(e, http.ErrAbortHandler) {
				t.Fatalf("recovered %v, want http.ErrAbortHandler to propagate", rec)
			}
			if logged != nil {
				t.Errorf("an aborted request should not reach the log handlers, got status %d", logged.Status)
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		t.Fatal("ServeHTTP returned, want the ErrAbortHandler panic to propagate")
	})

	t.Run("a real panic still becomes a 500", func(t *testing.T) {
		handler := NewApacheLoggingHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic("boom")
		}), func(*ApacheLogRecord) {})

		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Error running handler") {
			t.Errorf("body = %q, want it to carry the handler error", w.Body.String())
		}
	})
}
