package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHandlerTransportRoundTrip(t *testing.T) {
	transport := handlerTransport{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, `{"status":"success"}`)
	})}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://proxeus/api/v1/query", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status code = %d, want %d", resp.StatusCode, http.StatusTeapot)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	if string(body) != `{"status":"success"}` {
		t.Errorf("body = %q", body)
	}
	if resp.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d, want %d", resp.ContentLength, len(body))
	}
}

// A handler that writes nothing still answers 200, as it would over HTTP.
func TestHandlerTransportDefaultStatus(t *testing.T) {
	transport := handlerTransport{handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://proxeus/", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code = %d, want 200", resp.StatusCode)
	}
}

// The API client posts its queries as a form, so the handler has to be able to
// parse the request body.
func TestHandlerTransportPostForm(t *testing.T) {
	var got string
	transport := handlerTransport{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		got = r.PostFormValue("query")
	})}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://proxeus/api/v1/query", strings.NewReader("query=up"))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, want 200", resp.StatusCode)
	}
	if got != "up" {
		t.Errorf("query form value = %q, want %q", got, "up")
	}
}

// Nothing decompresses on the way back, so the handler must not be asked to
// compress -- and the caller's request must come back unmodified.
func TestHandlerTransportStripsAcceptEncoding(t *testing.T) {
	var got string
	transport := handlerTransport{handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Accept-Encoding")
	})}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://proxeus/", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip: %v", err)
	}
	defer resp.Body.Close()

	if got != "" {
		t.Errorf("handler saw Accept-Encoding %q, want it stripped", got)
	}
	if want := "gzip"; req.Header.Get("Accept-Encoding") != want {
		t.Errorf("caller's request was modified: Accept-Encoding = %q, want %q", req.Header.Get("Accept-Encoding"), want)
	}
}

func TestHandlerTransportContextCanceled(t *testing.T) {
	called := false
	transport := handlerTransport{handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	})}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://proxeus/", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	if _, err := transport.RoundTrip(req); err == nil {
		t.Error("RoundTrip on a canceled context returned no error")
	}
	if called {
		t.Error("the handler was called on a canceled context")
	}
}
