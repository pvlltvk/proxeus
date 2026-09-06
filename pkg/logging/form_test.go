package logging

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
)

// The access log records the form params of a request, which for a POST means
// reading its body — and the handler behind the wrapper (for /api/v1/* the
// embedded Prometheus handler) must still see that body.
func TestApacheLoggingHandlerPreservesBody(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		target      string
		contentType string
		body        string
		wantForm    string
	}{
		{
			name:        "post form",
			method:      http.MethodPost,
			target:      "/api/v1/query",
			contentType: "application/x-www-form-urlencoded",
			body:        "query=1%2B1&time=4",
			wantForm:    "query=1%2B1",
		},
		{
			name:        "post form with charset",
			method:      http.MethodPost,
			target:      "/api/v1/query",
			contentType: "application/x-www-form-urlencoded; charset=UTF-8",
			body:        "query=up",
			wantForm:    "query=up",
		},
		{
			name:     "get query",
			method:   http.MethodGet,
			target:   "/api/v1/query?query=up",
			wantForm: "query=up",
		},
		{
			// A JSON body is none of the access log's business: it must be
			// passed through untouched and not show up in the log line.
			name:        "post json",
			method:      http.MethodPost,
			target:      "/mcp",
			contentType: "application/json",
			body:        `{"query":"up"}`,
			wantForm:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got string
			var record *ApacheLogRecord
			handler := NewApacheLoggingHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("reading body in the handler: %v", err)
				}
				got = string(b)
			}), func(l *ApacheLogRecord) { record = l })

			req := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
			if test.contentType != "" {
				req.Header.Set("Content-Type", test.contentType)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)

			if got != test.body {
				t.Errorf("handler saw body %q, want %q", got, test.body)
			}
			// The order of the params in the log line follows map iteration,
			// so a request with several of them is checked by substring.
			if test.wantForm == "" {
				if record.FormPrefix != "" {
					t.Errorf("logged form = %q, want nothing logged", record.FormPrefix)
				}
			} else if !strings.Contains(record.FormPrefix, test.wantForm) {
				t.Errorf("logged form = %q, want it to contain %q", record.FormPrefix, test.wantForm)
			}
		})
	}
}

// End-to-end through the real thing: a POST proxied to a backend used to fail
// with "ContentLength=18 with Body length 0" (a 502) because the access log had
// already drained the body.
func TestApacheLoggingHandlerProxiesPostForm(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading body in the backend: %v", err)
		}
		_, _ = w.Write(b)
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(NewApacheLoggingHandler(httputil.NewSingleHostReverseProxy(backendURL), func(*ApacheLogRecord) {}))
	defer front.Close()

	resp, err := http.Post(front.URL+"/api/v1/query", "application/x-www-form-urlencoded", strings.NewReader("query=1%2B1&time=4"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", resp.StatusCode, body)
	}
	if string(body) != "query=1%2B1&time=4" {
		t.Errorf("backend echoed %q, want the full form body", body)
	}
}
