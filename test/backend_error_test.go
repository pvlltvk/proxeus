package test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"
)

// HTTP-level coverage for R8: a backend rejecting the query is the caller's
// answer and must reach them as the backend wrote it. The unit layer in
// pkg/promclient pins ErrorWrap and the fan-out; this test runs the whole path
// -- response decode, MultiAPI, proxyquerier, the PromQL engine and the v1
// API's error rendering -- to catch the framing being re-added downstream of
// those. ("expanding series: " is the engine's own prefix, not proxeus's.)

// rejectingBackend serves every endpoint with a prometheus error envelope of
// the given type, i.e. a backend that answers but refuses the query. Returns
// the "host:port" to configure as a target.
func rejectingBackend(t *testing.T, errType, msg string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := http.StatusUnprocessableEntity
		if errType == "bad_data" {
			code = http.StatusBadRequest
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status":    "error",
			"errorType": errType,
			"error":     msg,
		}); err != nil {
			t.Errorf("encode error envelope: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

// deadAddr returns a host:port nothing is listening on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// proxyFor stands up a proxeus API server for cfg and returns its base URL.
func proxyFor(t *testing.T, cfg string) string {
	t.Helper()
	srv, addr, stopChan := startAPIForTest(getProxyStorage(cfg))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-stopChan
	})
	return "http://" + addr
}

// apiError issues a request expected to fail and returns the envelope's
// errorType and error message.
func apiError(t *testing.T, url string) (errorType, msg string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	var env struct {
		Status    string `json:"status"`
		ErrorType string `json:"errorType"`
		Error     string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	if env.Status != "error" {
		t.Fatalf("GET %s: status = %q, want error", url, env.Status)
	}
	return env.ErrorType, env.Error
}

// The endpoints that reach a backend through a different code path: /query
// carries the failure on a storage.SeriesSet, the other two return it.
var errorEndpoints = map[string]string{
	"query":  "/api/v1/query?query=up&time=0",
	"labels": "/api/v1/labels?start=0&end=300",
	"series": "/api/v1/series?match%5B%5D=up&start=0&end=300",
}

func TestBackendQueryError_HTTP(t *testing.T) {
	const msg = `invalid label name "a\xc5z"`
	addr := rejectingBackend(t, "execution", msg)
	proxy := proxyFor(t, fmt.Sprintf(rawPSConfig, addr))

	for name, ep := range errorEndpoints {
		t.Run(name, func(t *testing.T) {
			errorType, got := apiError(t, proxy+ep)
			if errorType != "execution" {
				t.Errorf("errorType = %q, want execution", errorType)
			}
			want := "execution: " + msg
			if name == "query" {
				want = "expanding series: " + want
			}
			if got != want {
				t.Errorf("error = %q, want %q", got, want)
			}
			if strings.Contains(got, addr) {
				t.Errorf("error %q leaks the backend address %q", got, addr)
			}
		})
	}
}

// In partial-response mode the rejection is a warning instead of an error, and
// there the backend that disagreed is the whole point of the message -- the
// other one answered the same query fine.
func TestBackendQueryError_PartialResponse_HTTP(t *testing.T) {
	const msg = `invalid label name "a\xc5z"`
	store := promqltest.LoadedStorage(t, `
load 1m
  up{job="x"} 1 1 1 1 1
`)
	defer store.Close()

	healthy, healthyAddr, stopChan := startAPIForTest(store)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = healthy.Shutdown(ctx)
		<-stopChan
	}()

	proxy := proxyFor(t, `
proxeus:
  cross_group_dedup: true
  cross_group_partial_response: true
  server_groups:
    - static_configs:
        - targets:
          - `+healthyAddr+`
      labels:
        az: a
    - static_configs:
        - targets:
          - `+rejectingBackend(t, "execution", msg)+`
      labels:
        az: b
`)

	want := `partial_response: backend[1] rejected the query: execution: ` + msg
	for _, ep := range []string{errorEndpoints["query"], errorEndpoints["labels"]} {
		var env struct {
			Status   string   `json:"status"`
			Warnings []string `json:"warnings"`
		}
		getJSON(t, proxy+ep, &env)
		if env.Status != "success" {
			t.Errorf("GET %s: status = %q, want success", ep, env.Status)
		}
		if len(env.Warnings) != 1 || env.Warnings[0] != want {
			t.Errorf("GET %s: warnings = %v, want [%q]", ep, env.Warnings, want)
		}
	}
}

// The counterpart: a backend that fails to answer keeps its full context,
// because there the target identity is what an operator needs.
func TestUnreachableBackend_HTTP(t *testing.T) {
	addr := deadAddr(t)
	proxy := proxyFor(t, fmt.Sprintf(rawPSConfig, addr))

	for name, ep := range errorEndpoints {
		t.Run(name, func(t *testing.T) {
			_, got := apiError(t, proxy+ep)
			for _, want := range []string{"error in servergroup ord=0", "error in target=http://" + addr, "connection refused"} {
				if !strings.Contains(got, want) {
					t.Errorf("error %q is missing %q", got, want)
				}
			}
		})
	}
}
