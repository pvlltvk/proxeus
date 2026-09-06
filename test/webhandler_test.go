package test

import (
	"bufio"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/notifications"
	"github.com/prometheus/prometheus/web"

	"github.com/pvlltvk/proxeus/pkg/logging"
)

// proxeus mounts the embedded Prometheus web handler directly in its router
// (cmd/proxeus/main.go) instead of reverse-proxying to it over a loopback
// listener. These tests pin what that mount has to keep doing: serve the
// standard API routes for GET and for the POST form bodies Grafana sends, move
// with --web.route-prefix, and keep streaming the notifications SSE endpoint
// through the access-log wrapper it sits behind.

const webHandlerData = `
load 1m
  up{job="x"} 1 1 1 1 1
`

// newPromHandler builds the embedded Prometheus handler the way main.go does
// and fronts it with the access-log wrapper, returning the server it runs on.
func newPromHandler(t *testing.T, routePrefix string, notifs *notifications.Notifications) *httptest.Server {
	t.Helper()

	store := promqltest.LoadedStorage(t, webHandlerData)
	t.Cleanup(func() { store.Close() })

	// A fresh registry per handler: these are built several times per run and
	// the API metrics would collide on a shared one.
	reg := prometheus.NewRegistry()
	externalURL, err := url.Parse("http://localhost:9090" + strings.TrimSuffix(routePrefix, "/"))
	if err != nil {
		t.Fatalf("parsing external URL: %v", err)
	}

	webHandler := web.New(slog.New(slog.DiscardHandler), &web.Options{
		Registerer:          reg,
		Gatherer:            reg,
		Context:             t.Context(),
		Storage:             store,
		ExemplarStorage:     store.ExemplarQueryable(),
		QueryEngine:         testAPIEngine(),
		ListenAddresses:     []string{"127.0.0.1:9090"},
		RoutePrefix:         routePrefix,
		ExternalURL:         externalURL,
		Version:             &web.PrometheusVersion{Version: "test"},
		NotificationsGetter: notifs.Get,
		NotificationsSub:    notifs.Sub,
	})
	webHandler.SetReady(web.Ready)

	srv := httptest.NewServer(logging.NewApacheLoggingHandler(webHandler.HTTPHandler()))
	t.Cleanup(srv.Close)
	return srv
}

func TestPromHandlerRoutes(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		params   url.Values
		wantBody string
		getOnly  bool
	}{
		{
			name:     "instant query",
			path:     "/api/v1/query",
			params:   url.Values{"query": {"up"}, "time": {"60"}},
			wantBody: `"__name__":"up"`,
		},
		{
			name:     "range query",
			path:     "/api/v1/query_range",
			params:   url.Values{"query": {"up"}, "start": {"0"}, "end": {"120"}, "step": {"60"}},
			wantBody: `"resultType":"matrix"`,
		},
		{
			name:     "labels",
			path:     "/api/v1/labels",
			params:   url.Values{"start": {"0"}, "end": {"300"}},
			wantBody: `"job"`,
		},
		{
			name:     "buildinfo",
			path:     "/api/v1/status/buildinfo",
			wantBody: `"version":"test"`,
			getOnly:  true,
		},
		{
			name:     "healthy",
			path:     "/-/healthy",
			wantBody: "is Healthy.",
			getOnly:  true,
		},
		{
			name:     "ready",
			path:     "/-/ready",
			wantBody: "is Ready.",
			getOnly:  true,
		},
	}

	for _, routePrefix := range []string{"/", "/foo"} {
		t.Run("route prefix "+routePrefix, func(t *testing.T) {
			srv := newPromHandler(t, routePrefix, notifications.NewNotifications(16, nil))

			for _, c := range cases {
				// Grafana posts its queries as a form; a GET with the same
				// parameters in the query string must answer identically.
				methods := []string{http.MethodGet, http.MethodPost}
				if c.getOnly {
					methods = methods[:1]
				}
				for _, method := range methods {
					t.Run(c.name+" "+method, func(t *testing.T) {
						target := srv.URL + strings.TrimSuffix(routePrefix, "/") + c.path

						var (
							resp *http.Response
							err  error
						)
						if method == http.MethodGet {
							resp, err = http.Get(target + "?" + c.params.Encode())
						} else {
							resp, err = http.PostForm(target, c.params)
						}
						if err != nil {
							t.Fatalf("%s %s: %v", method, c.path, err)
						}
						defer resp.Body.Close()

						body, err := io.ReadAll(resp.Body)
						if err != nil {
							t.Fatalf("reading the body: %v", err)
						}
						if resp.StatusCode != http.StatusOK {
							t.Fatalf("status = %d, want 200 (%s)", resp.StatusCode, body)
						}
						if !strings.Contains(string(body), c.wantBody) {
							t.Errorf("body = %q, want it to contain %q", body, c.wantBody)
						}
					})
				}
			}
		})
	}
}

// The API is only reachable under the route prefix: a request that skips it
// must not be served.
func TestPromHandlerRoutePrefixIsEnforced(t *testing.T) {
	srv := newPromHandler(t, "/foo", notifications.NewNotifications(16, nil))

	resp, err := http.Get(srv.URL + "/api/v1/query?query=up")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Errorf("status = 200, want the unprefixed API path not to be served")
	}
}

// The notifications SSE endpoint type-asserts its ResponseWriter for
// http.Flusher and 500s without it. Mounted directly, it now writes through
// proxeus's access-log wrapper rather than through a reverse proxy.
func TestPromHandlerNotificationsSSE(t *testing.T) {
	notifs := notifications.NewNotifications(16, nil)
	srv := newPromHandler(t, "/", notifs)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/api/v1/notifications/live", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}

	notifs.AddNotification(notifications.ConfigurationUnsuccessful)

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
			t.Fatalf("reading the first event: %v", got.err)
		}
		if !strings.Contains(got.line, notifications.ConfigurationUnsuccessful) {
			t.Errorf("first event = %q, want it to carry %q", got.line, notifications.ConfigurationUnsuccessful)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the notification: the stream is buffered")
	}
}
