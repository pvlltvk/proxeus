package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/julienschmidt/httprouter"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/util/notifications"
	"golang.org/x/crypto/bcrypt"

	"github.com/pvlltvk/proxeus/pkg/auth"
)

// hasNotification reports whether an active notification with the given text
// is currently published.
func hasNotification(n *notifications.Notifications, text string) bool {
	for _, notif := range n.Get() {
		if notif.Text == text && notif.Active {
			return true
		}
	}
	return false
}

// A failed reload must surface in the UI's notification feed, and a later
// successful reload must clear it again. This also pins that reloadConfig
// never sees a nil *Notifications — the nil getter/subscriber is what made
// /api/v1/notifications panic before the handles were wired up.
func TestReloadConfigNotifications(t *testing.T) {
	notifs := notifications.NewNotifications(16, nil)
	noStepSubqueryInterval := &safePromQLNoStepSubqueryInterval{}

	origConfigFile := opts.ConfigFile
	t.Cleanup(func() { opts.ConfigFile = origConfigFile })

	opts.ConfigFile = filepath.Join(t.TempDir(), "does-not-exist.yaml")
	if err := reloadConfig(noStepSubqueryInterval, notifs); err == nil {
		t.Fatal("reloadConfig succeeded for a missing config file, want error")
	}
	if !hasNotification(notifs, notifications.ConfigurationUnsuccessful) {
		t.Errorf("after a failed reload, %q notification is missing", notifications.ConfigurationUnsuccessful)
	}

	opts.ConfigFile = "local_test.conf"
	if err := reloadConfig(noStepSubqueryInterval, notifs); err != nil {
		t.Fatalf("reloadConfig(%q): %v", opts.ConfigFile, err)
	}
	if hasNotification(notifs, notifications.ConfigurationUnsuccessful) {
		t.Errorf("after a successful reload, %q notification was not cleared", notifications.ConfigurationUnsuccessful)
	}
}

func TestIsReactRoute(t *testing.T) {
	cases := []struct {
		name        string
		routePrefix string
		urlPath     string
		want        bool
	}{
		// Root prefix (normalized to "/"): paths arrive unprefixed.
		{name: "root query", routePrefix: "/", urlPath: "/query", want: true},
		{name: "root backends", routePrefix: "/", urlPath: "/backends", want: true},
		{name: "root unknown", routePrefix: "/", urlPath: "/proxeus/backends", want: false},
		{name: "empty prefix query", routePrefix: "", urlPath: "/query", want: true},

		// Sub-path prefix: paths arrive prefixed with "/foo".
		{name: "foo query", routePrefix: "/foo", urlPath: "/foo/query", want: true},
		{name: "foo backends", routePrefix: "/foo", urlPath: "/foo/backends", want: true},
		// Unprefixed path under a sub-path prefix must NOT match.
		{name: "foo unprefixed query", routePrefix: "/foo", urlPath: "/query", want: false},
		// Non-react path under prefix.
		{name: "foo proxeus", routePrefix: "/foo", urlPath: "/foo/proxeus/backends", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isReactRoute(tc.routePrefix, tc.urlPath); got != tc.want {
				t.Errorf("isReactRoute(%q, %q) = %v, want %v", tc.routePrefix, tc.urlPath, got, tc.want)
			}
		})
	}
}

func TestOverrideRoutesAuthAndMethods(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("s3cret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := auth.New(context.Background(), &auth.Config{
		Basic: &auth.BasicConfig{Users: map[string]config_util.Secret{"alice": config_util.Secret(hash)}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var overrideCalls, uiCalls, upstreamCalls int
	count := func(n *int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) { *n++ }
	}
	paths := []string{
		"/api/v1/status/config",
		"/api/v1/metadata",
		"/api/v1/status/walreplay",
		"/api/v1/status/flags",
	}
	overrides := map[string]http.HandlerFunc{}
	for _, p := range paths {
		overrides[p] = count(&overrideCalls)
	}

	router := httprouter.New()
	registerOverrides(router, count(&upstreamCalls), overrides)
	router.NotFound = upstreamOptions(count(&uiCalls), count(&upstreamCalls))
	handler := authenticator.Middleware(router)

	tests := []struct {
		name         string
		method       string
		preflight    bool
		authed       bool
		status       int
		wantOverride int
		wantUpstream int
	}{
		{name: "GET without credentials", method: http.MethodGet, status: http.StatusUnauthorized},
		{name: "GET with credentials", method: http.MethodGet, authed: true, status: http.StatusOK, wantOverride: 1},
		{name: "preflight without credentials", method: http.MethodOptions, preflight: true, status: http.StatusOK, wantUpstream: 1},
		{name: "plain OPTIONS without credentials", method: http.MethodOptions, status: http.StatusUnauthorized},
		{name: "POST without credentials", method: http.MethodPost, status: http.StatusUnauthorized},
		{name: "POST with credentials", method: http.MethodPost, authed: true, status: http.StatusMethodNotAllowed},
		{name: "DELETE with credentials", method: http.MethodDelete, authed: true, status: http.StatusMethodNotAllowed},
	}
	for _, p := range paths {
		for _, tt := range tests {
			t.Run(tt.method+" "+p+" "+tt.name, func(t *testing.T) {
				overrideCalls, uiCalls, upstreamCalls = 0, 0, 0
				req := httptest.NewRequest(tt.method, p, nil)
				if tt.preflight {
					req.Header.Set("Access-Control-Request-Method", http.MethodGet)
				}
				if tt.authed {
					req.SetBasicAuth("alice", "s3cret")
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)

				if rec.Code != tt.status {
					t.Errorf("status = %d, want %d", rec.Code, tt.status)
				}
				if overrideCalls != tt.wantOverride || upstreamCalls != tt.wantUpstream || uiCalls != 0 {
					t.Errorf("calls override/upstream/ui = %d/%d/%d, want %d/%d/0",
						overrideCalls, upstreamCalls, uiCalls, tt.wantOverride, tt.wantUpstream)
				}
			})
		}
	}

	t.Run("preflight on an unrouted path never reaches the fallback", func(t *testing.T) {
		overrideCalls, uiCalls, upstreamCalls = 0, 0, 0
		req := httptest.NewRequest(http.MethodOptions, "/proxeus/api/inventory", nil)
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		handler.ServeHTTP(httptest.NewRecorder(), req)
		if uiCalls != 0 || upstreamCalls != 1 {
			t.Errorf("ui/upstream calls = %d/%d, want 0/1", uiCalls, upstreamCalls)
		}
	})
}

func TestNoRemoteRead(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("s3cret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := auth.New(context.Background(), &auth.Config{
		Basic: &auth.BasicConfig{Users: map[string]config_util.Secret{"alice": config_util.Secret(hash)}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var upstreamCalls int
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { upstreamCalls++ })
	router := httprouter.New()
	registerNoRemoteRead(router, "/prefix/api/v1/read")
	router.NotFound = upstreamOptions(upstream, upstream)
	handler := authenticator.Middleware(router)

	tests := []struct {
		name   string
		method string
		authed bool
		status int
	}{
		{name: "POST", method: http.MethodPost, authed: true, status: http.StatusNotImplemented},
		{name: "GET", method: http.MethodGet, authed: true, status: http.StatusNotImplemented},
		{name: "POST without credentials", method: http.MethodPost, status: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstreamCalls = 0
			req := httptest.NewRequest(tt.method, "/prefix/api/v1/read", nil)
			if tt.authed {
				req.SetBasicAuth("alice", "s3cret")
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Errorf("status = %d, want %d", rec.Code, tt.status)
			}
			if upstreamCalls != 0 {
				t.Errorf("upstream called %d times, want 0", upstreamCalls)
			}
		})
	}
}
