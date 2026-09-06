package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	config_util "github.com/prometheus/common/config"
	yaml "gopkg.in/yaml.v2"

	"golang.org/x/crypto/bcrypt"

	"github.com/pvlltvk/proxeus/pkg/logging"
)

// hashFor returns a bcrypt hash usable in a test config. MinCost keeps the
// suite fast; the provider does not care about the cost factor.
func hashFor(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("hashing %q: %v", password, err)
	}
	return string(hash)
}

// configFromYAML unmarshals an `auth` block the same way pkg/config does.
func configFromYAML(t *testing.T, raw string) *Config {
	t.Helper()
	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(raw), cfg); err != nil {
		t.Fatalf("unmarshaling config: %v", err)
	}
	return cfg
}

// identityHandler records the identity the middleware attached to the request.
func identityHandler(got *Identity) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id, ok := FromContext(r.Context()); ok {
			*got = id
		}
		w.WriteHeader(http.StatusOK)
	})
}

// An absent `auth` block must leave the handler chain untouched.
func TestNewWithoutConfig(t *testing.T) {
	a, err := New(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if a != nil {
		t.Fatalf("New(nil) = %v, want a nil Authenticator so the router is not wrapped", a)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		err  string
	}{
		{
			name: "no provider",
			raw:  "exempt_paths: [/-/healthy]",
			err:  "at least one of basic, oidc or trusted_header",
		},
		{
			name: "basic without users",
			raw:  "basic: {}",
			err:  "users must not be empty",
		},
		{
			name: "basic with a plaintext password",
			raw:  "basic:\n  users:\n    alice: hunter2",
			err:  "not a bcrypt hash",
		},
		{
			name: "oidc without issuer",
			raw:  "oidc:\n  client_id: proxeus",
			err:  "issuer_url must be set",
		},
		{
			name: "oidc without client id",
			raw:  "oidc:\n  issuer_url: https://issuer.example",
			err:  "client_id must be set",
		},
		{
			name: "trusted header without user header",
			raw:  "trusted_header:\n  trusted_proxies: [127.0.0.1/32]",
			err:  "user_header must be set",
		},
		{
			name: "trusted header without trusted proxies",
			raw:  "trusted_header:\n  user_header: X-Forwarded-User",
			err:  "trusted_proxies must not be empty",
		},
		{
			name: "trusted header with a bare IP",
			raw:  "trusted_header:\n  user_header: X-Forwarded-User\n  trusted_proxies: [127.0.0.1]",
			err:  "is not a CIDR",
		},
		{
			name: "authorization without an allow-list or routes",
			raw:  "authorization:\n  allowed_users: []",
			err:  "auth.authorization: at least one of allowed_users, allowed_groups or routes",
		},
		{
			name: "route without a path prefix",
			raw:  "authorization:\n  allowed_users: [alice]\n  routes:\n    - allowed_groups: [admins]",
			err:  `path_prefix "" must start with /`,
		},
		{
			name: "route with a relative path prefix",
			raw:  "authorization:\n  routes:\n    - path_prefix: mcp\n      allowed_users: [bot]",
			err:  `path_prefix "mcp" must start with /`,
		},
		{
			name: "route with the root as path prefix",
			raw:  "authorization:\n  routes:\n    - path_prefix: /\n      allowed_groups: [admins]",
			err:  "path_prefix / covers only the root",
		},
		{
			name: "null route",
			raw:  "authorization:\n  routes:\n    - ~",
			err:  `path_prefix "" must start with /`,
		},
		{
			name: "empty allow-list entry",
			raw:  "authorization:\n  allowed_users:\n    -",
			err:  "auth.authorization: allow-list entries must not be empty",
		},
		{
			name: "empty allow-list entry in a route",
			raw:  "authorization:\n  routes:\n    - path_prefix: /mcp\n      allowed_groups: ['']",
			err:  `"/mcp": allow-list entries must not be empty`,
		},
		{
			name: "route without an allow-list",
			raw:  "authorization:\n  allowed_users: [alice]\n  routes:\n    - path_prefix: /mcp",
			err:  `"/mcp": at least one of allowed_users or allowed_groups`,
		},
		{
			name: "authorization without a provider",
			raw:  "authorization:\n  allowed_users: [alice]",
			err:  "at least one of basic, oidc or trusted_header",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := yaml.Unmarshal([]byte(test.raw), &Config{})
			if err == nil {
				t.Fatalf("expected an error containing %q", test.err)
			}
			if !strings.Contains(err.Error(), test.err) {
				t.Fatalf("error = %q, want it to contain %q", err, test.err)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := configFromYAML(t, `
oidc:
  issuer_url: https://issuer.example
  client_id: proxeus
`)

	if cfg.ExemptPaths != nil {
		t.Errorf("ExemptPaths = %v, want nil so New falls back to the caller's defaults", cfg.ExemptPaths)
	}
	if got := cfg.OIDC.UsernameClaim; got != "sub" {
		t.Errorf("UsernameClaim = %q, want %q", got, "sub")
	}
}

func TestMiddleware(t *testing.T) {
	cfg := configFromYAML(t, `
basic:
  users:
    alice: `+hashFor(t, "s3cret")+`
trusted_header:
  user_header: X-Forwarded-User
  groups_header: X-Forwarded-Groups
  trusted_proxies: [192.0.2.1/32]
`)
	a, err := New(context.Background(), cfg, []string{"/-/healthy"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name       string
		path       string
		remoteAddr string
		header     http.Header
		basic      [2]string
		status     int
		identity   Identity
	}{
		{
			name:   "exempt path needs no credentials",
			path:   "/-/healthy",
			status: http.StatusOK,
		},
		{
			name:   "no credentials",
			path:   "/api/v1/query",
			status: http.StatusUnauthorized,
		},
		{
			name:     "valid basic credentials",
			path:     "/api/v1/query",
			basic:    [2]string{"alice", "s3cret"},
			status:   http.StatusOK,
			identity: Identity{Name: "alice", Provider: "basic"},
		},
		{
			name:   "wrong basic password",
			path:   "/api/v1/query",
			basic:  [2]string{"alice", "hunter2"},
			status: http.StatusUnauthorized,
		},
		{
			name:   "unknown basic user",
			path:   "/api/v1/query",
			basic:  [2]string{"mallory", "s3cret"},
			status: http.StatusUnauthorized,
		},
		{
			name:       "trusted header from a trusted proxy",
			path:       "/api/v1/query",
			remoteAddr: "192.0.2.1:4321",
			header:     http.Header{"X-Forwarded-User": {"bob"}, "X-Forwarded-Groups": {"admins, viewers"}},
			status:     http.StatusOK,
			identity:   Identity{Name: "bob", Groups: []string{"admins", "viewers"}, Provider: "trusted_header"},
		},
		{
			name:       "trusted header from anywhere else is not an identity",
			path:       "/api/v1/query",
			remoteAddr: "198.51.100.7:4321",
			header:     http.Header{"X-Forwarded-User": {"bob"}},
			status:     http.StatusUnauthorized,
		},
		{
			name:       "an untrusted header does not shadow valid basic credentials",
			path:       "/api/v1/query",
			remoteAddr: "198.51.100.7:4321",
			header:     http.Header{"X-Forwarded-User": {"bob"}},
			basic:      [2]string{"alice", "s3cret"},
			status:     http.StatusOK,
			identity:   Identity{Name: "alice", Provider: "basic"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var got Identity
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.remoteAddr != "" {
				req.RemoteAddr = test.remoteAddr
			}
			for k, v := range test.header {
				req.Header[k] = v
			}
			if test.basic[0] != "" {
				req.SetBasicAuth(test.basic[0], test.basic[1])
			}

			w := httptest.NewRecorder()
			a.Middleware(identityHandler(&got)).ServeHTTP(w, req)

			if w.Code != test.status {
				t.Fatalf("status = %d, want %d", w.Code, test.status)
			}
			if got.Name != test.identity.Name || got.Provider != test.identity.Provider {
				t.Errorf("identity = %+v, want %+v", got, test.identity)
			}
			if strings.Join(got.Groups, ",") != strings.Join(test.identity.Groups, ",") {
				t.Errorf("groups = %v, want %v", got.Groups, test.identity.Groups)
			}
			if test.status == http.StatusUnauthorized {
				if want := `Basic realm="proxeus"`; w.Header().Get("WWW-Authenticate") != want {
					t.Errorf("WWW-Authenticate = %q, want %q", w.Header().Get("WWW-Authenticate"), want)
				}
			}
		})
	}
}

// Exempt paths are taken verbatim -- the caller has already resolved them
// against --web.route-prefix -- and match on whole segments, so neither a
// longer name nor a traversal that lands elsewhere slips through.
func TestMiddlewareExemptPaths(t *testing.T) {
	basic := "basic:\n  users:\n    alice: " + hashFor(t, "s3cret")

	tests := []struct {
		name     string
		raw      string
		defaults []string
		path     string
		status   int
	}{
		{
			name:     "default exempt path",
			raw:      basic,
			defaults: []string{"/proxeus/-/ready", "/metrics"},
			path:     "/proxeus/-/ready",
			status:   http.StatusOK,
		},
		{
			name:     "below a default exempt path",
			raw:      basic,
			defaults: []string{"/metrics"},
			path:     "/metrics/foo",
			status:   http.StatusOK,
		},
		{
			name:     "a longer path is not the exempt one",
			raw:      basic,
			defaults: []string{"/metrics"},
			path:     "/metrics2",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "traversal out of an exempt path",
			raw:      basic,
			defaults: []string{"/metrics"},
			path:     "/metrics/../api/v1/query",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "configured paths replace the defaults",
			raw:      basic + "\nexempt_paths: [/healthz]",
			defaults: []string{"/metrics"},
			path:     "/metrics",
			status:   http.StatusUnauthorized,
		},
		{
			name:     "configured path",
			raw:      basic + "\nexempt_paths: [/healthz]",
			defaults: []string{"/metrics"},
			path:     "/healthz",
			status:   http.StatusOK,
		},
		{
			name:     "an empty list exempts nothing",
			raw:      basic + "\nexempt_paths: []",
			defaults: []string{"/metrics"},
			path:     "/metrics",
			status:   http.StatusUnauthorized,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, err := New(context.Background(), configFromYAML(t, test.raw), test.defaults)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			w := httptest.NewRecorder()
			// httptest.NewRequest parses the target, so build the URL by hand
			// to keep traversals intact the way a client would send them.
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL.Path = test.path
			a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, req)

			if w.Code != test.status {
				t.Fatalf("GET %s: status = %d, want %d", test.path, w.Code, test.status)
			}
		})
	}
}

// Authorization runs on the identity authentication produced: the top-level
// allow-lists decide every request, and the first matching route rule has to
// allow it too.
func TestMiddlewareAuthorization(t *testing.T) {
	providers := "basic:\n  users:\n    alice: " + hashFor(t, "s3cret") + `
trusted_header:
  user_header: X-Forwarded-User
  groups_header: X-Forwarded-Groups
  trusted_proxies: [192.0.2.1/32]
`
	const (
		byUser = `
authorization:
  allowed_users: [alice]
`
		byGroup = `
authorization:
  allowed_groups: [admins]
`
		withRoute = `
authorization:
  allowed_users: [alice]
  allowed_groups: [admins]
  routes:
    - path_prefix: /mcp
      allowed_groups: [mcp-users]
`
		routesOnly = `
authorization:
  routes:
    - path_prefix: /mcp
      allowed_users: [bob]
`
	)

	tests := []struct {
		name   string
		authz  string
		path   string
		basic  [2]string
		user   string
		groups string
		status int
	}{
		{
			name:   "no authorization block allows every identity",
			path:   "/api/v1/query",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusOK,
		},
		{
			name:   "routes only: anyone authenticated outside the rule",
			authz:  routesOnly,
			path:   "/api/v1/query",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusOK,
		},
		{
			name:   "routes only: the rule still restricts",
			authz:  routesOnly,
			path:   "/mcp",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusForbidden,
		},
		{
			name:   "routes only: the named user passes the rule",
			authz:  routesOnly,
			path:   "/mcp",
			user:   "bob",
			status: http.StatusOK,
		},
		{
			name:   "allowed by name",
			authz:  byUser,
			path:   "/api/v1/query",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusOK,
		},
		{
			name:   "an authenticated user not on the list",
			authz:  byUser,
			path:   "/api/v1/query",
			status: http.StatusForbidden,
		},
		{
			name:   "allowed by group",
			authz:  byGroup,
			path:   "/api/v1/query",
			groups: "admins, viewers",
			status: http.StatusOK,
		},
		{
			name:   "none of the groups is on the list",
			authz:  byGroup,
			path:   "/api/v1/query",
			groups: "viewers",
			status: http.StatusForbidden,
		},
		{
			name:   "basic auth carries no groups",
			authz:  byGroup,
			path:   "/api/v1/query",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusForbidden,
		},
		{
			name:   "a route rule tightens the top-level policy",
			authz:  withRoute,
			path:   "/mcp",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusForbidden,
		},
		{
			name:   "a route rule covers what is below its prefix",
			authz:  withRoute,
			path:   "/mcp/messages",
			groups: "admins, mcp-users",
			status: http.StatusOK,
		},
		{
			name:   "a route rule does not cover a longer name",
			authz:  withRoute,
			path:   "/mcpx",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusOK,
		},
		{
			name:   "a route rule does not widen the top-level policy",
			authz:  withRoute,
			path:   "/mcp",
			groups: "mcp-users",
			status: http.StatusForbidden,
		},
		{
			name:   "paths no rule covers need only the top-level policy",
			authz:  withRoute,
			path:   "/api/v1/query",
			groups: "admins",
			status: http.StatusOK,
		},
		{
			name:   "an exempt path stays exempt",
			authz:  withRoute + "exempt_paths: [/-/healthy]\n",
			path:   "/-/healthy",
			status: http.StatusOK,
		},
		{
			name:   "user names are matched case-sensitively",
			authz:  byUser,
			path:   "/api/v1/query",
			user:   "Alice",
			status: http.StatusForbidden,
		},
		{
			name:   "group names are matched case-sensitively",
			authz:  byGroup,
			path:   "/api/v1/query",
			groups: "Admins",
			status: http.StatusForbidden,
		},
		{
			name:   "duplicate and empty group entries do not change the outcome",
			authz:  byGroup,
			path:   "/api/v1/query",
			groups: "viewers, , admins, admins",
			status: http.StatusOK,
		},
		{
			name:   "a trailing slash still matches the route prefix",
			authz:  withRoute,
			path:   "/mcp/",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusForbidden,
		},
		{
			name:   "a traversal cannot reach a route-guarded path without its policy",
			authz:  withRoute,
			path:   "/api/../mcp/messages",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusForbidden,
		},
		{
			name:   "a traversal out of a route-guarded path only needs the top-level policy",
			authz:  withRoute,
			path:   "/mcp/../api/v1/query",
			basic:  [2]string{"alice", "s3cret"},
			status: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a, err := New(context.Background(), configFromYAML(t, providers+test.authz), nil)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.basic[0] != "" {
				req.SetBasicAuth(test.basic[0], test.basic[1])
			} else {
				user := test.user
				if user == "" {
					user = "bob"
				}
				req.RemoteAddr = "192.0.2.1:4321"
				req.Header.Set("X-Forwarded-User", user)
				req.Header.Set("X-Forwarded-Groups", test.groups)
			}

			w := httptest.NewRecorder()
			a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, req)

			if w.Code != test.status {
				t.Fatalf("GET %s: status = %d, want %d", test.path, w.Code, test.status)
			}
			if test.status != http.StatusForbidden {
				return
			}
			if got := w.Body.String(); got != http.StatusText(http.StatusForbidden)+"\n" {
				t.Errorf("body = %q, want %q", got, http.StatusText(http.StatusForbidden)+"\n")
			}
			// The credentials were fine, so there is nothing to challenge for.
			if got := w.Header().Get("WWW-Authenticate"); got != "" {
				t.Errorf("WWW-Authenticate = %q, want none", got)
			}
		})
	}
}

// Every route rule covering a request applies, whatever the order: a broader
// rule listed first does not shadow a stricter one underneath it.
func TestMiddlewareAuthorizationAllMatchingRoutesApply(t *testing.T) {
	cfg := configFromYAML(t, `
trusted_header:
  user_header: X-Forwarded-User
  groups_header: X-Forwarded-Groups
  trusted_proxies: [192.0.2.1/32]
authorization:
  allowed_users: [alice]
  allowed_groups: [api-users, admins]
  routes:
    - path_prefix: /api
      allowed_groups: [api-users]
    - path_prefix: /api/v1/admin
      allowed_groups: [admins]
`)
	a, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name   string
		groups string
		status int
	}{
		{
			name:   "in the broader group only",
			groups: "api-users",
			status: http.StatusForbidden,
		},
		{
			name:   "in the stricter group only",
			groups: "admins",
			status: http.StatusForbidden,
		},
		{
			name:   "in both",
			groups: "api-users,admins",
			status: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/admin", nil)
			req.RemoteAddr = "192.0.2.1:4321"
			req.Header.Set("X-Forwarded-User", "bob")
			req.Header.Set("X-Forwarded-Groups", test.groups)

			w := httptest.NewRecorder()
			a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, req)

			if w.Code != test.status {
				t.Fatalf("status = %d, want %d", w.Code, test.status)
			}
		})
	}
}

// Concurrent requests must not interfere with each other: the policy is built
// once in New and never mutated afterwards.
func TestMiddlewareAuthorizationConcurrent(t *testing.T) {
	cfg := configFromYAML(t, `
basic:
  users:
    alice: `+hashFor(t, "s3cret")+`
    mallory: `+hashFor(t, "hunter2")+`
authorization:
  allowed_users: [alice]
`)
	a, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Unlike identityHandler, this does not write through a shared pointer, so
	// concurrent requests cannot race on it -- only the middleware itself is
	// under test here.
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
			req.SetBasicAuth("alice", "s3cret")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("allowed user: status = %d, want %d", w.Code, http.StatusOK)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
			req.SetBasicAuth("mallory", "hunter2")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden {
				t.Errorf("denied user: status = %d, want %d", w.Code, http.StatusForbidden)
			}
		}()
	}
	wg.Wait()
}

// Browsers do not send Authorization on a CORS preflight, so it must not be
// answered with a 401.
func TestMiddlewarePassesCORSPreflight(t *testing.T) {
	cfg := configFromYAML(t, "basic:\n  users:\n    alice: "+hashFor(t, "s3cret"))
	a, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for _, test := range []struct {
		name   string
		method string
		header string
		status int
	}{
		{name: "preflight", method: http.MethodOptions, header: "GET", status: http.StatusOK},
		{name: "plain OPTIONS still needs credentials", method: http.MethodOptions, status: http.StatusUnauthorized},
		{name: "the header alone does not exempt a GET", method: http.MethodGet, header: "GET", status: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(test.method, "/api/v1/query", nil)
			if test.header != "" {
				req.Header.Set("Access-Control-Request-Method", test.header)
			}
			w := httptest.NewRecorder()
			a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, req)
			if w.Code != test.status {
				t.Fatalf("status = %d, want %d", w.Code, test.status)
			}
		})
	}
}

// With only trusted_header configured there is no auth scheme to advertise, so
// the 401 deliberately carries no WWW-Authenticate.
func TestMiddlewareTrustedHeaderOnlyHasNoChallenge(t *testing.T) {
	cfg := configFromYAML(t, "trusted_header:\n  user_header: X-Forwarded-User\n  trusted_proxies: [192.0.2.1/32]")
	a, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	w := httptest.NewRecorder()
	a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/query", nil))

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if got := w.Header().Get("WWW-Authenticate"); got != "" {
		t.Fatalf("WWW-Authenticate = %q, want none", got)
	}
}

// The access log's user field is filled in by the middleware, so a request can
// be traced back to whoever made it.
func TestMiddlewareRecordsUserInAccessLog(t *testing.T) {
	cfg := configFromYAML(t, "basic:\n  users:\n    alice: "+hashFor(t, "s3cret"))
	a, err := New(context.Background(), cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	req.SetBasicAuth("alice", "s3cret")
	record := &logging.ApacheLogRecord{ResponseWriter: httptest.NewRecorder()}
	a.Middleware(identityHandler(&Identity{})).ServeHTTP(record, req)

	if record.User != "alice" {
		t.Fatalf("access log user = %q, want %q", record.User, "alice")
	}
}

// Repeated requests with the same credentials must not re-run bcrypt.
func TestBasicProviderCachesVerifications(t *testing.T) {
	p := newBasicProvider(&BasicConfig{Users: map[string]config_util.Secret{
		"alice": config_util.Secret(hashFor(t, "s3cret")),
	}})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
	req.SetBasicAuth("alice", "s3cret")
	for i := 0; i < 2; i++ {
		if _, err := p.authenticate(req); err != nil {
			t.Fatalf("authenticate: %v", err)
		}
	}
	if len(p.verified) != 1 {
		t.Fatalf("cached %d verifications, want 1", len(p.verified))
	}

	// A wrong password for a cached user must still be rejected.
	req.SetBasicAuth("alice", "hunter2")
	if _, err := p.authenticate(req); err == nil {
		t.Fatal("authenticate with a wrong password succeeded")
	}
	if len(p.verified) != 1 {
		t.Fatalf("cached %d verifications after a wrong password, want 1", len(p.verified))
	}
}

// Unknown users are checked against a fixed hash for constant cost, but caching
// that would let anyone grow the map with names they made up.
func TestBasicProviderDoesNotCacheUnknownUsers(t *testing.T) {
	p := newBasicProvider(&BasicConfig{Users: map[string]config_util.Secret{
		"alice": config_util.Secret(hashFor(t, "s3cret")),
	}})

	for _, password := range []string{"s3cret", "fakepassword"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
		req.SetBasicAuth("mallory", password)
		if _, err := p.authenticate(req); err == nil {
			t.Fatalf("authenticate as an unknown user with %q succeeded", password)
		}
	}
	if len(p.verified) != 0 {
		t.Fatalf("cached %d verifications for an unknown user, want 0", len(p.verified))
	}
}
