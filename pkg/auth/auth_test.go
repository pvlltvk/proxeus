package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	a, err := New(context.Background(), nil, "/")
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

	if got, want := strings.Join(cfg.ExemptPaths, ","), "/-/healthy,/-/ready,/metrics"; got != want {
		t.Errorf("ExemptPaths = %q, want %q", got, want)
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
	a, err := New(context.Background(), cfg, "/")
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

// Exempt paths are prefixes below --web.route-prefix, like every other route
// proxeus registers.
func TestMiddlewareExemptPathsUseRoutePrefix(t *testing.T) {
	cfg := configFromYAML(t, "basic:\n  users:\n    alice: "+hashFor(t, "s3cret"))
	a, err := New(context.Background(), cfg, "/proxeus")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for path, want := range map[string]int{
		"/proxeus/-/ready": http.StatusOK,
		"/-/ready":         http.StatusUnauthorized,
	} {
		w := httptest.NewRecorder()
		a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Errorf("GET %s: status = %d, want %d", path, w.Code, want)
		}
	}
}

// The access log's user field is filled in by the middleware, so a request can
// be traced back to whoever made it.
func TestMiddlewareRecordsUserInAccessLog(t *testing.T) {
	cfg := configFromYAML(t, "basic:\n  users:\n    alice: "+hashFor(t, "s3cret"))
	a, err := New(context.Background(), cfg, "/")
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
	p := newBasicProvider(&BasicConfig{Users: map[string]string{"alice": hashFor(t, "s3cret")}})

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
}
