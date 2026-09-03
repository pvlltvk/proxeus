package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

// testIssuer is a minimal OIDC issuer: a discovery document, a JWKS and a way
// to mint tokens signed by the key in it.
type testIssuer struct {
	url    string
	signer jose.Signer
}

func newTestIssuer(t *testing.T) *testIssuer {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	signer, err := jose.NewSigner(
		jose.SigningKey{Algorithm: jose.RS256, Key: jose.JSONWebKey{Key: key, KeyID: "test"}},
		(&jose.SignerOptions{}).WithType("JWT"),
	)
	if err != nil {
		t.Fatalf("creating signer: %v", err)
	}

	issuer := &testIssuer{signer: signer}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{
			"issuer": %q,
			"authorization_endpoint": "%s/auth",
			"token_endpoint": "%s/token",
			"jwks_uri": "%s/keys",
			"id_token_signing_alg_values_supported": ["RS256"]
		}`, issuer.url, issuer.url, issuer.url, issuer.url)
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
			Key:       key.Public(),
			KeyID:     "test",
			Algorithm: string(jose.RS256),
			Use:       "sig",
		}}})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	issuer.url = srv.URL

	return issuer
}

func (i *testIssuer) token(t *testing.T, claims map[string]interface{}) string {
	t.Helper()

	full := map[string]interface{}{
		"iss": i.url,
		"iat": time.Now().Add(-time.Minute).Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	for k, v := range claims {
		full[k] = v
	}

	payload, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshaling claims: %v", err)
	}
	jws, err := i.signer.Sign(payload)
	if err != nil {
		t.Fatalf("signing token: %v", err)
	}
	raw, err := jws.CompactSerialize()
	if err != nil {
		t.Fatalf("serializing token: %v", err)
	}
	return raw
}

func TestOIDCProvider(t *testing.T) {
	issuer := newTestIssuer(t)
	cfg := configFromYAML(t, fmt.Sprintf(`
oidc:
  issuer_url: %s
  client_id: proxeus
  username_claim: preferred_username
  groups_claim: groups
`, issuer.url))
	a, err := New(context.Background(), cfg, "/")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tests := []struct {
		name     string
		claims   map[string]interface{}
		token    string
		status   int
		identity Identity
	}{
		{
			name: "valid token",
			claims: map[string]interface{}{
				"aud":                "proxeus",
				"preferred_username": "alice",
				"groups":             []string{"admins", "viewers"},
			},
			status:   http.StatusOK,
			identity: Identity{Name: "alice", Groups: []string{"admins", "viewers"}, Provider: "oidc"},
		},
		{
			name: "azp names the client while aud does not",
			claims: map[string]interface{}{
				"aud":                "account",
				"azp":                "proxeus",
				"preferred_username": "alice",
			},
			status:   http.StatusOK,
			identity: Identity{Name: "alice", Provider: "oidc"},
		},
		{
			name: "expired token",
			claims: map[string]interface{}{
				"aud":                "proxeus",
				"preferred_username": "alice",
				"exp":                time.Now().Add(-time.Minute).Unix(),
			},
			status: http.StatusUnauthorized,
		},
		{
			name: "wrong audience",
			claims: map[string]interface{}{
				"aud":                "other-client",
				"preferred_username": "alice",
			},
			status: http.StatusUnauthorized,
		},
		{
			name: "wrong issuer",
			claims: map[string]interface{}{
				"iss":                "https://issuer.example",
				"aud":                "proxeus",
				"preferred_username": "alice",
			},
			status: http.StatusUnauthorized,
		},
		{
			name:   "missing username claim",
			claims: map[string]interface{}{"aud": "proxeus"},
			status: http.StatusUnauthorized,
		},
		{
			name:   "not a token at all",
			token:  "garbage",
			status: http.StatusUnauthorized,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			token := test.token
			if token == "" {
				token = issuer.token(t, test.claims)
			}

			var got Identity
			req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			a.Middleware(identityHandler(&got)).ServeHTTP(w, req)

			if w.Code != test.status {
				t.Fatalf("status = %d, want %d (body %q)", w.Code, test.status, strings.TrimSpace(w.Body.String()))
			}
			if got.Name != test.identity.Name || got.Provider != test.identity.Provider {
				t.Errorf("identity = %+v, want %+v", got, test.identity)
			}
			if strings.Join(got.Groups, ",") != strings.Join(test.identity.Groups, ",") {
				t.Errorf("groups = %v, want %v", got.Groups, test.identity.Groups)
			}
			if test.status == http.StatusUnauthorized {
				if want := `Basic realm="proxeus"`; strings.Contains(w.Header().Get("WWW-Authenticate"), want) {
					t.Errorf("WWW-Authenticate = %q, want no Basic challenge when basic auth is not configured", w.Header().Get("WWW-Authenticate"))
				}
			}
		})
	}
}

// Startup must fail loudly rather than serve with an issuer proxeus could never
// verify tokens against.
func TestOIDCDiscoveryFailureFailsStartup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	defer srv.Close()

	cfg := configFromYAML(t, fmt.Sprintf("oidc:\n  issuer_url: %s\n  client_id: proxeus", srv.URL))
	if _, err := New(context.Background(), cfg, "/"); err == nil {
		t.Fatal("New succeeded against an issuer with no discovery document")
	}
}

// Credentials one provider claims are never handed to the next: a bad password
// must not be rescued by a valid bearer token, and vice versa.
func TestChainDoesNotFallThrough(t *testing.T) {
	issuer := newTestIssuer(t)
	cfg := configFromYAML(t, fmt.Sprintf(`
basic:
  users:
    alice: %s
oidc:
  issuer_url: %s
  client_id: proxeus
`, hashFor(t, "s3cret"), issuer.url))
	a, err := New(context.Background(), cfg, "/")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	token := issuer.token(t, map[string]interface{}{"aud": "proxeus", "sub": "alice"})

	t.Run("both schemes are offered", func(t *testing.T) {
		w := httptest.NewRecorder()
		a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/query", nil))
		if want := `Basic realm="proxeus", Bearer`; w.Header().Get("WWW-Authenticate") != want {
			t.Fatalf("WWW-Authenticate = %q, want %q", w.Header().Get("WWW-Authenticate"), want)
		}
	})

	t.Run("bearer token is checked by oidc", func(t *testing.T) {
		var got Identity
		req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		a.Middleware(identityHandler(&got)).ServeHTTP(w, req)
		if w.Code != http.StatusOK || got.Provider != "oidc" {
			t.Fatalf("status = %d, identity = %+v, want 200 from the oidc provider", w.Code, got)
		}
	})

	t.Run("a bad password is not retried as a bearer token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/query", nil)
		req.SetBasicAuth("alice", "hunter2")
		w := httptest.NewRecorder()
		a.Middleware(identityHandler(&Identity{})).ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", w.Code)
		}
	})
}
