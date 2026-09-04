// Package auth authenticates incoming requests to proxeus. It offers three
// providers -- a trusted header set by an authenticating proxy, HTTP basic auth
// and OIDC bearer tokens -- which are tried in that fixed order.
package auth

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/logging"
)

// errNoCredentials is what a provider returns when the request carries no
// credentials it recognizes, which lets the next provider in the chain try.
var errNoCredentials = errors.New("no credentials")

// Identity is the authenticated caller behind a request.
type Identity struct {
	Name     string
	Groups   []string
	Provider string
}

type contextKey int

const identityKey contextKey = 0

// newContext returns ctx with id attached.
func newContext(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey, id)
}

// FromContext returns the identity authenticated for this request, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey).(Identity)
	return id, ok
}

// provider authenticates a request against one credential source.
type provider interface {
	// authenticate returns the identity the request carries. It returns
	// errNoCredentials when the request carries no credentials this provider
	// handles -- the chain then moves on -- and any other error when the
	// credentials are present but not valid, which fails the request outright.
	authenticate(r *http.Request) (Identity, error)
}

// Authenticator is the configured provider chain.
type Authenticator struct {
	exemptPaths []string
	providers   []provider
	challenge   string
}

// New builds the provider chain described by cfg. It returns a nil
// Authenticator when cfg is nil (no `auth` block: every request is anonymous),
// so callers can leave their handler unwrapped. defaultExempt are the paths
// exempted when the config does not list any of its own; the caller owns them
// because only it knows where its routes ended up (--web.route-prefix,
// --metrics-path).
//
// OIDC discovery happens here, so a bad or unreachable issuer fails startup.
func New(ctx context.Context, cfg *Config, defaultExempt []string) (*Authenticator, error) {
	if cfg == nil {
		return nil, nil
	}

	exempt := cfg.ExemptPaths
	if exempt == nil {
		exempt = defaultExempt
	}
	a := &Authenticator{exemptPaths: make([]string, len(exempt))}
	for i, p := range exempt {
		a.exemptPaths[i] = path.Clean(p)
	}

	var challenges []string
	if cfg.TrustedHeader != nil {
		p, err := newTrustedHeaderProvider(cfg.TrustedHeader)
		if err != nil {
			return nil, err
		}
		a.providers = append(a.providers, p)
	}
	if cfg.Basic != nil {
		a.providers = append(a.providers, newBasicProvider(cfg.Basic))
		challenges = append(challenges, `Basic realm="proxeus"`)
	}
	if cfg.OIDC != nil {
		p, err := newOIDCProvider(ctx, cfg.OIDC)
		if err != nil {
			return nil, err
		}
		a.providers = append(a.providers, p)
		challenges = append(challenges, "Bearer")
	}
	a.challenge = strings.Join(challenges, ", ")

	return a, nil
}

// Middleware wraps next with the provider chain.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A CORS preflight carries no Authorization header -- the browser will
		// not send one until it has the answer -- and the API responds to it
		// without touching any data.
		if a.exempt(r.URL.Path) || isPreflight(r) {
			next.ServeHTTP(w, r)
			return
		}

		for _, p := range a.providers {
			id, err := p.authenticate(r)
			if errors.Is(err, errNoCredentials) {
				continue
			}
			if err != nil {
				a.unauthorized(w, r, err)
				return
			}

			logging.SetUser(w, id.Name)
			next.ServeHTTP(w, r.WithContext(newContext(r.Context(), id)))
			return
		}

		a.unauthorized(w, r, errNoCredentials)
	})
}

// exempt matches on whole path segments, so exempting /metrics covers
// /metrics/foo but neither /metrics2 nor /metrics/../api/v1/query.
func (a *Authenticator) exempt(urlPath string) bool {
	clean := path.Clean(urlPath)
	for _, p := range a.exemptPaths {
		if clean == p || strings.HasPrefix(clean, p+"/") {
			return true
		}
	}
	return false
}

func isPreflight(r *http.Request) bool {
	return r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
}

// unauthorized rejects the request. The reason is only logged: telling the
// client which part of its credentials was wrong helps nobody but an attacker.
// The challenge is empty when trusted_header is the only provider -- there is
// no auth scheme for "come back through the proxy", so none is advertised.
func (a *Authenticator) unauthorized(w http.ResponseWriter, r *http.Request, err error) {
	logrus.Debugf("Rejecting %s %s from %s: %v", r.Method, r.URL.Path, r.RemoteAddr, err)
	if a.challenge != "" {
		w.Header().Set("WWW-Authenticate", a.challenge)
	}
	http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
}

// credentials returns the credentials of an Authorization header with the given
// scheme. Scheme names are case-insensitive (RFC 7235).
func credentials(r *http.Request, scheme string) (string, bool) {
	header := r.Header.Get("Authorization")
	prefix := scheme + " "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	return header[len(prefix):], true
}
