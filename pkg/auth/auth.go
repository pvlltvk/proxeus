// Package auth authenticates incoming requests to proxeus. It offers three
// providers -- a trusted header set by an authenticating proxy, HTTP basic auth
// and OIDC bearer tokens -- which are tried in that fixed order. The identity
// they produce is then checked against the optional authorization policy.
package auth

import (
	"context"
	"errors"
	"net/http"
	"path"
	"slices"
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

// NewContext returns ctx with id attached, the way the middleware does it.
func NewContext(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey, id)
}

// FromContext returns the identity authenticated for this request, if any.
func FromContext(ctx context.Context) (Identity, bool) {
	if id, ok := ctx.Value(identityKey).(Identity); ok {
		return id, true
	}
	// Behind the internal loopback listener the request context is a fresh one
	// built by net/http: the connection's local address is the only thing the
	// identity could travel in. See loopback.go.
	if addr, ok := ctx.Value(http.LocalAddrContextKey).(identityAddr); ok {
		return addr.identity()
	}
	return Identity{}, false
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
	authz       *authorization
}

// authorization is the compiled `auth.authorization` block: the policy every
// identity must pass, plus the route rules that tighten it. A nil authorization
// allows every authenticated identity.
type authorization struct {
	policy policy
	routes []routePolicy
}

// policy is one allow-list pair. An identity passes it by name or by group; a
// policy with neither list -- only the top-level one can be empty, routes are
// validated -- passes everyone.
type policy struct {
	users  []string
	groups []string
}

type routePolicy struct {
	pathPrefix string
	policy     policy
}

func newAuthorization(cfg *AuthorizationConfig) *authorization {
	a := &authorization{policy: policy{users: cfg.AllowedUsers, groups: cfg.AllowedGroups}}
	for _, route := range cfg.Routes {
		a.routes = append(a.routes, routePolicy{
			pathPrefix: path.Clean(route.PathPrefix),
			policy:     policy{users: route.AllowedUsers, groups: route.AllowedGroups},
		})
	}
	return a
}

func (p policy) allows(id Identity) bool {
	if len(p.users) == 0 && len(p.groups) == 0 {
		return true
	}
	if slices.Contains(p.users, id.Name) {
		return true
	}
	for _, group := range id.Groups {
		if slices.Contains(p.groups, group) {
			return true
		}
	}
	return false
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

	if cfg.Authorization != nil {
		a.authz = newAuthorization(cfg.Authorization)
	}

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
			if !a.allowed(id, r.URL.Path) {
				a.forbidden(w, r, id)
				return
			}

			next.ServeHTTP(w, r.WithContext(NewContext(r.Context(), id)))
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
		if under(clean, p) {
			return true
		}
	}
	return false
}

// allowed reports whether id may have urlPath: it has to pass the top-level
// policy and, if one of the route rules covers the path, that rule too. The
// first matching rule is the only one consulted.
func (a *Authenticator) allowed(id Identity, urlPath string) bool {
	if a.authz == nil {
		return true
	}
	if !a.authz.policy.allows(id) {
		return false
	}

	clean := path.Clean(urlPath)
	for _, route := range a.authz.routes {
		if under(clean, route.pathPrefix) {
			return route.policy.allows(id)
		}
	}
	return true
}

// under reports whether the cleaned path is prefix or sits below it, matched on
// whole segments.
func under(clean, prefix string) bool {
	return clean == prefix || strings.HasPrefix(clean, prefix+"/")
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

// forbidden rejects a caller the authorization policy does not allow. No
// challenge is sent: the credentials were fine, presenting them again changes
// nothing.
func (a *Authenticator) forbidden(w http.ResponseWriter, r *http.Request, id Identity) {
	logrus.Debugf("Denying %s %s to %q: not allowed by the authorization policy", r.Method, r.URL.Path, id.Name)
	http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
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
