package auth

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/coreos/go-oidc/v3/oidc"
)

type oidcProvider struct {
	verifier      *oidc.IDTokenVerifier
	clientID      string
	usernameClaim string
	groupsClaim   string
}

// newOIDCProvider fetches the issuer's discovery document and builds the token
// verifier from it. The JWKS behind it is fetched lazily and cached by go-oidc.
func newOIDCProvider(ctx context.Context, cfg *OIDCConfig) (*oidcProvider, error) {
	provider, err := oidc.NewProvider(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, fmt.Errorf("auth.oidc: discovery of %s failed: %w", cfg.IssuerURL, err)
	}

	return &oidcProvider{
		// The audience is checked below rather than by go-oidc, which only
		// looks at `aud` -- see verifyAudience.
		verifier: provider.Verifier(&oidc.Config{
			ClientID:          cfg.ClientID,
			SkipClientIDCheck: true,
			SkipIssuerCheck:   cfg.SkipIssuerVerification,
		}),
		clientID:      cfg.ClientID,
		usernameClaim: cfg.UsernameClaim,
		groupsClaim:   cfg.GroupsClaim,
	}, nil
}

func (p *oidcProvider) authenticate(r *http.Request) (Identity, error) {
	rawToken, ok := credentials(r, "Bearer")
	if !ok {
		return Identity{}, errNoCredentials
	}

	// Verifies the signature against the issuer's JWKS, plus `iss` and `exp`.
	token, err := p.verifier.Verify(r.Context(), rawToken)
	if err != nil {
		return Identity{}, fmt.Errorf("oidc: %w", err)
	}

	var claims map[string]interface{}
	if err := token.Claims(&claims); err != nil {
		return Identity{}, fmt.Errorf("oidc: reading claims: %w", err)
	}

	if !p.verifyAudience(token, claims) {
		return Identity{}, fmt.Errorf("oidc: token audience %v is not %q", token.Audience, p.clientID)
	}

	name, ok := claims[p.usernameClaim].(string)
	if !ok || name == "" {
		return Identity{}, fmt.Errorf("oidc: token has no %q claim", p.usernameClaim)
	}

	return Identity{Name: name, Groups: p.groups(claims), Provider: "oidc"}, nil
}

// verifyAudience accepts a token whose `aud` contains the client id, or -- the
// Keycloak shape -- whose `azp` names it while `aud` is something else.
func (p *oidcProvider) verifyAudience(token *oidc.IDToken, claims map[string]interface{}) bool {
	if slices.Contains(token.Audience, p.clientID) {
		return true
	}
	azp, _ := claims["azp"].(string)
	return azp == p.clientID
}

func (p *oidcProvider) groups(claims map[string]interface{}) []string {
	if p.groupsClaim == "" {
		return nil
	}

	var groups []string
	switch claim := claims[p.groupsClaim].(type) {
	case string:
		groups = append(groups, claim)
	case []interface{}:
		for _, group := range claim {
			if s, ok := group.(string); ok {
				groups = append(groups, s)
			}
		}
	}
	return groups
}
