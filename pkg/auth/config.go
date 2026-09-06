package auth

import (
	"fmt"
	"net/netip"
	"path"
	"slices"
	"strings"

	config_util "github.com/prometheus/common/config"
	"golang.org/x/crypto/bcrypt"
)

// DefaultOIDCConfig is the base config an `auth.oidc` block is unmarshaled into.
var DefaultOIDCConfig = OIDCConfig{
	UsernameClaim: "sub",
}

// Config is the `proxeus.auth` block: the set of authentication providers
// proxeus applies to incoming requests. When the block is absent every request
// is anonymous.
type Config struct {
	// ExemptPaths are request paths that bypass authentication entirely,
	// matched verbatim: a path is exempt when it is equal to an entry or sits
	// below it. They must include --web.route-prefix if one is set. When unset
	// the defaults passed to New apply; an empty list exempts nothing.
	ExemptPaths []string `yaml:"exempt_paths"`

	// Providers, tried in the order they are documented here: trusted_header,
	// basic, oidc. At least one must be set.
	Basic         *BasicConfig         `yaml:"basic,omitempty"`
	OIDC          *OIDCConfig          `yaml:"oidc,omitempty"`
	TrustedHeader *TrustedHeaderConfig `yaml:"trusted_header,omitempty"`

	// Authorization restricts which authenticated identities are served. When
	// the block is absent every identity is allowed.
	Authorization *AuthorizationConfig `yaml:"authorization,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	return c.validate()
}

func (c *Config) validate() error {
	if c.Basic == nil && c.OIDC == nil && c.TrustedHeader == nil {
		return fmt.Errorf("auth: at least one of basic, oidc or trusted_header must be configured")
	}
	return nil
}

// BasicConfig configures HTTP basic authentication. Users has the same shape as
// exporter-toolkit's basic_auth_users: username -> bcrypt hash. The hashes are
// Secrets so that /api/v1/status/config renders them as <secret> rather than
// handing every caller something to crack offline.
type BasicConfig struct {
	Users map[string]config_util.Secret `yaml:"users"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *BasicConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain BasicConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	return c.validate()
}

func (c *BasicConfig) validate() error {
	if len(c.Users) == 0 {
		return fmt.Errorf("auth.basic: users must not be empty")
	}
	for user, hash := range c.Users {
		if _, err := bcrypt.Cost([]byte(hash)); err != nil {
			return fmt.Errorf("auth.basic: user %q: password is not a bcrypt hash: %w", user, err)
		}
	}
	return nil
}

// OIDCConfig configures bearer-token authentication against an OIDC issuer.
type OIDCConfig struct {
	// IssuerURL is the issuer's base URL; its discovery document
	// (/.well-known/openid-configuration) is fetched once at startup.
	IssuerURL string `yaml:"issuer_url"`
	// ClientID is the expected audience of the token. A token is also accepted
	// when its `azp` claim matches, which is how Keycloak identifies the client
	// while leaving `aud` set to something else (e.g. "account").
	ClientID string `yaml:"client_id"`
	// UsernameClaim is the claim holding the identity name. Defaults to "sub".
	UsernameClaim string `yaml:"username_claim"`
	// GroupsClaim is the claim holding the identity's groups. Optional; when
	// unset the identity has no groups.
	GroupsClaim string `yaml:"groups_claim"`
	// SkipIssuerVerification stops proxeus checking that a token's `iss` claim
	// matches IssuerURL. UNSAFE: any issuer whose keys the discovery document
	// advertises can then mint tokens proxeus accepts. Only useful when the
	// issuer is reachable under a different URL than the one it announces.
	SkipIssuerVerification bool `yaml:"skip_issuer_verification"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *OIDCConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultOIDCConfig

	type plain OIDCConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	return c.validate()
}

func (c *OIDCConfig) validate() error {
	if c.IssuerURL == "" {
		return fmt.Errorf("auth.oidc: issuer_url must be set")
	}
	if c.ClientID == "" {
		return fmt.Errorf("auth.oidc: client_id must be set")
	}
	return nil
}

// TrustedHeaderConfig configures identity taken from a request header set by an
// authenticating proxy in front of proxeus (oauth2-proxy, an ingress, ...).
type TrustedHeaderConfig struct {
	// UserHeader carries the identity name, e.g. X-Forwarded-User.
	UserHeader string `yaml:"user_header"`
	// GroupsHeader carries a comma-separated group list. Optional.
	GroupsHeader string `yaml:"groups_header"`
	// TrustedProxies are the CIDRs the headers are honored from, matched
	// against the connection's remote address. Required: without it anyone who
	// can reach proxeus directly could set the header themselves.
	TrustedProxies []string `yaml:"trusted_proxies"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *TrustedHeaderConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain TrustedHeaderConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	return c.validate()
}

func (c *TrustedHeaderConfig) validate() error {
	if c.UserHeader == "" {
		return fmt.Errorf("auth.trusted_header: user_header must be set")
	}
	if len(c.TrustedProxies) == 0 {
		return fmt.Errorf("auth.trusted_header: trusted_proxies must not be empty")
	}
	_, err := c.prefixes()
	return err
}

// prefixes parses TrustedProxies into the netip form the provider matches on.
func (c *TrustedHeaderConfig) prefixes() ([]netip.Prefix, error) {
	prefixes := make([]netip.Prefix, len(c.TrustedProxies))
	for i, proxy := range c.TrustedProxies {
		prefix, err := netip.ParsePrefix(proxy)
		if err != nil {
			return nil, fmt.Errorf("auth.trusted_header: trusted_proxies %q is not a CIDR: %w", proxy, err)
		}
		prefixes[i] = prefix.Masked()
	}
	return prefixes, nil
}

// AuthorizationConfig is the `auth.authorization` block: which of the callers
// authentication let in are actually served. An identity passes an allow-list
// pair when its name is in allowed_users or one of its groups is in
// allowed_groups. With both top-level lists empty every authenticated identity
// passes and only the route rules restrict.
type AuthorizationConfig struct {
	// AllowedUsers are identity names allowed anywhere.
	AllowedUsers []string `yaml:"allowed_users"`
	// AllowedGroups are groups whose members are allowed anywhere. Only the
	// providers that carry groups can satisfy this -- oidc with a groups_claim,
	// trusted_header with a groups_header -- so a group-only policy locks out
	// basic auth users, who have no groups at all.
	AllowedGroups []string `yaml:"allowed_groups"`

	// Routes tighten the policy for parts of the API. Every rule whose
	// path_prefix covers the request applies in addition to the lists above:
	// an identity must pass all of them. A path no rule covers needs only the
	// top-level policy.
	Routes []RouteAuthorizationConfig `yaml:"routes"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *AuthorizationConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain AuthorizationConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	return c.validate()
}

func (c *AuthorizationConfig) validate() error {
	if len(c.AllowedUsers) == 0 && len(c.AllowedGroups) == 0 && len(c.Routes) == 0 {
		return fmt.Errorf("auth.authorization: at least one of allowed_users, allowed_groups or routes must not be empty")
	}
	if err := validateNames("auth.authorization", c.AllowedUsers, c.AllowedGroups); err != nil {
		return err
	}
	// Validated from here rather than in their own UnmarshalYAML: yaml skips
	// the unmarshaler for a null item, which would let an empty rule through.
	for _, route := range c.Routes {
		if err := route.validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateNames rejects empty entries: no provider ever yields an empty name
// or group, so the entry can only match nothing and turn the list into a
// blanket denial with no hint as to why.
func validateNames(where string, lists ...[]string) error {
	for _, list := range lists {
		if slices.Contains(list, "") {
			return fmt.Errorf("%s: allow-list entries must not be empty", where)
		}
	}
	return nil
}

// RouteAuthorizationConfig is one entry of `auth.authorization.routes`.
type RouteAuthorizationConfig struct {
	// PathPrefix is the request path the rule covers, matched on whole segments
	// the way exempt_paths is: /mcp also covers /mcp/foo, but not /mcpx. It
	// must include --web.route-prefix if one is set. "/" is rejected: it would
	// match only the root itself, and a rule for everything is what the
	// top-level lists are.
	PathPrefix    string   `yaml:"path_prefix"`
	AllowedUsers  []string `yaml:"allowed_users"`
	AllowedGroups []string `yaml:"allowed_groups"`
}

func (c *RouteAuthorizationConfig) validate() error {
	if !strings.HasPrefix(c.PathPrefix, "/") {
		return fmt.Errorf("auth.authorization.routes: path_prefix %q must start with /", c.PathPrefix)
	}
	if path.Clean(c.PathPrefix) == "/" {
		return fmt.Errorf("auth.authorization.routes: path_prefix / covers only the root; use allowed_users/allowed_groups for everything")
	}
	if len(c.AllowedUsers) == 0 && len(c.AllowedGroups) == 0 {
		return fmt.Errorf("auth.authorization.routes: %q: at least one of allowed_users or allowed_groups must not be empty", c.PathPrefix)
	}
	return validateNames(fmt.Sprintf("auth.authorization.routes: %q", c.PathPrefix), c.AllowedUsers, c.AllowedGroups)
}
