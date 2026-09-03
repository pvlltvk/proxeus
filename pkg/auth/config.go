package auth

import (
	"fmt"
	"net/netip"

	"golang.org/x/crypto/bcrypt"
)

// DefaultConfig is the base config an `auth` block is unmarshaled into.
var DefaultConfig = Config{
	ExemptPaths: []string{"/-/healthy", "/-/ready", "/metrics"},
}

// DefaultOIDCConfig is the base config an `auth.oidc` block is unmarshaled into.
var DefaultOIDCConfig = OIDCConfig{
	UsernameClaim: "sub",
}

// Config is the `proxeus.auth` block: the set of authentication providers
// proxeus applies to incoming requests. When the block is absent every request
// is anonymous.
type Config struct {
	// ExemptPaths are request path prefixes that bypass authentication
	// entirely. They are resolved relative to --web.route-prefix.
	ExemptPaths []string `yaml:"exempt_paths"`

	// Providers, tried in the order they are documented here: trusted_header,
	// basic, oidc. At least one must be set.
	Basic         *BasicConfig         `yaml:"basic,omitempty"`
	OIDC          *OIDCConfig          `yaml:"oidc,omitempty"`
	TrustedHeader *TrustedHeaderConfig `yaml:"trusted_header,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig

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
// exporter-toolkit's basic_auth_users: username -> bcrypt hash.
type BasicConfig struct {
	Users map[string]string `yaml:"users"`
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
