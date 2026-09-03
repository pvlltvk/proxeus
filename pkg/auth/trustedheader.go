package auth

import (
	"net"
	"net/http"
	"net/netip"
	"strings"

	"github.com/sirupsen/logrus"
)

type trustedHeaderProvider struct {
	userHeader   string
	groupsHeader string
	trusted      []netip.Prefix
}

func newTrustedHeaderProvider(cfg *TrustedHeaderConfig) (*trustedHeaderProvider, error) {
	trusted, err := cfg.prefixes()
	if err != nil {
		return nil, err
	}
	return &trustedHeaderProvider{
		userHeader:   cfg.UserHeader,
		groupsHeader: cfg.GroupsHeader,
		trusted:      trusted,
	}, nil
}

func (p *trustedHeaderProvider) authenticate(r *http.Request) (Identity, error) {
	user := r.Header.Get(p.userHeader)
	if user == "" {
		return Identity{}, errNoCredentials
	}

	// A header anyone can set is only an identity when it comes from a proxy we
	// put there. From anywhere else it is not a failed login, it is not a login
	// at all -- the rest of the chain still gets its turn.
	if !p.trustedRemote(r.RemoteAddr) {
		logrus.Debugf("Ignoring %s from untrusted address %s", p.userHeader, r.RemoteAddr)
		return Identity{}, errNoCredentials
	}

	id := Identity{Name: user, Provider: "trusted_header"}
	if p.groupsHeader != "" {
		for _, group := range strings.Split(r.Header.Get(p.groupsHeader), ",") {
			if group = strings.TrimSpace(group); group != "" {
				id.Groups = append(id.Groups, group)
			}
		}
	}

	return id, nil
}

func (p *trustedHeaderProvider) trustedRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		// RemoteAddr is "IP:port" for TCP, but not for a unix socket listener.
		host = remoteAddr
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	addr = addr.Unmap()
	for _, prefix := range p.trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}
