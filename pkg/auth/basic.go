package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

// fakeHash is compared against when the user is unknown, so that an
// unauthenticated caller cannot enumerate usernames by timing the response.
// It is a bcrypt hash of "fakepassword", straight from exporter-toolkit.
const fakeHash = "$2y$10$QOauhQNbBCuQDKes6eFzPeMqBSjb7Mr5DUmpZ/VcEd00UAV/LDeSi"

type basicProvider struct {
	users map[string]string

	// bcryptMtx keeps bcrypt.CompareHashAndPassword -- deliberately expensive,
	// ~100ms at cost 10 -- from running in parallel and eating every core, as
	// exporter-toolkit does for the same reason.
	bcryptMtx sync.Mutex

	// verified holds the (user, password) pairs already checked against their
	// hash, so a dashboard polling every few seconds pays for bcrypt once.
	// Only successful checks of configured users are cached, which bounds it
	// at one entry per user -- an unknown user or a wrong password must never
	// let a caller grow this map.
	mtx      sync.Mutex
	verified map[string]struct{}
}

func newBasicProvider(cfg *BasicConfig) *basicProvider {
	p := &basicProvider{
		users:    make(map[string]string, len(cfg.Users)),
		verified: make(map[string]struct{}, len(cfg.Users)),
	}
	for user, hash := range cfg.Users {
		p.users[user] = string(hash)
	}
	return p
}

func (p *basicProvider) authenticate(r *http.Request) (Identity, error) {
	if _, ok := credentials(r, "Basic"); !ok {
		return Identity{}, errNoCredentials
	}
	user, password, ok := r.BasicAuth()
	if !ok {
		return Identity{}, fmt.Errorf("basic: malformed Authorization header")
	}

	hash, known := p.users[user]
	if !known {
		// Spend the same bcrypt round on a fixed hash, so that an unknown user
		// costs what a wrong password costs. Nothing is cached here: the key
		// would be entirely attacker-chosen.
		p.bcryptMatches(fakeHash, password)
		return Identity{}, fmt.Errorf("basic: invalid credentials for user %q", user)
	}

	if !p.verify(user, hash, password) {
		return Identity{}, fmt.Errorf("basic: invalid credentials for user %q", user)
	}

	return Identity{Name: user, Provider: "basic"}, nil
}

// verify reports whether password matches the configured hash of user,
// remembering the successful pairs it has already seen.
func (p *basicProvider) verify(user, hash, password string) bool {
	sum := sha256.Sum256([]byte(password))
	key := user + "\x00" + hash + "\x00" + hex.EncodeToString(sum[:])

	p.mtx.Lock()
	_, cached := p.verified[key]
	p.mtx.Unlock()
	if cached {
		return true
	}

	if !p.bcryptMatches(hash, password) {
		return false
	}

	p.mtx.Lock()
	p.verified[key] = struct{}{}
	p.mtx.Unlock()
	return true
}

func (p *basicProvider) bcryptMatches(hash, password string) bool {
	p.bcryptMtx.Lock()
	defer p.bcryptMtx.Unlock()
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}
