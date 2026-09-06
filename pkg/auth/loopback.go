package auth

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// Loopback carries the identity of a request across proxeus's internal
// loopback listener.
//
// /api/v1 and the UI are reverse-proxied to the embedded Prometheus web
// handler over a TCP listener on localhost. That handler builds a fresh
// request context, so the identity the middleware attached outside is lost
// before the PromQL engine reaches the storage layer -- which is where the
// per-request Mimir tenant is needed. Prometheus' web.Handler takes no
// middleware, but net/http does put the accepted connection's local address
// into every request context (http.LocalAddrContextKey), so the connection is
// the seam: the dialer records the identity under the address it dialed from,
// the listener gives the accepted connection a local address that knows how to
// look it up again, and FromContext resolves it. Callers therefore see an
// identity whether they run inside or outside the loopback.
//
// The lookup is deliberately deferred to FromContext rather than done when the
// connection is accepted: the kernel completes the TCP handshake on its own, so
// Accept can return before the dialer has recorded anything. By the time a
// handler asks, the request itself has been written and the entry is there.
//
// The zero value is not usable; call NewLoopback.
type Loopback struct {
	mtx sync.Mutex
	ids map[string]Identity
}

// NewLoopback returns a Loopback whose Listener and Transport belong together:
// one wraps the internal listener, the other the reverse proxy in front of it.
func NewLoopback() *Loopback {
	return &Loopback{ids: make(map[string]Identity)}
}

// Transport is the RoundTripper for the reverse proxy. Keep-alives are off
// because the identity travels with the connection: a pooled one would hand
// the next request the identity of whoever opened it.
func (l *Loopback) Transport() *http.Transport {
	return &http.Transport{
		DialContext:       l.dialContext,
		DisableKeepAlives: true,
	}
}

// Listener wraps the internal listener so requests served on it resolve the
// identity of the caller the connection was dialed for, if any.
func (l *Loopback) Listener(inner net.Listener) net.Listener {
	return &loopbackListener{Listener: inner, loopback: l}
}

func (l *Loopback) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	id, ok := FromContext(ctx)
	if !ok {
		return conn, nil
	}

	key := conn.LocalAddr().String()
	l.mtx.Lock()
	l.ids[key] = id
	l.mtx.Unlock()
	return &registeredConn{Conn: conn, loopback: l, key: key}, nil
}

func (l *Loopback) get(key string) (Identity, bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	id, ok := l.ids[key]
	return id, ok
}

func (l *Loopback) forget(key string) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	delete(l.ids, key)
}

// registeredConn forgets its identity once the connection is gone. Only the
// dial side does this, never the accepted side: dialContext's Transport has
// DisableKeepAlives set, which forces "Connection: close" onto every request,
// and Go's Transport responds to that by closing the conn it dialed exactly
// once, whether the round trip succeeds, fails, or is never accepted at all --
// so this single Close covers every case, including the "closed before it is
// ever accepted" one. Crucially, it forgets *before* closing the underlying
// conn, so the OS cannot hand the same local port to a new connection until
// after the entry is gone. An accepted-side forget would have no such
// ordering guarantee -- its own Close races the client's port reuse on an
// unrelated, concurrent connection -- and could delete a fresh registration
// out from under it. See TestLoopbackAcceptedConnCloseLeavesRegistryAlone.
type registeredConn struct {
	net.Conn
	loopback *Loopback
	key      string
}

func (c *registeredConn) Close() error {
	c.loopback.forget(c.key)
	return c.Conn.Close()
}

type loopbackListener struct {
	net.Listener
	loopback *Loopback
}

func (l *loopbackListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &acceptedConn{
		Conn: conn,
		addr: identityAddr{Addr: conn.LocalAddr(), loopback: l.loopback, key: conn.RemoteAddr().String()},
	}, nil
}

// acceptedConn is a connection whose local address resolves an identity. It
// only ever reads the registry, through its LocalAddr -- see registeredConn's
// comment for why cleanup is the dial side's job alone.
type acceptedConn struct {
	net.Conn
	addr identityAddr
}

func (c *acceptedConn) LocalAddr() net.Addr { return c.addr }

type identityAddr struct {
	net.Addr
	loopback *Loopback
	key      string
}

func (a identityAddr) identity() (Identity, bool) { return a.loopback.get(a.key) }
