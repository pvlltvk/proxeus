package auth

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"
)

// loopbackFixture wires the real thing: an inner server behind a loopback
// listener wrapped by the Loopback, and an outer server reverse-proxying to it
// through the Loopback transport. The outer server attaches the identity named
// by the X-Test-User header the way the middleware would; inner runs with the
// identity the inner request context carries, if any.
func loopbackFixture(t *testing.T, inner http.HandlerFunc) (*httptest.Server, *Loopback) {
	t.Helper()

	loopback := NewLoopback()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	innerSrv := &http.Server{Handler: inner}
	go func() { _ = innerSrv.Serve(loopback.Listener(ln)) }()
	t.Cleanup(func() { innerSrv.Close() })

	target, err := url.Parse("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("parsing target: %v", err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = loopback.Transport()

	outer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user := r.Header.Get("X-Test-User"); user != "" {
			r = r.WithContext(NewContext(r.Context(), Identity{
				Name:     user,
				Groups:   []string{"admins"},
				Provider: "basic",
			}))
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(outer.Close)

	return outer, loopback
}

// get calls the outer server as user, or anonymously when user is empty.
func get(t *testing.T, srv *httptest.Server, user string) *http.Response {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if user != "" {
		req.Header.Set("X-Test-User", user)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return resp
}

// TestLoopbackTransportDisablesKeepAlives pins the one flag the whole scheme
// depends on: a pooled connection would hand a later request the identity of
// whoever dialed it first. See TestLoopbackPropagatesIdentity's two sequential
// subtests (one authenticated, one anonymous) on the same fixture for that
// scenario exercised end to end.
func TestLoopbackTransportDisablesKeepAlives(t *testing.T) {
	if !NewLoopback().Transport().DisableKeepAlives {
		t.Fatal("Loopback.Transport() must disable keep-alives, or a pooled connection could carry a later request's identity from an earlier one")
	}
}

func TestLoopbackPropagatesIdentity(t *testing.T) {
	type result struct {
		id Identity
		ok bool
	}
	results := make(chan result, 1)
	outer, _ := loopbackFixture(t, func(_ http.ResponseWriter, r *http.Request) {
		id, ok := FromContext(r.Context())
		results <- result{id, ok}
	})

	tests := []struct {
		name string
		user string
		want Identity
	}{
		{
			name: "authenticated request",
			user: "alice",
			want: Identity{Name: "alice", Groups: []string{"admins"}, Provider: "basic"},
		},
		{
			name: "anonymous request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			get(t, outer, tt.user).Body.Close()

			got := <-results
			if tt.user == "" {
				if got.ok {
					t.Fatalf("inner handler saw identity %v, want none", got.id)
				}
				return
			}
			if !got.ok {
				t.Fatal("inner handler saw no identity, want one")
			}
			if got.id.Name != tt.want.Name || got.id.Provider != tt.want.Provider {
				t.Fatalf("inner handler saw identity %v, want %v", got.id, tt.want)
			}
			if len(got.id.Groups) != 1 || got.id.Groups[0] != tt.want.Groups[0] {
				t.Fatalf("inner handler saw groups %v, want %v", got.id.Groups, tt.want.Groups)
			}
		})
	}
}

// Callers in flight at once must not see each other's identity, and no
// registration may outlive its request.
func TestLoopbackConcurrentIdentities(t *testing.T) {
	outer, loopback := loopbackFixture(t, func(w http.ResponseWriter, r *http.Request) {
		id, _ := FromContext(r.Context())
		w.Header().Set("X-Identity", id.Name)
	})

	const callers = 20
	errs := make(chan string, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			user := string(rune('a' + i))
			req, err := http.NewRequest(http.MethodGet, outer.URL, nil)
			if err != nil {
				errs <- err.Error()
				return
			}
			req.Header.Set("X-Test-User", user)
			resp, err := outer.Client().Do(req)
			if err != nil {
				errs <- err.Error()
				return
			}
			resp.Body.Close()
			if got := resp.Header.Get("X-Identity"); got != user {
				errs <- "inner handler saw identity " + got + ", want " + user
				return
			}
			errs <- ""
		}(i)
	}
	for i := 0; i < callers; i++ {
		if msg := <-errs; msg != "" {
			t.Fatal(msg)
		}
	}

	loopback.mtx.Lock()
	defer loopback.mtx.Unlock()
	if len(loopback.ids) != 0 {
		t.Fatalf("registry still holds %d identities after the requests, want it empty", len(loopback.ids))
	}
}

// TestLoopbackAcceptedConnCloseLeavesRegistryAlone pins the invariant that
// makes ephemeral-port reuse safe: closing the accepted side of a connection
// must never remove a registration, because that close has no ordering
// relationship with when the dialer's local port becomes free again. If it
// did forget here, a registration a concurrent, unrelated connection just
// made by reusing that same freed port could be deleted out from under it.
// Cleanup is the dial side's job alone -- see registeredConn's comment.
func TestLoopbackAcceptedConnCloseLeavesRegistryAlone(t *testing.T) {
	loopback := NewLoopback()
	const key = "127.0.0.1:1"
	loopback.mtx.Lock()
	loopback.ids[key] = Identity{Name: "bob"}
	loopback.mtx.Unlock()

	server, client := net.Pipe()
	defer client.Close()
	accepted := &acceptedConn{
		Conn: server,
		addr: identityAddr{Addr: server.LocalAddr(), loopback: loopback, key: key},
	}

	if err := accepted.Close(); err != nil {
		t.Fatalf("closing the accepted conn: %v", err)
	}

	id, ok := loopback.get(key)
	if !ok || id.Name != "bob" {
		t.Fatalf("accepted conn's Close forgot a registration it does not own: got id=%v ok=%v, want the untouched registration", id, ok)
	}
}

// TestLoopbackForgetsWhenNeverAccepted covers the case the dial side alone has
// to handle: a connection dialContext registered an identity for that never
// reaches a handler at all, because nothing ever accepts (and here, times
// out) it. The registration must not outlive it regardless.
func TestLoopbackForgetsWhenNeverAccepted(t *testing.T) {
	loopback := NewLoopback()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer ln.Close() // deliberately never Accept()s

	ctx, cancel := context.WithTimeout(NewContext(context.Background(), Identity{Name: "alice"}), 50*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String(), nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}

	if _, err := loopback.Transport().RoundTrip(req); err == nil {
		t.Fatal("expected the request to fail since nothing ever accepts it")
	}

	loopback.mtx.Lock()
	defer loopback.mtx.Unlock()
	if len(loopback.ids) != 0 {
		t.Fatalf("registry still holds %d identities after an unaccepted, canceled request, want it empty", len(loopback.ids))
	}
}

// TestLoopbackWithoutTransportInstalled mirrors cmd/proxeus/main.go without an
// `auth` block: the listener is wrapped unconditionally, but the reverse
// proxy's transport is left at its default because there is no identity to
// carry. FromContext must resolve to no identity, not panic, even though the
// request's http.LocalAddrContextKey value is still our identityAddr type.
func TestLoopbackWithoutTransportInstalled(t *testing.T) {
	loopback := NewLoopback()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	type result struct {
		id Identity
		ok bool
	}
	results := make(chan result, 1)
	innerSrv := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		id, ok := FromContext(r.Context())
		results <- result{id, ok}
	})}
	go func() { _ = innerSrv.Serve(loopback.Listener(ln)) }()
	t.Cleanup(func() { innerSrv.Close() })

	resp, err := http.Get("http://" + ln.Addr().String())
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()

	got := <-results
	if got.ok {
		t.Fatalf("expected no identity without the transport installed, got %v", got.id)
	}
}
