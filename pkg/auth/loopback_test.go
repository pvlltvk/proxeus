package auth

import (
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
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
