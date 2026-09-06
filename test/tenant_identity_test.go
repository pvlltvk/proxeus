package test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/prometheus/prometheus/util/teststorage"

	"github.com/pvlltvk/proxeus/pkg/auth"
)

const rawTenantFromIdentityConfig = `
proxeus:
  server_groups:
    - static_configs:
        - targets:
          - %s
      backend_type: mimir
      mimir:
        tenant_from_identity:
          source: group
          map:
            admins: tenant-a
            devs: tenant-b
`

// The per-request tenant has to survive the whole way from the outer handler,
// where the request is authenticated, over the internal loopback listener the
// API is reverse-proxied to, through the engine and down to the server group's
// HTTP call. Everything below the reverse proxy is wired the way cmd/proxeus
// wires it.
func TestTenantFromIdentityEndToEnd(t *testing.T) {
	tenants := make(chan string, 1)
	var backendAPI http.Handler
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenants <- r.Header.Get("X-Scope-OrgID")
		backendAPI.ServeHTTP(w, r)
	}))
	defer backend.Close()
	backendAPI = newAPIHandler(teststorage.New(t), testAPIEngine(), backend.Listener.Addr().String())

	ps := getProxyStorage(fmt.Sprintf(rawTenantFromIdentityConfig, backend.Listener.Addr().String()))
	eng := testAPIEngine()
	eng.NodeReplacer = ps.NodeReplacer

	loopback := auth.NewLoopback()
	internalLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("binding the internal listener: %v", err)
	}
	internal := &http.Server{Handler: newAPIHandler(ps, eng, internalLn.Addr().String())}
	go func() { _ = internal.Serve(loopback.Listener(internalLn)) }()
	defer internal.Close()

	internalURL, err := url.Parse("http://" + internalLn.Addr().String())
	if err != nil {
		t.Fatalf("parsing the internal URL: %v", err)
	}
	webProxy := httputil.NewSingleHostReverseProxy(internalURL)
	webProxy.Transport = loopback.Transport()
	proxeus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Stands in for the auth middleware: X-Test-Group names the single
		// group of the caller, no header at all means an anonymous request.
		if group := r.Header.Get("X-Test-Group"); group != "" {
			r = r.WithContext(auth.NewContext(r.Context(), auth.Identity{Name: "alice", Groups: []string{group}}))
		}
		webProxy.ServeHTTP(w, r)
	}))
	defer proxeus.Close()

	tests := []struct {
		name       string
		group      string
		wantStatus int
		wantTenant string
	}{
		{
			name:       "caller in a mapped group",
			group:      "admins",
			wantStatus: http.StatusOK,
			wantTenant: "tenant-a",
		},
		{
			name:       "caller in another mapped group",
			group:      "devs",
			wantStatus: http.StatusOK,
			wantTenant: "tenant-b",
		},
		{
			name:       "caller in no mapped group",
			group:      "everyone",
			wantStatus: http.StatusUnprocessableEntity,
		},
		{
			name:       "anonymous caller",
			wantStatus: http.StatusUnprocessableEntity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, proxeus.URL+"/api/v1/query?query=up", nil)
			if err != nil {
				t.Fatalf("building the request: %v", err)
			}
			if tt.group != "" {
				req.Header.Set("X-Test-Group", tt.group)
			}
			resp, err := proxeus.Client().Do(req)
			if err != nil {
				t.Fatalf("querying: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, resp.StatusCode)
			}
			if tt.wantTenant == "" {
				select {
				case tenant := <-tenants:
					t.Fatalf("backend was queried with tenant %q, expected no query at all", tenant)
				default:
				}
				return
			}
			select {
			case tenant := <-tenants:
				if tenant != tt.wantTenant {
					t.Fatalf("expected backend to be queried with tenant %q, got %q", tt.wantTenant, tenant)
				}
			default:
				t.Fatal("backend was not queried")
			}
		})
	}
}
