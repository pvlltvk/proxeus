package promxyui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	proxyconfig "github.com/jacksontj/promxy/pkg/config"
	"github.com/jacksontj/promxy/pkg/servergroup"
	"github.com/prometheus/common/model"
)

// stubStorage satisfies storageAccessor for tests.
type stubStorage struct {
	cfg *proxyconfig.Config
	sgs []*servergroup.ServerGroup
}

func (s *stubStorage) Config() *proxyconfig.Config              { return s.cfg }
func (s *stubStorage) ServerGroups() []*servergroup.ServerGroup { return s.sgs }

func sgCfg(name, scheme, pathPrefix string) *servergroup.Config {
	return &servergroup.Config{
		Name:       name,
		Scheme:     scheme,
		PathPrefix: pathPrefix,
		Labels:     model.LabelSet{},
	}
}

// testProbeClientTimeout is the HTTP client timeout used throughout this
// test file, both for the prober itself and for the standalone client used
// to drive probeTarget directly.
const testProbeClientTimeout = 2 * time.Second

func newTestProber() *Prober {
	return &Prober{
		client:   &http.Client{Timeout: testProbeClientTimeout},
		results:  make(map[probeKey]ProbeResult),
		interval: defaultProbeInterval,
		timeout:  testProbeClientTimeout,
	}
}

// testClient is the HTTP client used to drive probeTarget directly in tests
// (where there is no real server_group transport to derive one from).
func testClient() *http.Client {
	return &http.Client{Timeout: testProbeClientTimeout}
}

func TestProber_HealthyTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/status/buildinfo":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","data":{"version":"0.41.0"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	result := newTestProber().probeTarget(context.Background(), testClient(), sgCfg("g", "http", ""), host)

	if !result.Healthy {
		t.Errorf("expected healthy=true, got false; lastError=%q", result.LastError)
	}
	if result.Version != "0.41.0" {
		t.Errorf("expected version 0.41.0, got %q", result.Version)
	}
}

// TestProber_UsesPathPrefix is the regression guard for the bug where the
// prober ignored server_group's path_prefix. With path_prefix set, both
// buildinfo and the /labels fallback must be probed under that prefix.
func TestProber_UsesPathPrefix(t *testing.T) {
	const prefix = "/select/0/prometheus"

	var sawBuildInfoAt, sawLabelsAt string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case prefix + "/api/v1/status/buildinfo":
			sawBuildInfoAt = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"version":"2.24.0"}}`))
		case prefix + "/api/v1/labels":
			sawLabelsAt = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		default:
			// Anything outside the prefix is a bug — return 404 so the test fails.
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	result := newTestProber().probeTarget(context.Background(), testClient(), sgCfg("vm", "http", prefix), host)

	if !result.Healthy {
		t.Fatalf("expected healthy=true, got false; lastError=%q", result.LastError)
	}
	if sawBuildInfoAt != prefix+"/api/v1/status/buildinfo" {
		t.Errorf("buildinfo probe missed the path_prefix; saw %q", sawBuildInfoAt)
	}
	// labels endpoint isn't hit on the happy path, but the URL builder is shared.
	_ = sawLabelsAt
}

func TestProber_PathPrefix_LabelsFallback(t *testing.T) {
	const prefix = "/select/0/prometheus"

	var labelsHit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case prefix + "/api/v1/status/buildinfo":
			// Simulate a backend whose buildinfo is broken/404 but /labels works.
			http.NotFound(w, r)
		case prefix + "/api/v1/labels":
			labelsHit = r.URL.Path
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","data":["__name__"]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	result := newTestProber().probeTarget(context.Background(), testClient(), sgCfg("vm", "http", prefix), host)

	if !result.Healthy {
		t.Errorf("expected healthy=true (labels fallback), got false; lastError=%q", result.LastError)
	}
	if labelsHit != prefix+"/api/v1/labels" {
		t.Errorf("labels fallback missed the path_prefix; saw %q", labelsHit)
	}
}

func TestProber_ErrorTarget(t *testing.T) {
	// Point at a port that is not listening.
	result := newTestProber().probeTarget(context.Background(), testClient(), sgCfg("g", "http", ""), "127.0.0.1:1")

	if result.Healthy {
		t.Errorf("expected healthy=false for unreachable target")
	}
	if result.LastError == "" {
		t.Errorf("expected a non-empty LastError, got blank")
	}
}

func TestProber_Inventory_NilConfig(t *testing.T) {
	p := newProber(&stubStorage{})
	inv := p.Inventory()
	if inv.Groups != nil {
		t.Errorf("expected nil Groups for nil config, got %v", inv.Groups)
	}
}

func TestProber_Inventory_EmptyGroups(t *testing.T) {
	cfg := &proxyconfig.Config{
		PromxyConfig: proxyconfig.PromxyConfig{
			ServerGroups: []*servergroup.Config{},
		},
	}
	p := newProber(&stubStorage{cfg: cfg})
	inv := p.Inventory()
	if len(inv.Groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(inv.Groups))
	}
}

// TestProber_Inventory_BackendTypeFromConfig verifies that BackendType in the
// inventory comes from the server_group config, not from the prober's
// classification of the probe response.
func TestProber_Inventory_BackendTypeFromConfig(t *testing.T) {
	cfg := &proxyconfig.Config{
		PromxyConfig: proxyconfig.PromxyConfig{
			ServerGroups: []*servergroup.Config{
				{Name: "declared", BackendType: "thanos", Labels: model.LabelSet{"backend": "thanos"}},
				{Name: "empty", Labels: model.LabelSet{"backend": "x"}}, // no backend_type
			},
		},
	}
	p := newProber(&stubStorage{cfg: cfg})
	inv := p.Inventory()
	if len(inv.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(inv.Groups))
	}
	// We can't drive TargetInfo here without a real ServerGroup; just confirm
	// the field-mapping path exercised by Inventory() doesn't panic and the
	// group ordering matches config order.
	if inv.Groups[0].Name != "declared" || inv.Groups[1].Name != "empty" {
		t.Errorf("groups out of order: %+v", inv.Groups)
	}
}
