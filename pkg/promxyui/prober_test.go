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

func (s *stubStorage) Config() *proxyconfig.Config          { return s.cfg }
func (s *stubStorage) ServerGroups() []*servergroup.ServerGroup { return s.sgs }

// makeTestConfig builds a minimal PromxyConfig with one server_group whose
// name and resolved target are provided by the caller.
func makeTestConfig(name, _ string) *proxyconfig.Config {
	return &proxyconfig.Config{
		PromxyConfig: proxyconfig.PromxyConfig{
			ServerGroups: []*servergroup.Config{
				{
					Name:   name,
					Scheme: "http",
					Labels: model.LabelSet{},
				},
			},
		},
	}
}

func TestProber_HealthyTarget(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/status/buildinfo":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","data":{"version":"0.36.0","application":"thanos query"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	result := (&Prober{
		client:   &http.Client{Timeout: 2 * time.Second},
		results:  make(map[probeKey]ProbeResult),
		interval: defaultProbeInterval,
	}).probeTarget(context.Background(), "http", host)

	if !result.Healthy {
		t.Errorf("expected healthy=true, got false; lastError=%q", result.LastError)
	}
	if result.BackendType != BackendThanos {
		t.Errorf("expected BackendThanos, got %q", result.BackendType)
	}
	if result.Version != "0.36.0" {
		t.Errorf("expected version 0.36.0, got %q", result.Version)
	}
}

func TestProber_VictoriaMetrics404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/status/buildinfo":
			http.NotFound(w, r)
		case "/api/v1/labels":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","data":[]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	result := (&Prober{
		client:   &http.Client{Timeout: 2 * time.Second},
		results:  make(map[probeKey]ProbeResult),
		interval: defaultProbeInterval,
	}).probeTarget(context.Background(), "http", host)

	if !result.Healthy {
		t.Errorf("expected healthy=true (labels fallback), got false; lastError=%q", result.LastError)
	}
	if result.BackendType != BackendVictoriaMetrics {
		t.Errorf("expected BackendVictoriaMetrics, got %q", result.BackendType)
	}
}

func TestProber_ErrorTarget(t *testing.T) {
	// Point at a port that is not listening.
	result := (&Prober{
		client:   &http.Client{Timeout: 200 * time.Millisecond},
		results:  make(map[probeKey]ProbeResult),
		interval: defaultProbeInterval,
	}).probeTarget(context.Background(), "http", "127.0.0.1:1")

	if result.Healthy {
		t.Errorf("expected healthy=false for unreachable target")
	}
	if result.BackendType != BackendUnknown {
		t.Errorf("expected BackendUnknown, got %q", result.BackendType)
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
