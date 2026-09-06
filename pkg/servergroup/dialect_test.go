package servergroup

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	"gopkg.in/yaml.v2"

	"github.com/pvlltvk/proxeus/pkg/auth"
	"github.com/pvlltvk/proxeus/pkg/promclient"
)

func TestDialectConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		wantErr     bool
		errMsg      string
		wantParams  url.Values
		wantHeaders map[string]string
	}{
		{
			name: "no backend_type and no dialect block",
			config: `
static_configs:
  - targets:
      - localhost:9090
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "backend_type without a dialect block",
			config: `
backend_type: prometheus
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "unknown backend_type",
			config: `
backend_type: influxdb
`,
			wantErr: true,
			errMsg:  `invalid backend_type "influxdb"`,
		},
		{
			name: "empty thanos block emits nothing",
			config: `
backend_type: thanos
thanos: {}
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "full thanos block",
			config: `
backend_type: thanos
thanos:
  dedup: false
  partial_response: true
  max_source_resolution: 5m
  replica_labels:
    - replica
    - prometheus_replica
`,
			wantParams: url.Values{
				"dedup":                 []string{"false"},
				"partial_response":      []string{"true"},
				"max_source_resolution": []string{"5m"},
				"replicaLabels[]":       []string{"replica", "prometheus_replica"},
			},
			wantHeaders: map[string]string{},
		},
		{
			name: "thanos max_source_resolution auto",
			config: `
backend_type: thanos
thanos:
  max_source_resolution: auto
`,
			wantParams:  url.Values{"max_source_resolution": []string{"auto"}},
			wantHeaders: map[string]string{},
		},
		{
			name: "thanos max_source_resolution 0s",
			config: `
backend_type: thanos
thanos:
  max_source_resolution: 0s
`,
			wantParams:  url.Values{"max_source_resolution": []string{"0s"}},
			wantHeaders: map[string]string{},
		},
		{
			name: "thanos max_source_resolution is not a duration",
			config: `
backend_type: thanos
thanos:
  max_source_resolution: sometimes
`,
			wantErr: true,
			errMsg:  `invalid thanos max_source_resolution "sometimes"`,
		},
		{
			name: "thanos block without matching backend_type",
			config: `
thanos:
  dedup: true
`,
			wantErr: true,
			errMsg:  "thanos block requires backend_type: thanos",
		},
		{
			name: "thanos block with the wrong backend_type",
			config: `
backend_type: victoriametrics
thanos:
  dedup: true
`,
			wantErr: true,
			errMsg:  `thanos block requires backend_type: thanos, got "victoriametrics"`,
		},
		{
			name: "full victoriametrics block",
			config: `
backend_type: victoriametrics
victoriametrics:
  nocache: true
  max_lookback: 10m
  deny_partial_response: true
  extra_filters:
    - '{env="prod"}'
    - '{job=~"api.*"}'
`,
			wantParams: url.Values{
				"nocache":               []string{"1"},
				"max_lookback":          []string{"10m"},
				"deny_partial_response": []string{"1"},
				"extra_filters":         []string{`{env="prod"}`, `{job=~"api.*"}`},
			},
			wantHeaders: map[string]string{},
		},
		{
			name: "victoriametrics nocache explicitly disabled",
			config: `
backend_type: victoriametrics
victoriametrics:
  nocache: false
`,
			wantParams:  url.Values{"nocache": []string{"0"}},
			wantHeaders: map[string]string{},
		},
		{
			name: "victoriametrics extra_filters that does not parse",
			config: `
backend_type: victoriametrics
victoriametrics:
  extra_filters:
    - 'env=prod'
`,
			wantErr: true,
			errMsg:  `error parsing victoriametrics extra_filters entry "env=prod"`,
		},
		{
			name: "victoriametrics max_lookback is not a duration",
			config: `
backend_type: victoriametrics
victoriametrics:
  max_lookback: 10 minutes
`,
			wantErr: true,
			errMsg:  `invalid victoriametrics max_lookback "10 minutes"`,
		},
		{
			name: "victoriametrics block without matching backend_type",
			config: `
backend_type: prometheus
victoriametrics:
  nocache: true
`,
			wantErr: true,
			errMsg:  "victoriametrics block requires backend_type: victoriametrics",
		},
		{
			name: "mimir block",
			config: `
backend_type: mimir
mimir:
  tenant: tenant-a
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{"X-Scope-OrgID": "tenant-a"},
		},
		{
			name: "mimir block is accepted for cortex",
			config: `
backend_type: cortex
mimir:
  tenant: tenant-a
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{"X-Scope-OrgID": "tenant-a"},
		},
		{
			name: "mimir block without a tenant",
			config: `
backend_type: mimir
mimir: {}
`,
			wantErr: true,
			errMsg:  "mimir tenant is required",
		},
		{
			name: "mimir tenant_from_identity without a static tenant",
			config: `
backend_type: mimir
mimir:
  tenant_from_identity:
    source: user
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "mimir tenant_from_identity with a static tenant as the fallback",
			config: `
backend_type: mimir
mimir:
  tenant: tenant-fallback
  tenant_from_identity:
    source: group
    map:
      admins: tenant-a
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{"X-Scope-OrgID": "tenant-fallback"},
		},
		{
			name: "mimir tenant_from_identity without a source",
			config: `
backend_type: mimir
mimir:
  tenant_from_identity: {}
`,
			wantErr: true,
			errMsg:  `invalid mimir tenant_from_identity source ""`,
		},
		{
			name: "mimir tenant_from_identity with an unknown source",
			config: `
backend_type: mimir
mimir:
  tenant_from_identity:
    source: token
`,
			wantErr: true,
			errMsg:  `invalid mimir tenant_from_identity source "token"`,
		},
		{
			name: "mimir tenant_from_identity by group without a map",
			config: `
backend_type: mimir
mimir:
  tenant_from_identity:
    source: group
`,
			wantErr: true,
			errMsg:  "mimir tenant_from_identity map is required with source: group",
		},
		{
			name: "mimir tenant_from_identity without matching backend_type",
			config: `
backend_type: thanos
mimir:
  tenant_from_identity:
    source: user
`,
			wantErr: true,
			errMsg:  "mimir block requires backend_type: mimir or cortex",
		},
		{
			name: "mimir block without matching backend_type",
			config: `
backend_type: thanos
mimir:
  tenant: tenant-a
`,
			wantErr: true,
			errMsg:  "mimir block requires backend_type: mimir or cortex",
		},
		{
			name: "generic query_params and http_headers without a dialect block",
			config: `
query_params:
  nocache: "1"
http_headers:
  X-Scope-OrgID: tenant-a
`,
			wantParams:  url.Values{"nocache": []string{"1"}},
			wantHeaders: map[string]string{"X-Scope-OrgID": "tenant-a"},
		},
		{
			name: "generic config overrides the dialect",
			config: `
backend_type: thanos
thanos:
  dedup: true
  partial_response: true
query_params:
  dedup: "false"
http_headers:
  X-Proxeus-Source: proxeus-1
`,
			wantParams: url.Values{
				"dedup":            []string{"false"},
				"partial_response": []string{"true"},
			},
			wantHeaders: map[string]string{"X-Proxeus-Source": "proxeus-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte(tt.config), &cfg)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if !contains(err.Error(), tt.errMsg) {
					t.Fatalf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if params := cfg.queryParams(); !reflect.DeepEqual(params, tt.wantParams) {
				t.Errorf("query params mismatch\nexpected=%v\nactual=%v", tt.wantParams, params)
			}
			if headers := cfg.httpHeaders(); !reflect.DeepEqual(headers, tt.wantHeaders) {
				t.Errorf("headers mismatch\nexpected=%v\nactual=%v", tt.wantHeaders, headers)
			}
		})
	}
}

// TestDialectQueryParamsAreOwnedByCaller checks the params don't alias the
// config slices -- the caller merges query_params into them.
func TestDialectQueryParamsAreOwnedByCaller(t *testing.T) {
	cfg := Config{
		BackendType: BackendThanos,
		Thanos:      &ThanosConfig{ReplicaLabels: []string{"replica"}},
	}

	cfg.queryParams()["replicaLabels[]"][0] = "mutated"

	if cfg.Thanos.ReplicaLabels[0] != "replica" {
		t.Fatalf("expected config to be untouched, got replica_labels=%v", cfg.Thanos.ReplicaLabels)
	}
}

// TestDialectQueryParamsInURL checks the params actually land on the downstream
// URL -- including the repeated ones, which the client wrap must not collapse.
func TestDialectQueryParamsInURL(t *testing.T) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`
backend_type: thanos
thanos:
  dedup: true
  replica_labels:
    - replica
    - prometheus_replica
query_params:
  dedup: "false"
`), &cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	client, err := api.NewClient(api.Config{Address: "http://localhost:9090"})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	u := promclient.NewClientArgsWrap(client, cfg.queryParams()).URL("/api/v1/query", nil)

	expected := url.Values{
		"dedup":           []string{"false"},
		"replicaLabels[]": []string{"replica", "prometheus_replica"},
	}
	if !reflect.DeepEqual(u.Query(), expected) {
		t.Fatalf("query mismatch\nexpected=%v\nactual=%v (%s)", expected, u.Query(), u.RawQuery)
	}
}

// TestDialectHeadersOnRequest checks the mimir tenant reaches the downstream as
// an X-Scope-OrgID header, and that http_headers still wins over it.
func TestDialectHeadersOnRequest(t *testing.T) {
	tests := []struct {
		name       string
		cfg        *Config
		wantTenant string
	}{
		{
			name: "tenant from the mimir block",
			cfg: &Config{
				BackendType: BackendMimir,
				Mimir:       &MimirConfig{Tenant: "tenant-a"},
			},
			wantTenant: "tenant-a",
		},
		{
			name: "http_headers overrides the mimir block",
			cfg: &Config{
				BackendType:       BackendCortex,
				Mimir:             &MimirConfig{Tenant: "tenant-a"},
				HTTPClientHeaders: map[string]string{"X-Scope-OrgID": "tenant-b"},
			},
			wantTenant: "tenant-b",
		},
		{
			name:       "no dialect block and no http_headers",
			cfg:        &Config{},
			wantTenant: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTenant string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotTenant = r.Header.Get("X-Scope-OrgID")
			}))
			defer server.Close()

			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			tt.cfg.Scheme = "http"
			tt.cfg.HTTPConfig.DialTimeout = 200 * time.Millisecond
			if err := sg.ApplyConfig(tt.cfg); err != nil {
				t.Fatalf("failed to apply config: %v", err)
			}

			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			resp, err := sg.RoundTrip(req)
			if err != nil {
				t.Fatalf("failed to make request: %v", err)
			}
			resp.Body.Close()

			if gotTenant != tt.wantTenant {
				t.Fatalf("expected X-Scope-OrgID %q, got %q", tt.wantTenant, gotTenant)
			}
		})
	}
}

func TestResolveTenant(t *testing.T) {
	tests := []struct {
		name     string
		mimir    *MimirConfig
		identity auth.Identity
		want     string
	}{
		{
			name:     "user without a map is the tenant",
			mimir:    &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{Source: TenantSourceUser}},
			identity: auth.Identity{Name: "alice"},
			want:     "alice",
		},
		{
			name: "user the map renames",
			mimir: &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
				Source: TenantSourceUser,
				Map:    map[string]string{"alice": "tenant-a"},
			}},
			identity: auth.Identity{Name: "alice"},
			want:     "tenant-a",
		},
		{
			name: "user the map does not rename keeps its name",
			mimir: &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
				Source: TenantSourceUser,
				Map:    map[string]string{"alice": "tenant-a"},
			}},
			identity: auth.Identity{Name: "bob"},
			want:     "bob",
		},
		{
			name: "first mapped group in identity order wins",
			mimir: &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
				Source: TenantSourceGroup,
				Map:    map[string]string{"admins": "tenant-a", "devs": "tenant-b"},
			}},
			identity: auth.Identity{Name: "alice", Groups: []string{"everyone", "devs", "admins"}},
			want:     "tenant-b",
		},
		{
			name: "no group is mapped",
			mimir: &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
				Source: TenantSourceGroup,
				Map:    map[string]string{"admins": "tenant-a"},
			}},
			identity: auth.Identity{Name: "alice", Groups: []string{"everyone"}},
			want:     "",
		},
		{
			name: "identity has no groups at all",
			mimir: &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
				Source: TenantSourceGroup,
				Map:    map[string]string{"admins": "tenant-a"},
			}},
			identity: auth.Identity{Name: "alice"},
			want:     "",
		},
		{
			name:     "static tenant only resolves nothing",
			mimir:    &MimirConfig{Tenant: "tenant-a"},
			identity: auth.Identity{Name: "alice", Groups: []string{"admins"}},
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mimir.resolveTenant(tt.identity); got != tt.want {
				t.Fatalf("expected tenant %q, got %q", tt.want, got)
			}
		})
	}
}

// TestDialectTenantFromIdentityOnRequest checks the tenant of the caller in the
// request context reaches the downstream, and that a request nothing resolves a
// tenant for is failed rather than sent without one.
func TestDialectTenantFromIdentityOnRequest(t *testing.T) {
	fromGroup := &MimirConfig{TenantFromIdentity: &TenantFromIdentityConfig{
		Source: TenantSourceGroup,
		Map:    map[string]string{"admins": "tenant-a", "devs": "tenant-b"},
	}}
	withFallback := &MimirConfig{Tenant: "tenant-fallback", TenantFromIdentity: fromGroup.TenantFromIdentity}

	tests := []struct {
		name       string
		cfg        *Config
		identity   *auth.Identity
		wantTenant string
		wantErr    string
	}{
		{
			name:       "identity resolves a tenant",
			cfg:        &Config{BackendType: BackendMimir, Mimir: fromGroup},
			identity:   &auth.Identity{Name: "alice", Groups: []string{"devs"}},
			wantTenant: "tenant-b",
		},
		{
			name:       "identity resolves a tenant over http_headers",
			cfg:        &Config{BackendType: BackendMimir, Mimir: fromGroup, HTTPClientHeaders: map[string]string{"X-Scope-OrgID": "tenant-header"}},
			identity:   &auth.Identity{Name: "alice", Groups: []string{"admins"}},
			wantTenant: "tenant-a",
		},
		{
			name:       "identity resolves nothing and falls back",
			cfg:        &Config{BackendType: BackendMimir, Mimir: withFallback},
			identity:   &auth.Identity{Name: "alice", Groups: []string{"everyone"}},
			wantTenant: "tenant-fallback",
		},
		{
			name:       "anonymous request falls back",
			cfg:        &Config{BackendType: BackendMimir, Mimir: withFallback},
			wantTenant: "tenant-fallback",
		},
		{
			name:     "identity resolves nothing and there is no fallback",
			cfg:      &Config{BackendType: BackendMimir, Mimir: fromGroup},
			identity: &auth.Identity{Name: "alice", Groups: []string{"everyone"}},
			wantErr:  `no mimir tenant for user "alice"`,
		},
		{
			name:    "anonymous request and there is no fallback",
			cfg:     &Config{BackendType: BackendMimir, Mimir: fromGroup},
			wantErr: "no mimir tenant for this request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotTenant string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotTenant = r.Header.Get("X-Scope-OrgID")
			}))
			defer server.Close()

			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			tt.cfg.Scheme = "http"
			tt.cfg.HTTPConfig.DialTimeout = 200 * time.Millisecond
			if err := sg.ApplyConfig(tt.cfg); err != nil {
				t.Fatalf("failed to apply config: %v", err)
			}

			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			if tt.identity != nil {
				req = req.WithContext(auth.NewContext(req.Context(), *tt.identity))
			}

			resp, err := sg.RoundTrip(req)
			if tt.wantErr != "" {
				if err == nil {
					resp.Body.Close()
					t.Fatalf("expected error containing %q, got none (tenant %q)", tt.wantErr, gotTenant)
				}
				if !contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("failed to make request: %v", err)
			}
			resp.Body.Close()

			if gotTenant != tt.wantTenant {
				t.Fatalf("expected X-Scope-OrgID %q, got %q", tt.wantTenant, gotTenant)
			}
		})
	}
}
