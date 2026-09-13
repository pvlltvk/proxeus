package servergroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v2"

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
			name: "victoriametrics raw_fetch export is not a query param",
			config: `
backend_type: victoriametrics
victoriametrics:
  raw_fetch: export
  export_max_rows_per_line: 10000
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "victoriametrics raw_fetch is not a known mode",
			config: `
backend_type: victoriametrics
victoriametrics:
  raw_fetch: remote_read
`,
			wantErr: true,
			errMsg:  `invalid victoriametrics raw_fetch "remote_read"`,
		},
		{
			name: "victoriametrics export_max_rows_per_line without export",
			config: `
backend_type: victoriametrics
victoriametrics:
  export_max_rows_per_line: 10000
`,
			wantErr: true,
			errMsg:  "export_max_rows_per_line requires raw_fetch: export",
		},
		{
			name: "victoriametrics export_max_rows_per_line is negative",
			config: `
backend_type: victoriametrics
victoriametrics:
  raw_fetch: export
  export_max_rows_per_line: -1
`,
			wantErr: true,
			errMsg:  "invalid victoriametrics export_max_rows_per_line -1",
		},
		{
			name: "victoriametrics raw_fetch export with remote_read",
			config: `
backend_type: victoriametrics
remote_read: true
victoriametrics:
  raw_fetch: export
`,
			wantErr: true,
			errMsg:  "raw_fetch: export is mutually exclusive with remote_read: true",
		},
		{
			name: "victoriametrics raw_fetch query with remote_read",
			config: `
backend_type: victoriametrics
remote_read: true
victoriametrics:
  raw_fetch: query
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
		},
		{
			name: "victoriametrics raw_fetch is case-sensitive",
			config: `
backend_type: victoriametrics
victoriametrics:
  raw_fetch: EXPORT
`,
			wantErr: true,
			errMsg:  `invalid victoriametrics raw_fetch "EXPORT"`,
		},
		{
			name: "victoriametrics raw_fetch explicitly empty is the default",
			config: `
backend_type: victoriametrics
victoriametrics:
  raw_fetch: ""
`,
			wantParams:  url.Values{},
			wantHeaders: map[string]string{},
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
			name: "victoriametrics block with backend_type: thanos",
			config: `
backend_type: thanos
victoriametrics:
  raw_fetch: export
`,
			wantErr: true,
			errMsg:  `victoriametrics block requires backend_type: victoriametrics, got "thanos"`,
		},
		{
			name: "victoriametrics block with no backend_type at all",
			config: `
victoriametrics:
  raw_fetch: export
`,
			wantErr: true,
			errMsg:  `victoriametrics block requires backend_type: victoriametrics, got ""`,
		},
		{
			// Regression: validateDialect used to return as soon as it found
			// the first non-nil dialect block, so a valid thanos: block hid a
			// mismatched victoriametrics: block sitting alongside it -- the
			// config loaded, and vmExport() (which only looks at
			// VictoriaMetrics, not backend_type) would have wrapped a Thanos
			// group's client to call /api/v1/export against it.
			name: "victoriametrics block alongside a valid thanos block, same backend_type: thanos",
			config: `
backend_type: thanos
thanos:
  dedup: true
victoriametrics:
  raw_fetch: export
`,
			wantErr: true,
			errMsg:  `victoriametrics block requires backend_type: victoriametrics, got "thanos"`,
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

// TestDialectVMRawFetch checks which endpoint a raw fetch lands on: the
// portable /api/v1/query range selector by default, VictoriaMetrics'
// /api/v1/export when the dialect asks for it.
func TestDialectVMRawFetch(t *testing.T) {
	for _, tt := range []struct {
		name     string
		rawFetch VMRawFetch
		wantPath string
	}{
		{name: "default", wantPath: "/api/v1/query"},
		{name: "query", rawFetch: VMRawFetchQuery, wantPath: "/api/v1/query"},
		{name: "export", rawFetch: VMRawFetchExport, wantPath: "/api/v1/export"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				if r.URL.Path == "/api/v1/export" {
					_, _ = w.Write([]byte(`{"metric":{"__name__":"up"},"values":[1],"timestamps":[1000]}` + "\n"))
					return
				}
				_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
					`{"metric":{"__name__":"up"},"values":[[1,"1"]]}]}}`))
			}))
			defer server.Close()

			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			cfg := &Config{
				Scheme:      "http",
				BackendType: BackendVictoriaMetrics,
				VictoriaMetrics: &VictoriaMetricsConfig{
					RawFetch: tt.rawFetch,
				},
				HTTPConfig: HTTPClientConfig{DialTimeout: 200 * time.Millisecond},
			}
			if err := sg.ApplyConfig(cfg); err != nil {
				t.Fatalf("failed to apply config: %v", err)
			}
			host := strings.TrimPrefix(server.URL, "http://")
			if err := sg.loadTargetGroupMap(map[string][]*targetgroup.Group{
				"x": {{Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(host)}}}},
			}); err != nil {
				t.Fatalf("loadTargetGroupMap: %v", err)
			}

			matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}
			ss := sg.GetValue(context.Background(), time.Unix(0, 0), time.Unix(60, 0), matchers)
			if !ss.Next() {
				t.Fatalf("no series returned: %v", ss.Err())
			}
			if gotPath != tt.wantPath {
				t.Fatalf("raw fetch hit %q, want %q", gotPath, tt.wantPath)
			}
		})
	}
}
