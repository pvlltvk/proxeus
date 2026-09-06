package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/pvlltvk/proxeus/pkg/fakeprom"
	"github.com/pvlltvk/proxeus/pkg/proxeusui"
	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// testBackend stands in for proxeus's own API.
func testBackend(series int) http.Handler {
	return fakeprom.New(fakeprom.Config{Series: series})
}

// testMetadata stands in for the federated metadata lookup, which does not go
// over the API.
func testMetadata(_ context.Context, _, _ string) (map[string][]v1.Metadata, error) {
	return map[string][]v1.Metadata{
		"fake_metric":  {{Type: "gauge", Help: "a fake metric"}},
		"other_metric": {{Type: "counter", Help: "another one", Unit: "seconds"}},
	}, nil
}

// newTestSession serves an MCP endpoint in front of backend and returns a
// client session connected to it.
func newTestSession(t *testing.T, backend http.Handler, cfg Config) *mcp.ClientSession {
	t.Helper()

	// The tools reach the backend through the in-process transport, exactly
	// as they reach the Prometheus handler in proxeus.
	cfg.Handler = backend
	if cfg.Inventory == nil {
		cfg.Inventory = func() proxeusui.Inventory { return proxeusui.Inventory{} }
	}
	if cfg.Metadata == nil {
		cfg.Metadata = testMetadata
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("error creating MCP server: %v", err)
	}

	mcpSrv := httptest.NewServer(s)
	t.Cleanup(mcpSrv.Close)

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0.0.1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: mcpSrv.URL}, nil)
	if err != nil {
		t.Fatalf("error connecting to MCP endpoint: %v", err)
	}
	t.Cleanup(func() { session.Close() }) //nolint:errcheck
	return session
}

// callTool calls a tool that is expected to succeed and decodes its structured
// output.
func callTool[T any](t *testing.T, session *mcp.ClientSession, name string, args map[string]any) T {
	t.Helper()

	res := call(t, session, name, args)
	if res.IsError {
		t.Fatalf("tool %s returned an error: %s", name, resultText(res))
	}
	b, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("error marshaling structured content of %s: %v", name, err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("error decoding structured content of %s: %v", name, err)
	}
	return out
}

func call(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()

	res, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("error calling tool %s: %v", name, err)
	}
	return res
}

func resultText(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}

func TestListTools(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	res, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("error listing tools: %v", err)
	}

	var got []string
	for _, tool := range res.Tools {
		got = append(got, tool.Name)
	}
	slices.Sort(got)

	want := []string{"build_info", "label_names", "label_values", "list_server_groups", "metric_metadata", "query", "range_query", "series"}
	if !slices.Equal(got, want) {
		t.Errorf("tools: got %v, want %v", got, want)
	}
}

func TestQuery(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[queryOutput](t, session, "query", map[string]any{
		"query": "fake_metric",
	})

	if out.ResultType != "vector" {
		t.Errorf("result_type: got %q, want vector", out.ResultType)
	}
	if len(out.Result) != 3 {
		t.Fatalf("got %d series, want 3", len(out.Result))
	}
	if got := out.Result[0].Metric["__name__"]; got != "fake_metric" {
		t.Errorf("__name__: got %q, want fake_metric", got)
	}
	if len(out.Result[0].Values) != 1 {
		t.Errorf("instant query series carries %d samples, want 1", len(out.Result[0].Values))
	}
	if out.Truncated || out.Returned != 3 || out.TotalBeforeTruncation != 3 {
		t.Errorf("truncation: got %+v, want all 3 series returned untruncated", out.truncation)
	}
}

func TestQueryAtTimestamp(t *testing.T) {
	session := newTestSession(t, testBackend(1), Config{})

	out := callTool[queryOutput](t, session, "query", map[string]any{
		"query":     "fake_metric",
		"timestamp": "2026-05-23T14:00:00Z",
	})

	want := float64(time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC).Unix())
	if got := out.Result[0].Values[0].Timestamp; got != want {
		t.Errorf("sample timestamp: got %v, want %v", got, want)
	}
}

func TestRangeQuery(t *testing.T) {
	start := time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)
	session := newTestSession(t, testBackend(2), Config{})

	out := callTool[queryOutput](t, session, "range_query", map[string]any{
		"query":      "fake_metric",
		"start_time": strconv.FormatInt(start.Unix(), 10),
		"end_time":   strconv.FormatInt(start.Add(5*time.Minute).Unix(), 10),
		"step":       "1m",
	})

	if out.ResultType != "matrix" {
		t.Errorf("result_type: got %q, want matrix", out.ResultType)
	}
	if len(out.Result) != 2 {
		t.Fatalf("got %d series, want 2", len(out.Result))
	}
	if got := len(out.Result[0].Values); got != 6 {
		t.Errorf("got %d samples over 5m at 1m, want 6", got)
	}
	if out.Truncated {
		t.Errorf("truncation: got %+v, want untruncated", out.truncation)
	}
}

func TestQueryTruncation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		maxSeries  int
		argLimit   int
		wantSeries int
	}{
		{name: "server cap", maxSeries: 2, wantSeries: 2},
		{name: "argument lowers the cap", maxSeries: 4, argLimit: 1, wantSeries: 1},
		{name: "argument cannot raise the cap", maxSeries: 2, argLimit: 5, wantSeries: 2},
		{name: "uncapped", wantSeries: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := newTestSession(t, testBackend(5), Config{MaxSeries: tc.maxSeries})

			args := map[string]any{"query": "fake_metric"}
			if tc.argLimit > 0 {
				args["truncation_limit"] = tc.argLimit
			}
			out := callTool[queryOutput](t, session, "query", args)

			if len(out.Result) != tc.wantSeries {
				t.Errorf("got %d series, want %d", len(out.Result), tc.wantSeries)
			}
			if out.Returned != tc.wantSeries {
				t.Errorf("returned: got %d, want %d", out.Returned, tc.wantSeries)
			}
			if out.TotalBeforeTruncation != 5 {
				t.Errorf("total_before_truncation: got %d, want 5", out.TotalBeforeTruncation)
			}
			if want := tc.wantSeries < 5; out.Truncated != want {
				t.Errorf("truncated: got %v, want %v", out.Truncated, want)
			}
			if out.Truncated && out.Note == "" {
				t.Error("truncated result carries no note")
			}
		})
	}
}

func TestRangeQuerySampleCap(t *testing.T) {
	start := time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)
	session := newTestSession(t, testBackend(2), Config{MaxSamples: 4})

	out := callTool[queryOutput](t, session, "range_query", map[string]any{
		"query":      "fake_metric",
		"start_time": strconv.FormatInt(start.Unix(), 10),
		"end_time":   strconv.FormatInt(start.Add(5*time.Minute).Unix(), 10),
		"step":       "1m",
	})

	if !out.Truncated {
		t.Fatalf("truncation: got %+v, want truncated", out.truncation)
	}
	if len(out.Result) != 1 {
		t.Fatalf("got %d series, want 1 (the second does not fit the sample budget)", len(out.Result))
	}
	if got := len(out.Result[0].Values); got != 4 {
		t.Errorf("got %d samples, want the 4-sample budget", got)
	}
	if out.Returned != 1 || out.TotalBeforeTruncation != 2 {
		t.Errorf("truncation: got %+v, want 1 of 2 series", out.truncation)
	}
}

func TestSeries(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[seriesOutput](t, session, "series", map[string]any{
		"matches": []string{"fake_metric"},
	})

	if len(out.Series) != 3 {
		t.Fatalf("got %d series, want 3", len(out.Series))
	}
	if got := out.Series[0]["__name__"]; got != "fake_metric" {
		t.Errorf("__name__: got %q, want fake_metric", got)
	}
	if out.Truncated {
		t.Errorf("truncation: got %+v, want untruncated", out.truncation)
	}
}

func TestLabelNames(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[labelNamesOutput](t, session, "label_names", nil)

	if !slices.Contains(out.Names, "job") {
		t.Errorf("label names %v do not contain job", out.Names)
	}
	if out.Returned != len(out.Names) {
		t.Errorf("returned: got %d, want %d", out.Returned, len(out.Names))
	}
}

func TestLabelValues(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[labelValuesOutput](t, session, "label_values", map[string]any{"label": "job"})

	if want := []string{"fake"}; !slices.Equal(out.Values, want) {
		t.Errorf("values: got %v, want %v", out.Values, want)
	}
}

func TestLabelValuesRequiresLabel(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	res := call(t, session, "label_values", map[string]any{"label": ""})
	if !res.IsError {
		t.Fatal("expected an error result for an empty label")
	}
	if got := resultText(res); !strings.Contains(got, "label parameter is required") {
		t.Errorf("error text: got %q", got)
	}
}

func TestMetricMetadata(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[metricMetadataOutput](t, session, "metric_metadata", nil)

	md, ok := out.Metadata["fake_metric"]
	if !ok {
		t.Fatalf("metadata does not contain fake_metric: %v", out.Metadata)
	}
	if len(md) != 1 || md[0].Type != "gauge" || md[0].Help != "a fake metric" {
		t.Errorf("fake_metric metadata: got %+v", md)
	}
	if out.Returned != 2 {
		t.Errorf("returned: got %d, want 2", out.Returned)
	}
}

func TestMetricMetadataTruncated(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{MaxSeries: 1})

	out := callTool[metricMetadataOutput](t, session, "metric_metadata", nil)

	if len(out.Metadata) != 1 || !out.Truncated || out.TotalBeforeTruncation != 2 {
		t.Errorf("got %d entries, truncation %+v; want 1 of 2 truncated", len(out.Metadata), out.truncation)
	}
}

func TestBuildInfo(t *testing.T) {
	session := newTestSession(t, testBackend(3), Config{})

	out := callTool[buildInfoOutput](t, session, "build_info", nil)

	if out.Version != "3.5.0" || out.Revision != "fakeprom" {
		t.Errorf("build info: got %+v", out)
	}
}

func TestAPIErrorIsToolError(t *testing.T) {
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"status":"error","errorType":"bad_data","error":"invalid parameter \"query\": unexpected end of input"}`) //nolint:errcheck
	})
	session := newTestSession(t, backend, Config{})

	res := call(t, session, "query", map[string]any{"query": "sum("})
	if !res.IsError {
		t.Fatal("expected an error result")
	}
	if got := resultText(res); !strings.Contains(got, "unexpected end of input") {
		t.Errorf("error text: got %q, want the API error message", got)
	}
}

func TestListServerGroups(t *testing.T) {
	probedAt := time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)
	inv := proxeusui.Inventory{
		GeneratedAt: probedAt,
		Groups: []proxeusui.GroupInfo{
			{
				Name:       "thanos",
				Labels:     map[string]string{"backend": "thanos"},
				RemoteRead: true,
				TimeRange:  "now-90d to now-3d",
				Targets: []proxeusui.TargetInfo{
					{
						URL:         "http://thanos:10902",
						BackendType: servergroup.BackendThanos,
						ProbeResult: proxeusui.ProbeResult{Healthy: true, LastProbeAt: probedAt, Version: "0.36.0"},
					},
					{
						URL:         "http://thanos-2:10902",
						BackendType: servergroup.BackendThanos,
						ProbeResult: proxeusui.ProbeResult{Healthy: false, LastError: "probe timed out", LastProbeAt: probedAt},
					},
				},
			},
			{Name: "empty"},
		},
	}
	session := newTestSession(t, testBackend(0), Config{Inventory: func() proxeusui.Inventory { return inv }})

	out := callTool[serverGroupsOutput](t, session, "list_server_groups", nil)

	if len(out.Groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(out.Groups))
	}
	got := out.Groups[0]
	if got.Name != "thanos" || got.BackendType != "thanos" || !got.RemoteRead || got.TimeRange != "now-90d to now-3d" {
		t.Errorf("group: got %+v", got)
	}
	if len(got.Targets) != 2 {
		t.Fatalf("got %d targets, want 2", len(got.Targets))
	}
	if !got.Targets[0].Healthy || got.Targets[0].Version != "0.36.0" {
		t.Errorf("healthy target: got %+v", got.Targets[0])
	}
	if got.Targets[1].Healthy || got.Targets[1].LastError != "probe timed out" {
		t.Errorf("unhealthy target: got %+v", got.Targets[1])
	}
	if out.Groups[1].BackendType != string(proxeusui.BackendUnknown) {
		t.Errorf("group without targets: got backend type %q, want unknown", out.Groups[1].BackendType)
	}
}

func TestNewRequiresDependencies(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{name: "no handler", cfg: Config{Inventory: func() proxeusui.Inventory { return proxeusui.Inventory{} }, Metadata: testMetadata}},
		{name: "no inventory", cfg: Config{Handler: testBackend(1), Metadata: testMetadata}},
		{name: "no metadata", cfg: Config{Handler: testBackend(1), Inventory: func() proxeusui.Inventory { return proxeusui.Inventory{} }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Error("expected an error, got none")
			}
		})
	}
}

// TestRoutePrefix guards the tools against a proxeus running under
// --web.route-prefix: the API is then served under a path.
func TestRoutePrefix(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/foo/", http.StripPrefix("/foo", testBackend(1)))
	session := newTestSession(t, mux, Config{RoutePrefix: "/foo"})

	out := callTool[queryOutput](t, session, "query", map[string]any{"query": "fake_metric"})
	if len(out.Result) != 1 {
		t.Errorf("got %d series, want 1", len(out.Result))
	}
}

func TestQueryTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{name: "no timeout", timeout: 0},
		{name: "generous timeout", timeout: 10 * time.Second},
		{name: "expired timeout", timeout: time.Nanosecond, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := newTestSession(t, testBackend(1), Config{QueryTimeout: tc.timeout})

			res := call(t, session, "query", map[string]any{"query": "fake_metric"})
			if res.IsError != tc.wantErr {
				t.Errorf("IsError: got %v, want %v (%s)", res.IsError, tc.wantErr, resultText(res))
			}
		})
	}
}

func TestSeriesRequiresMatches(t *testing.T) {
	session := newTestSession(t, testBackend(1), Config{})

	res := call(t, session, "series", nil)
	if !res.IsError {
		t.Fatal("expected an error result when matches is missing")
	}
	if got := resultText(res); !strings.Contains(got, "matches") {
		t.Errorf("error text: got %q, want it to name the missing argument", got)
	}
}

func TestMetadataError(t *testing.T) {
	session := newTestSession(t, testBackend(1), Config{
		Metadata: func(context.Context, string, string) (map[string][]v1.Metadata, error) {
			return nil, errors.New("all server_groups failed")
		},
	})

	res := call(t, session, "metric_metadata", nil)
	if !res.IsError {
		t.Fatal("expected an error result")
	}
	if got := resultText(res); !strings.Contains(got, "all server_groups failed") {
		t.Errorf("error text: got %q", got)
	}
}

func TestPanicFailsOnlyTheCall(t *testing.T) {
	session := newTestSession(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}), Config{})

	res := call(t, session, "query", map[string]any{"query": "up"})
	if !res.IsError {
		t.Fatal("expected an error result")
	}
	if got := resultText(res); !strings.Contains(got, "boom") {
		t.Errorf("error text: got %q", got)
	}

	// The server is still up and answers the next call.
	if res := call(t, session, "list_server_groups", nil); res.IsError {
		t.Errorf("list_server_groups after the panic: %s", resultText(res))
	}
}

func TestBothCapsTruncate(t *testing.T) {
	start := time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)
	session := newTestSession(t, testBackend(4), Config{MaxSeries: 3, MaxSamples: 8})

	out := callTool[queryOutput](t, session, "range_query", map[string]any{
		"query":      "fake_metric",
		"start_time": strconv.FormatInt(start.Unix(), 10),
		"end_time":   strconv.FormatInt(start.Add(5*time.Minute).Unix(), 10),
		"step":       "1m",
	})

	// 4 series of 6 samples: the series cap keeps 3, the sample cap then keeps
	// the first one whole and cuts the second at the 8-sample budget.
	if len(out.Result) != 2 {
		t.Fatalf("got %d series, want 2", len(out.Result))
	}
	if got := len(out.Result[1].Values); got != 2 {
		t.Errorf("second series carries %d samples, want 2", got)
	}
	if !strings.Contains(out.Note, "3 of 4 entries") {
		t.Errorf("note does not report the series cap: %q", out.Note)
	}
	if !strings.Contains(out.Note, "8 of 18 samples") {
		t.Errorf("note does not report the sample cap: %q", out.Note)
	}
}

func TestParseTime(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    time.Time
		wantErr bool
	}{
		{in: "1779580800", want: time.Unix(1779580800, 0)},
		{in: "2026-05-23T14:00:00Z", want: time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)},
		{in: "1779580800000", wantErr: true}, // milliseconds
		{in: "NaN", wantErr: true},
		{in: "+Inf", wantErr: true},
		{in: "1e300", wantErr: true},
		{in: "yesterday", wantErr: true},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseTime(tc.in, time.Time{})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseTimeRelative(t *testing.T) {
	got, err := parseTime("1h30m", time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d := time.Since(got); d < 90*time.Minute || d > 91*time.Minute {
		t.Errorf("1h30m resolved to %v ago, want ~90m", d)
	}
}

func TestAPIErrorDetailIsClipped(t *testing.T) {
	detail := strings.Repeat("x", 4096)
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
			"status": "error", "errorType": "bad_data", "error": "boom", "errorDetail": detail,
		})
	})
	session := newTestSession(t, backend, Config{})

	res := call(t, session, "query", map[string]any{"query": "up"})
	if !res.IsError {
		t.Fatal("expected an error result")
	}
	if got := len(resultText(res)); got > maxErrorDetail+256 {
		t.Errorf("error text is %d bytes, want the detail clipped to ~%d", got, maxErrorDetail)
	}
}
