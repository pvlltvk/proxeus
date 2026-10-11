package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/notifications"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/web"
	api_v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/pvlltvk/proxeus/pkg/querystats"
)

// recordingStorage stands in for the server groups: every Select records one
// request into the context's recorder, the way QueryStatsAPI does, and counts
// the Selects that ran without one.
type recordingStorage struct {
	storage.Storage
	withoutRecorder *atomic.Int64
}

func (s recordingStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.Storage.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return recordingQuerier{Querier: q, withoutRecorder: s.withoutRecorder}, nil
}

type recordingQuerier struct {
	storage.Querier
	withoutRecorder *atomic.Int64
}

func (q recordingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	rec := querystats.FromContext(ctx)
	if rec == nil {
		q.withoutRecorder.Add(1)
		return q.Querier.Select(ctx, sortSeries, hints, matchers...)
	}
	e := rec.Begin(querystats.Request{GroupOrdinal: 0, Op: "get_value", Query: "test_select"})
	e.Finish(1, 1, nil)
	return q.Querier.Select(ctx, sortSeries, hints, matchers...)
}

var testGroups = []querystats.Group{
	{Name: "thanos", Ordinal: 0, Labels: map[string]string{"backend": "thanos"}},
	{Name: "sg-1", Ordinal: 1, Labels: map[string]string{"backend": "vm"}},
}

// newQueryStatsHandler builds the embedded Prometheus handler the way main
// does, with the stats renderer and the query stats wrapper in place.
func newQueryStatsHandler(t *testing.T, routePrefix string) (http.Handler, *atomic.Int64) {
	t.Helper()
	st := teststorage.New(t)
	app := st.Appender(context.Background())
	now := time.Now()
	for i := 4; i >= 0; i-- {
		ts := now.Add(-time.Duration(i) * time.Minute).UnixMilli()
		if _, err := app.Append(0, labels.FromStrings("__name__", "up", "job", "a"), ts, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatal(err)
	}

	withoutRecorder := &atomic.Int64{}
	reg := prometheus.NewRegistry()
	notifs := notifications.NewNotifications(1, reg)
	engine := promql.NewEngine(promql.EngineOpts{
		MaxSamples:         1e6,
		Timeout:            time.Minute,
		EnablePerStepStats: true,
	})
	h := web.New(nil, &web.Options{
		Registerer:          reg,
		Gatherer:            reg,
		Context:             context.Background(),
		Storage:             recordingStorage{Storage: st, withoutRecorder: withoutRecorder},
		QueryEngine:         engine,
		LookbackDelta:       5 * time.Minute,
		ListenAddresses:     []string{":0"},
		RoutePrefix:         routePrefix,
		ExternalURL:         &url.URL{Scheme: "http", Host: "localhost", Path: routePrefix},
		Version:             &web.PrometheusVersion{},
		Flags:               map[string]string{},
		NotificationsGetter: notifs.Get,
		NotificationsSub:    notifs.Sub,
		StatsRenderer:       renderQueryStats,
	})
	h.SetReady(web.Ready)
	groups := func() []querystats.Group { return testGroups }
	return withQueryStats(h.HTTPHandler(), routePrefix, groups), withoutRecorder
}

type queryResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []json.RawMessage          `json:"result"`
		Stats  map[string]json.RawMessage `json:"stats"`
	} `json:"data"`
}

func doQuery(t *testing.T, h http.Handler, method, target string, form url.Values) queryResponse {
	t.Helper()
	var req *http.Request
	if method == http.MethodPost {
		req = httptest.NewRequest(method, target, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req = httptest.NewRequest(method, target+"?"+form.Encode(), nil)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s: status %d, body %s", method, target, rec.Code, rec.Body.String())
	}
	var resp queryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return resp
}

func TestQueryStatsAPIResponse(t *testing.T) {
	now := time.Now()
	instant := url.Values{"query": {"sum(up)"}, "time": {strconvUnix(now)}}
	ranged := url.Values{
		"query": {"sum(up)"},
		"start": {strconvUnix(now.Add(-5 * time.Minute))},
		"end":   {strconvUnix(now)},
		"step":  {"60"},
	}
	for _, tc := range []struct {
		name, method, path string
		form               url.Values
	}{
		{"query GET", http.MethodGet, "/api/v1/query", instant},
		{"query POST", http.MethodPost, "/api/v1/query", instant},
		{"query_range GET", http.MethodGet, "/api/v1/query_range", ranged},
		{"query_range POST", http.MethodPost, "/api/v1/query_range", ranged},
	} {
		for _, param := range []string{"true", "all"} {
			t.Run(tc.name+" stats="+param, func(t *testing.T) {
				h, withoutRecorder := newQueryStatsHandler(t, "/")
				form := url.Values{"stats": {param}}
				for k, v := range tc.form {
					form[k] = v
				}
				resp := doQuery(t, h, tc.method, tc.path, form)
				if len(resp.Data.Result) == 0 {
					t.Fatalf("empty result: the query (and its POST body) didn't reach the engine")
				}
				for _, key := range []string{"timings", "samples", "backends"} {
					if _, ok := resp.Data.Stats[key]; !ok {
						t.Fatalf("stats has no %q: %v", key, resp.Data.Stats)
					}
				}
				var backends querystats.Stats
				if err := json.Unmarshal(resp.Data.Stats["backends"], &backends); err != nil {
					t.Fatal(err)
				}
				if len(backends.Groups) != 2 || backends.Groups[0].Name != "thanos" || backends.Groups[1].Name != "sg-1" {
					t.Fatalf("groups = %+v, want the snapshot thanos, sg-1", backends.Groups)
				}
				if backends.Groups[0].Requests == 0 || len(backends.Requests) == 0 || backends.Requests[0].Query != "test_select" {
					t.Fatalf("the engine's Select didn't record into the request's recorder: %+v", backends)
				}
				if n := withoutRecorder.Load(); n != 0 {
					t.Fatalf("%d Selects ran without a recorder", n)
				}
			})
		}
	}
}

func TestQueryStatsNotRequested(t *testing.T) {
	h, withoutRecorder := newQueryStatsHandler(t, "/")
	resp := doQuery(t, h, http.MethodGet, "/api/v1/query", url.Values{"query": {"sum(up)"}})
	if resp.Data.Stats != nil {
		t.Fatalf("stats present without the stats param: %v", resp.Data.Stats)
	}
	if withoutRecorder.Load() == 0 {
		t.Fatalf("expected the query to run without a recorder")
	}
}

func TestQueryStatsRoutePrefix(t *testing.T) {
	h, withoutRecorder := newQueryStatsHandler(t, "/prefix")
	resp := doQuery(t, h, http.MethodGet, "/prefix/api/v1/query", url.Values{"query": {"sum(up)"}, "stats": {"true"}})
	if _, ok := resp.Data.Stats["backends"]; !ok {
		t.Fatalf("no backends under the route prefix: %v", resp.Data.Stats)
	}
	if n := withoutRecorder.Load(); n != 0 {
		t.Fatalf("%d Selects ran without a recorder", n)
	}
}

// Only the two query endpoints, at their prefixed paths, get a recorder.
func TestQueryStatsWrapperPaths(t *testing.T) {
	var got bool
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = querystats.FromContext(r.Context()) != nil
	})
	h := withQueryStats(next, "/prefix", func() []querystats.Group { return testGroups })
	for _, tc := range []struct {
		target string
		want   bool
	}{
		{"/prefix/api/v1/query?stats=true", true},
		{"/prefix/api/v1/query_range?stats=all", true},
		{"/prefix/api/v1/query", false},
		{"/prefix/api/v1/query?stats=", false},
		{"/prefix/api/v1/query_exemplars?stats=true", false},
		{"/prefix/api/v1/series?stats=true", false},
		{"/api/v1/query?stats=true", false},
		{"/prefix/api/v1/query/?stats=true", false},
	} {
		got = false
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, tc.target, nil))
		if got != tc.want {
			t.Errorf("%s: recorder installed = %v, want %v", tc.target, got, tc.want)
		}
	}
}

// Adding backends must leave Prometheus' own stats JSON byte-identical.
func TestQueryStatsRendererKeepsBuiltinBytes(t *testing.T) {
	s := &stats.Statistics{
		Timers:  stats.NewQueryTimers(),
		Samples: stats.NewQuerySamples(true),
	}
	s.Timers.GetTimer(stats.EvalTotalTime).Start().Stop()
	s.Samples.InitStepTracking(0, 2000, 1000)
	s.Samples.IncrementSamplesAtStep(0, 3)
	s.Samples.IncrementSamplesAtStep(1, 4)

	ctx := querystats.NewContext(context.Background(), querystats.NewRecorder(testGroups))
	for _, param := range []string{"true", "all"} {
		want, err := json.Marshal(api_v1.DefaultStatsRenderer(ctx, s, param))
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(renderQueryStats(ctx, s, param))
		if err != nil {
			t.Fatal(err)
		}
		prefix := string(want[:len(want)-1]) + `,"backends":`
		if !strings.HasPrefix(string(got), prefix) {
			t.Fatalf("stats=%s:\n got %s\nwant %s + backends", param, got, want)
		}
	}

	if got := renderQueryStats(context.Background(), s, "true"); got == nil {
		t.Fatal("no recorder: want the default stats, got nil")
	} else if _, ok := got.(*stats.BuiltinStats); !ok {
		t.Fatalf("no recorder: want the default stats type, got %T", got)
	}
	if got := renderQueryStats(ctx, s, ""); got != nil {
		t.Fatalf("no stats param: want nil, got %#v", got)
	}
}

func strconvUnix(t time.Time) string {
	return time.Unix(t.Unix(), 0).UTC().Format(time.RFC3339)
}
