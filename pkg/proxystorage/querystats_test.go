package proxystorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promclient"
	"github.com/pvlltvk/proxeus/pkg/querystats"
)

var queryStatsNow = time.Unix(10000, 0)

// queryStatsStub is one server_group holding one series per metric it is
// asked about, carrying its own name in the `group` label. Pushed-down calls
// and raw fetches are both answered with that series. A non-nil err makes
// every data call fail. enter, when set, is called at the start of every data
// call.
type queryStatsStub struct {
	stubAPI
	name  string
	err   error
	enter func()
}

func (a *queryStatsStub) series(metric string, ts ...time.Time) storage.SeriesSet {
	if a.err != nil {
		return storage.ErrSeriesSet(a.err)
	}
	samples := make([]chunks.Sample, 0, len(ts))
	for _, t := range ts {
		samples = append(samples, promapi.FloatSample(t.UnixMilli(), 1))
	}
	s := promapi.NewSeries(labels.FromStrings(model.MetricNameLabel, metric, "group", a.name), samples)
	return promapi.NewSeriesSet([]storage.Series{s}, nil, nil)
}

func (a *queryStatsStub) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	if a.enter != nil {
		a.enter()
	}
	return a.series("pushed", ts)
}

func (a *queryStatsStub) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	if a.enter != nil {
		a.enter()
	}
	return a.series("pushed", r.Start, r.End)
}

func (a *queryStatsStub) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	if a.enter != nil {
		a.enter()
	}
	metric := "unknown"
	for _, m := range matchers {
		if m.Name == model.MetricNameLabel && m.Type == labels.MatchEqual {
			metric = m.Value
		}
	}
	return a.series(metric, end.Add(-time.Minute), end.Add(-30*time.Second))
}

type queryStatsSetup struct {
	ps  *ProxyStorage
	eng *promql.Engine
}

// newQueryStatsSetup puts a promclient.QueryStatsAPI in front of each stub,
// where a real ServerGroup has it, and wires the groups into a ProxyStorage
// with cross-group dedup on.
func newQueryStatsSetup(t *testing.T, apis []promclient.API, exact, partial bool) *queryStatsSetup {
	t.Helper()
	backends := make([]promclient.CrossGroupBackend, len(apis))
	for i, api := range apis {
		backends[i] = promclient.CrossGroupBackend{
			API:    &promclient.QueryStatsAPI{API: api, Ordinal: i},
			Name:   queryStatsGroupName(i),
			Labels: model.LabelSet{"backend": model.LabelValue(queryStatsGroupName(i))},
		}
	}
	client, err := promclient.NewCrossGroupMultiAPI(backends, promclient.CrossGroupOpts{PartialResponse: partial})
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}

	ps := &ProxyStorage{}
	ps.state.Store(&proxyStorageState{
		client: client,
		sgs:    newServerGroups(len(apis)),
		cfg: &proxyconfig.Config{
			ProxeusConfig: proxyconfig.ProxeusConfig{
				CrossGroupDedup:           true,
				CrossGroupExact:           exact,
				CrossGroupPartialResponse: partial,
			},
		},
	})
	eng := promql.NewEngine(promql.EngineOpts{
		MaxSamples:    1e6,
		Timeout:       10 * time.Second,
		LookbackDelta: 5 * time.Minute,
	})
	eng.NodeReplacer = ps.NodeReplacer
	return &queryStatsSetup{ps: ps, eng: eng}
}

func queryStatsGroupName(i int) string { return string(rune('a' + i)) }

func newQueryStatsRecorder(n int) *querystats.Recorder {
	groups := make([]querystats.Group, n)
	for i := range groups {
		groups[i] = querystats.Group{Name: queryStatsGroupName(i), Ordinal: i, Labels: map[string]string{"backend": queryStatsGroupName(i)}}
	}
	return querystats.NewRecorder(groups)
}

func (s *queryStatsSetup) run(t *testing.T, rec *querystats.Recorder, expr string) *promql.Result {
	t.Helper()
	q, err := s.eng.NewInstantQuery(context.Background(), s.ps, nil, expr, queryStatsNow)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	res := q.Exec(querystats.NewContext(context.Background(), rec))
	if res.Err != nil {
		t.Fatalf("%s: %v", expr, res.Err)
	}
	return res
}

func queryStatsStubs(n int) []promclient.API {
	apis := make([]promclient.API, n)
	for i := range apis {
		apis[i] = &queryStatsStub{name: queryStatsGroupName(i)}
	}
	return apis
}

// requireQueryStatsTotals checks the AC-4 invariant: each group's totals are
// the sums over its requests (no truncation at these sizes).
func requireQueryStatsTotals(t *testing.T, st querystats.Stats) {
	t.Helper()
	if st.RequestsTruncated != 0 {
		t.Fatalf("unexpected truncation: %d", st.RequestsTruncated)
	}
	for _, g := range st.Groups {
		var requests, errs int
		var series, samples int64
		for _, r := range st.Requests {
			if r.Group != g.Name {
				continue
			}
			requests++
			series += r.Series
			samples += r.Samples
			if r.Status == querystats.StatusError {
				errs++
			}
		}
		if g.Requests != requests || g.Errors != errs || g.Series != series || g.Samples != samples {
			t.Fatalf("group %s totals %+v do not match its requests (n=%d errors=%d series=%d samples=%d)", g.Name, g, requests, errs, series, samples)
		}
	}
}

func queryStatsRequestsOf(st querystats.Stats, group string) []querystats.RequestStats {
	var out []querystats.RequestStats
	for _, r := range st.Requests {
		if r.Group == group {
			out = append(out, r)
		}
	}
	return out
}

func TestQueryStatsEngine_PushableQueryIsPathPushdown(t *testing.T) {
	s := newQueryStatsSetup(t, queryStatsStubs(2), false, false)
	rec := newQueryStatsRecorder(2)

	s.run(t, rec, "sum(x)")

	st := rec.Stats()
	for _, g := range st.Groups {
		if g.Requests < 1 || g.Series < 1 || g.Samples < 1 {
			t.Fatalf("group %s got no traffic recorded: %+v", g.Name, g)
		}
	}
	for _, r := range st.Requests {
		if r.Path != querystats.PathPushdown {
			t.Fatalf("request %+v: path %q, want %q", r, r.Path, querystats.PathPushdown)
		}
		if r.Op != "query" || !strings.Contains(r.Query, "x") {
			t.Fatalf("request %+v: want op query with the pushed expression", r)
		}
	}
	requireQueryStatsTotals(t, st)
}

func TestQueryStatsEngine_ExactModeFetchesArePathRaw(t *testing.T) {
	s := newQueryStatsSetup(t, queryStatsStubs(2), true, false)
	rec := newQueryStatsRecorder(2)

	s.run(t, rec, "sum(x)")

	st := rec.Stats()
	for _, g := range st.Groups {
		if len(queryStatsRequestsOf(st, g.Name)) < 1 {
			t.Fatalf("group %s has no request recorded", g.Name)
		}
	}
	for _, r := range st.Requests {
		if r.Path != querystats.PathRaw || r.Op != "get_value" {
			t.Fatalf("request %+v: want path raw, op get_value", r)
		}
		if !strings.Contains(r.Query, `__name__="x"`) {
			t.Fatalf("request %+v: selector text should contain __name__=\"x\"", r)
		}
	}
	requireQueryStatsTotals(t, st)
}

func TestQueryStatsEngine_GroupFilteredByLabelMatcherStillListsRequests(t *testing.T) {
	// Real groups filter on their labels below the stats seam, so the stats
	// decorator wraps the label client here too.
	apis := make([]promclient.API, 2)
	for i := range apis {
		apis[i] = &promclient.AddLabelClient{
			API:    &queryStatsStub{name: queryStatsGroupName(i)},
			Labels: model.LabelSet{"backend": model.LabelValue(queryStatsGroupName(i))},
		}
	}

	for _, tc := range []struct {
		name  string
		exact bool
		path  string
	}{
		{name: "pushdown", path: querystats.PathPushdown},
		{name: "raw", exact: true, path: querystats.PathRaw},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newQueryStatsSetup(t, apis, tc.exact, false)
			rec := newQueryStatsRecorder(2)

			s.run(t, rec, `sum(x{backend="a"})`)

			st := rec.Stats()
			a, b := st.Groups[0], st.Groups[1]
			if a.Requests < 1 || a.Series < 1 {
				t.Fatalf("matching group a: %+v, want requests>=1 and series>=1", a)
			}
			if b.Requests < 1 || b.Series != 0 || b.Samples != 0 {
				t.Fatalf("filtered group b: %+v, want requests>=1 with series=0", b)
			}
			for _, r := range queryStatsRequestsOf(st, "b") {
				if r.Path != tc.path || r.Status != querystats.StatusSuccess {
					t.Fatalf("filtered group request %+v: want path %s, success", r, tc.path)
				}
			}
			requireQueryStatsTotals(t, st)
		})
	}
}

func TestQueryStatsEngine_FailingGroupWithPartialResponse(t *testing.T) {
	apis := queryStatsStubs(2)
	apis[1].(*queryStatsStub).err = errors.New("backend b exploded")
	s := newQueryStatsSetup(t, apis, false, true)
	rec := newQueryStatsRecorder(2)

	res := s.run(t, rec, "sum(x)")

	if len(res.Warnings.AsErrors()) == 0 {
		t.Fatal("expected a partial-response warning on the result")
	}
	st := rec.Stats()
	a, b := st.Groups[0], st.Groups[1]
	if a.Errors != 0 || a.Series < 1 {
		t.Fatalf("healthy group a: %+v", a)
	}
	if b.Errors != 1 {
		t.Fatalf("failing group b: %+v, want errors=1", b)
	}
	failed := queryStatsRequestsOf(st, "b")
	if len(failed) != 1 || failed[0].Status != querystats.StatusError || !strings.Contains(failed[0].Error, "backend b exploded") {
		t.Fatalf("failing group requests: %+v", failed)
	}
	requireQueryStatsTotals(t, st)
}

func TestQueryStatsEngine_ParallelQueriesDoNotMixStats(t *testing.T) {
	metrics := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	const groups = 2

	for _, exact := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact=%t", exact), func(t *testing.T) {
			// Hold every group call until each query has reached each group,
			// so all recorders are in use at the same time.
			var arrived atomic.Int32
			gate := make(chan struct{})
			enter := func() {
				if int(arrived.Add(1)) == len(metrics)*groups {
					close(gate)
				}
				select {
				case <-gate:
				case <-time.After(10 * time.Second):
				}
			}
			apis := make([]promclient.API, groups)
			for i := range apis {
				apis[i] = &queryStatsStub{name: queryStatsGroupName(i), enter: enter}
			}
			s := newQueryStatsSetup(t, apis, exact, false)

			recs := make([]*querystats.Recorder, len(metrics))
			errs := make([]error, len(metrics))
			var wg sync.WaitGroup
			for i, m := range metrics {
				recs[i] = newQueryStatsRecorder(groups)
				wg.Add(1)
				go func() {
					defer wg.Done()
					q, err := s.eng.NewInstantQuery(context.Background(), s.ps, nil, fmt.Sprintf("sum(%s)", m), queryStatsNow)
					if err != nil {
						errs[i] = err
						return
					}
					defer q.Close()
					if res := q.Exec(querystats.NewContext(context.Background(), recs[i])); res.Err != nil {
						errs[i] = res.Err
					}
				}()
			}
			wg.Wait()

			for i, m := range metrics {
				if errs[i] != nil {
					t.Fatalf("%s: %v", m, errs[i])
				}
				st := recs[i].Stats()
				if len(st.Requests) < groups {
					t.Fatalf("%s: %d requests recorded, want at least one per group", m, len(st.Requests))
				}
				for _, r := range st.Requests {
					if !strings.Contains(r.Query, m) {
						t.Fatalf("recorder of %q holds a foreign request: %+v", m, r)
					}
					for _, other := range metrics {
						if other != m && strings.Contains(r.Query, other) {
							t.Fatalf("recorder of %q holds a request for %q: %+v", m, other, r)
						}
					}
				}
				requireQueryStatsTotals(t, st)
			}
		})
	}
}
