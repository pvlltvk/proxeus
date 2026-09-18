package proxystorage

import (
	"context"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promclient"
)

// newExactAggregatesStorage builds a ProxyStorage with n server_groups and
// cross_group_exact_aggregates set as given.
func newExactAggregatesStorage(client promclient.API, n int, exact bool) *ProxyStorage {
	ps := &ProxyStorage{}
	ps.state.Store(&proxyStorageState{
		client: client,
		sgs:    newServerGroups(n),
		cfg: &proxyconfig.Config{
			ProxeusConfig: proxyconfig.ProxeusConfig{
				CrossGroupDedup:           exact,
				CrossGroupExactAggregates: exact,
			},
		},
	})
	return ps
}

// TestNodeReplacerExactAggregates pins the pushdown decision for
// cross_group_exact_aggregates: with more than one server_group the
// aggregation is not pushed down (so the engine computes it over deduped raw
// series), with one server_group it still is.
func TestNodeReplacerExactAggregates(t *testing.T) {
	// One expression per branch of the aggregation switch, plus the
	// AggregateExpr-under-BinaryExpr path (node label "binary").
	tests := []struct {
		expr string
		node string
	}{
		{expr: "sum(foo)", node: "aggregate"},
		{expr: "min(foo)", node: "aggregate"},
		{expr: "max(foo)", node: "aggregate"},
		{expr: "topk(1, foo)", node: "aggregate"},
		{expr: "bottomk(1, foo)", node: "aggregate"},
		{expr: "group(foo)", node: "aggregate"},
		{expr: "avg(foo)", node: "aggregate"},
		{expr: "count(foo)", node: "aggregate"},
		{expr: `count_values("v", foo)`, node: "aggregate"},
		{expr: "min(foo) > 1", node: "binary"},
	}

	now := time.Unix(10000, 0)
	replace := func(t *testing.T, ps *ProxyStorage, expr string) parser.Node {
		t.Helper()
		parsed, err := parser.ParseExpr(expr)
		if err != nil {
			t.Fatal(err)
		}
		node, err := ps.NodeReplacer(context.Background(), &parser.EvalStmt{Expr: parsed, Start: now, End: now}, parsed, nil)
		if err != nil {
			t.Fatal(err)
		}
		return node
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			api := &stubAPI{}

			deltas := counterDeltas(t, pushdownNodes.WithLabelValues(test.node, resultFallback, reasonExactAggregates))
			if node := replace(t, newExactAggregatesStorage(api, 2, true), test.expr); node != nil {
				t.Fatalf("exact aggregates, 2 server_groups: pushed down as %s, want no replacement", node)
			}
			if got := deltas()[0]; got != 1 {
				t.Fatalf("pushdown_nodes{node=%s,result=%s,reason=%s}: got %v, want 1", test.node, resultFallback, reasonExactAggregates, got)
			}
			if queries := api.getQueries(); len(queries) != 0 {
				t.Fatalf("exact aggregates sent queries downstream: %v", queries)
			}

			if node := replace(t, newExactAggregatesStorage(api, 2, false), test.expr); node == nil {
				t.Fatal("flag off, 2 server_groups: no replacement, want pushdown")
			}
			api.getQueries()

			if node := replace(t, newExactAggregatesStorage(api, 1, true), test.expr); node == nil {
				t.Fatal("exact aggregates, 1 server_group: no replacement, want pushdown")
			}
		})
	}
}

// overlapStub is one server_group holding a single `up` series carrying that
// group's external label. Any query is answered with it: as a raw selector
// fetch it is the series itself, and as a pushed-down `count(up)` it is that
// group's partial -- value 1, the count of the one series it holds.
type overlapStub struct {
	stubAPI
	group string
	ts    time.Time
}

func (a *overlapStub) set() storage.SeriesSet {
	s := promapi.NewSeries(
		labels.FromStrings(model.MetricNameLabel, "up", "instance", "i1", "backend", a.group),
		[]chunks.Sample{promapi.FloatSample(a.ts.UnixMilli(), 1)},
	)
	return promapi.NewSeriesSet([]storage.Series{s}, nil, nil)
}

func (a *overlapStub) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return a.set()
}

func (a *overlapStub) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	return a.set()
}

func (a *overlapStub) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	return a.set()
}

// TestExactAggregates_E2E_CountOverlap is the acceptance property: one series
// present in two overlapping server_groups must be counted once. Without the
// flag each group answers `count(up)` with its own partial and the engine adds
// them up to 2 -- the double count cross_group_dedup cannot fix, since it only
// sees the partials. With the flag the aggregation is not pushed down, the raw
// fan-out goes through cross-group dedup, and count sees the one series.
func TestExactAggregates_E2E_CountOverlap(t *testing.T) {
	now := time.Unix(10000, 0)

	count := func(t *testing.T, exact bool) float64 {
		t.Helper()
		client, err := promclient.NewCrossGroupMultiAPI([]promclient.CrossGroupBackend{
			{API: &overlapStub{group: "sg0", ts: now}, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
			{API: &overlapStub{group: "sg1", ts: now}, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
		}, promclient.CrossGroupOpts{})
		if err != nil {
			t.Fatalf("NewCrossGroupMultiAPI: %v", err)
		}

		ps := newExactAggregatesStorage(client, 2, exact)
		eng := promql.NewEngine(promql.EngineOpts{
			MaxSamples:    1e6,
			Timeout:       10 * time.Second,
			LookbackDelta: 5 * time.Minute,
		})
		eng.NodeReplacer = ps.NodeReplacer

		q, err := eng.NewInstantQuery(context.Background(), ps, nil, "count(up)", now)
		if err != nil {
			t.Fatal(err)
		}
		res := q.Exec(context.Background())
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		vec, err := res.Vector()
		if err != nil {
			t.Fatal(err)
		}
		if len(vec) != 1 {
			t.Fatalf("count(up) returned %d samples, want 1: %v", len(vec), vec)
		}
		return vec[0].F
	}

	if got := count(t, true); got != 1 {
		t.Fatalf("with cross_group_exact_aggregates: count(up) = %v, want 1", got)
	}
	// Control: the documented double count, proving the flag is what fixes it.
	if got := count(t, false); got != 2 {
		t.Fatalf("without cross_group_exact_aggregates: count(up) = %v, want the 2 of the union", got)
	}
}
