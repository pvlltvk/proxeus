package proxystorage

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promclient"
	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// NodeReplacer's observable contract is two things at once: the
// proxeus_pushdown_nodes_total{node,result,reason} series a query produces, and
// the downstream requests it issues (query string, window, and whether the
// context carries the aggregate-pushdown marker that decides union-vs-dedup).
// TestNodeReplacer in proxy_test.go pins the replacement tree for the shapes
// that push down; these tests pin both halves of the contract for the guards
// and bail-outs, where the tree is unchanged and the decision is all there is
// to observe.
//
// Every expectation below is driven through parser.Inspect rather than a single
// NodeReplacer call, so the path-dependent guards (subquery_child,
// matrix_parent) and the revisit suppression see a real walk.

// gatherPushdownNodes reads proxeus_pushdown_nodes_total keyed as
// "node/result/reason".
func gatherPushdownNodes(t *testing.T) map[string]float64 {
	t.Helper()
	out := map[string]float64{}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "proxeus_pushdown_nodes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			lbls := map[string]string{}
			for _, l := range m.GetLabel() {
				lbls[l.GetName()] = l.GetValue()
			}
			out[fmt.Sprintf("%s/%s/%s", lbls["node"], lbls["result"], lbls["reason"])] = m.GetCounter().GetValue()
		}
	}
	return out
}

// pushdownDecisions snapshots the decision counters and returns a func
// reporting which ones moved, as sorted "node/result/reason=n" strings. The
// metrics are package-global, so every assertion is on a delta.
func pushdownDecisions(t *testing.T) func() []string {
	t.Helper()
	before := gatherPushdownNodes(t)
	return func() []string {
		out := []string{}
		for k, v := range gatherPushdownNodes(t) {
			if d := v - before[k]; d != 0 {
				out = append(out, fmt.Sprintf("%s=%g", k, d))
			}
		}
		sort.Strings(out)
		return out
	}
}

// recordingAPI records every downstream request with its window and whether the
// context was marked by promclient.WithAggregatePushdown. `response` picks what
// comes back: a float sample, a native histogram (so the lossy_histogram guard
// fires), or an error.
type recordingAPI struct {
	stubAPI
	response string // "float" (default), "histogram", "error"
	calls    []string
}

func (a *recordingAPI) result() storage.SeriesSet {
	switch a.response {
	case "error":
		return promapi.NewSeriesSet(nil, nil, fmt.Errorf("downstream boom"))
	case "histogram":
		h := &histogram.FloatHistogram{Count: 3, Sum: 6, ZeroThreshold: 0.001}
		return promapi.NewSeriesSet([]storage.Series{promapi.NewSeries(
			labels.FromStrings(model.MetricNameLabel, "foo"),
			[]chunks.Sample{promapi.HistogramSample(decisionNowMs, h)},
		)}, nil, nil)
	default:
		return promapi.NewSeriesSet([]storage.Series{promapi.NewSeries(
			labels.FromStrings(model.MetricNameLabel, "foo"),
			[]chunks.Sample{promapi.FloatSample(decisionNowMs, 1)},
		)}, nil, nil)
	}
}

func (a *recordingAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	a.calls = append(a.calls, fmt.Sprintf("Query[agg=%v] %s @ %d",
		promclient.IsAggregatePushdown(ctx), query, ts.UnixMilli()))
	return a.result()
}

func (a *recordingAPI) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	a.calls = append(a.calls, fmt.Sprintf("QueryRange[agg=%v] %s @ %d to %d step %s",
		promclient.IsAggregatePushdown(ctx), query, r.Start.UnixMilli(), r.End.UnixMilli(), r.Step))
	return a.result()
}

func (a *recordingAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	a.calls = append(a.calls, "GetValue")
	return a.result()
}

func (a *recordingAPI) take() []string {
	c := a.calls
	a.calls = nil
	if c == nil {
		c = []string{}
	}
	return c
}

const (
	decisionNow   = int64(1700000000) // seconds
	decisionNowMs = decisionNow * 1000
)

// decisionSGs builds n server_groups with remote_read on, so the strict
// native-histogram fidelity check stays out of the way.
func decisionSGs(n int) []*servergroup.ServerGroup {
	sgs := make([]*servergroup.ServerGroup, n)
	for i := range sgs {
		sgs[i] = &servergroup.ServerGroup{Cfg: &servergroup.Config{Ordinal: i, RemoteRead: true}}
	}
	return sgs
}

func newDecisionStorage(client promclient.API, sgs []*servergroup.ServerGroup) *ProxyStorage {
	ps := &ProxyStorage{
		NoStepSubqueryIntervalFn: func(rangeMillis int64) int64 { return time.Minute.Milliseconds() },
	}
	ps.state.Store(&proxyStorageState{client: client, sgs: sgs, cfg: &proxyconfig.Config{}})
	return ps
}

// inspectDecisions walks expr the way the engine does and reports the decisions
// recorded plus the downstream requests issued.
func inspectDecisions(t *testing.T, ps *ProxyStorage, api *recordingAPI, expr parser.Expr, stmt *parser.EvalStmt) ([]string, []string, error) {
	t.Helper()
	deltas := pushdownDecisions(t)
	_, err := parser.Inspect(context.Background(), stmt,
		func(parser.Node, []parser.Node) error { return nil }, ps.NodeReplacer)
	return deltas(), api.take(), err
}

func TestNodeReplacerDecisions(t *testing.T) {
	tests := []struct {
		expr      string
		sgs       int
		interval  time.Duration
		response  string
		decisions []string
		calls     []string
	}{
		// --- prepare() guards -------------------------------------------
		// An aggregation below us has its own combining rules.
		{
			expr:      "sum(sum(foo))",
			decisions: []string{"aggregate/fallback/nested_aggregate=1", "aggregate/pushed/=1"},
			calls:     []string{"Query[agg=true] sum(foo) @ 1700000000000"},
		},
		// A selector directly under a MatrixSelector: nothing smaller to ask
		// for, and neither node issues a request.
		{
			expr:      "foo[5m]",
			decisions: []string{"matrix_selector/fallback/unsupported=1", "vector_selector/fallback/matrix_parent=1"},
			calls:     []string{},
		},
		// Below a subquery the replacement already happened on the inner
		// EvalStmt, so the outer walk must not re-decide (or re-request).
		{
			expr: "rate(foo[5m])[10m:1m]",
			decisions: []string{
				"call/pushed/=1", "subquery/pushed/=1", "vector_selector/fallback/subquery_child=1",
			},
			calls: []string{"QueryRange[agg=false] rate(foo[5m]) @ 1699999440000 to 1700000000000 step 1m0s"},
		},
		// No VectorSelector at all -> the offset never converges.
		{
			expr:      "time()",
			decisions: []string{"call/fallback/offset_mismatch=1"},
			calls:     []string{},
		},
		// Two selectors may not live on the same backend, so the binary node
		// is declined -- but each selector below it is still pushed.
		{
			expr:      "foo > bar",
			decisions: []string{"binary/fallback/multi_vector_selector=1", "vector_selector/pushed/=2"},
			calls: []string{
				"Query[agg=false] foo @ 1700000000000",
				"Query[agg=false] bar @ 1700000000000",
			},
		},

		// --- replaceBinary ----------------------------------------------
		// vector(1) is not a literal, so there is no literal side to pair the
		// selector with.
		{
			expr: "foo > vector(1)",
			decisions: []string{
				"binary/fallback/no_literal_operand=1", "call/fallback/offset_mismatch=1",
				"vector_selector/pushed/=1",
			},
			calls: []string{"Query[agg=false] foo @ 1700000000000"},
		},
		// A literal side whose partner is a non-reentrant aggregate.
		{
			expr:      "avg(foo) > 1",
			decisions: []string{"aggregate/pushed/=3", "binary/fallback/unsupported_operand=1"},
			calls: []string{
				"Query[agg=true] sum(foo) @ 1700000000000",
				"Query[agg=true] count(foo) @ 1700000000000",
			},
		},

		// --- replaceSubquery --------------------------------------------
		// Step 0 takes NoStepSubqueryIntervalFn for the inner interval.
		{
			expr: "rate(foo[1m])[10m:]",
			decisions: []string{
				"call/pushed/=1", "subquery/pushed/=1", "vector_selector/fallback/subquery_child=1",
			},
			calls: []string{"QueryRange[agg=false] rate(foo[1m]) @ 1699999440000 to 1700000000000 step 1m0s"},
		},
		// An @ on the subquery pins the inner window to the @ time instead of
		// the request's; here the whole call goes down in one piece.
		{
			expr:      "sum_over_time(foo[10m:1m] @ 100)",
			decisions: []string{"call/pushed/=1"},
			calls:     []string{`Query[agg=false] sum_over_time(foo[10m:1m] @ 100.000) @ 1700000000000`},
		},

		// --- replaceAggregate: the avg rewrites -------------------------
		// __name__ in the grouping needs the preserve-label workaround, since
		// the downstream aggregation drops the metric name.
		{
			expr: "avg by (__name__, instance) (foo)",
			decisions: []string{
				"aggregate/pushed/=3", "binary/fallback/nested_aggregate=1",
				"call/fallback/nested_aggregate=1",
			},
			calls: []string{
				`Query[agg=true] sum by (__name, instance) (label_replace(foo, "__name", "$1", "__name__", "(.*)")) @ 1700000000000`,
				`Query[agg=true] count by (__name, instance) (label_replace(foo, "__name", "$1", "__name__", "(.*)")) @ 1700000000000`,
			},
		},
		// The same workaround fires for an exclusion list mentioning __name__.
		{
			expr: "avg without (__name__) (foo)",
			decisions: []string{
				"aggregate/pushed/=3", "binary/fallback/nested_aggregate=1",
				"call/fallback/nested_aggregate=1",
			},
			calls: []string{
				`Query[agg=true] sum without (__name) (label_replace(foo, "__name", "$1", "__name__", "(.*)")) @ 1700000000000`,
				`Query[agg=true] count without (__name) (label_replace(foo, "__name", "$1", "__name__", "(.*)")) @ 1700000000000`,
			},
		},

		// --- lossy_histogram, one per arm -------------------------------
		// The request goes out, the response carries a native histogram, so
		// the arm hands the node back to the engine. The retry the engine then
		// makes for the raw series is visible as the second request.
		{
			expr:      "sum(foo)",
			response:  "histogram",
			decisions: []string{"aggregate/fallback/lossy_histogram=1", "vector_selector/fallback/lossy_histogram=1"},
			calls: []string{
				"Query[agg=true] sum(foo) @ 1700000000000",
				"Query[agg=false] foo @ 1700000000000",
			},
		},
		{
			expr:     "rate(foo[1m])",
			response: "histogram",
			decisions: []string{
				"call/fallback/lossy_histogram=1", "matrix_selector/fallback/unsupported=1",
				"vector_selector/fallback/matrix_parent=1",
			},
			calls: []string{"Query[agg=false] rate(foo[1m]) @ 1700000000000"},
		},
		{
			expr:      "foo",
			response:  "histogram",
			decisions: []string{"vector_selector/fallback/lossy_histogram=1"},
			calls:     []string{"Query[agg=false] foo @ 1700000000000"},
		},
		{
			expr:      "foo > 1",
			response:  "histogram",
			decisions: []string{"binary/fallback/lossy_histogram=1", "vector_selector/fallback/lossy_histogram=1"},
			calls: []string{
				"Query[agg=false] foo > 1 @ 1700000000000",
				"Query[agg=false] foo @ 1700000000000",
			},
		},
		// The aggregate-under-binary arm: its request carries the
		// aggregate-pushdown marker, so the cross-group merge unions the
		// per-group partials instead of deduping them.
		{
			expr:     "min(foo) > 1",
			response: "histogram",
			decisions: []string{
				"aggregate/fallback/lossy_histogram=1", "binary/fallback/lossy_histogram=1",
				"vector_selector/fallback/lossy_histogram=1",
			},
			calls: []string{
				"Query[agg=true] min(foo) > 1 @ 1700000000000",
				"Query[agg=true] min(foo) @ 1700000000000",
				"Query[agg=false] foo @ 1700000000000",
			},
		},
		{
			expr:      `count_values("v", foo)`,
			response:  "histogram",
			decisions: []string{"aggregate/fallback/lossy_histogram=1", "vector_selector/fallback/lossy_histogram=1"},
			calls: []string{
				`Query[agg=true] count_values("v", foo) @ 1700000000000`,
				"Query[agg=false] foo @ 1700000000000",
			},
		},

		// --- range queries ----------------------------------------------
		// The synthesized lookback is step-1ms, and the window is shifted by
		// the offset the replacement re-applies.
		{
			expr:      "foo",
			interval:  time.Minute,
			decisions: []string{"vector_selector/pushed/=1"},
			calls:     []string{"QueryRange[agg=false] foo @ 1699999400000 to 1700000000000 step 1m0s"},
		},
		{
			expr:      "foo offset 5m",
			interval:  time.Minute,
			decisions: []string{"vector_selector/pushed/=1"},
			calls:     []string{"QueryRange[agg=false] foo @ 1699999100000 to 1699999700000 step 1m0s"},
		},
		// With an @ in the subtree the offset stays in the string and the
		// window is NOT shifted: the downstream resolves @+offset itself.
		{
			expr:      "sum(foo @ 100 offset 50s)",
			interval:  time.Minute,
			decisions: []string{"aggregate/pushed/=1"},
			calls:     []string{"QueryRange[agg=true] sum(foo @ 100.000 offset 50s) @ 1699999400000 to 1700000000000 step 1m0s"},
		},

		// --- single server_group ----------------------------------------
		// avg is reentrant against one backend, so it goes down whole rather
		// than being split into sum()/count().
		{
			expr:      "avg(foo)",
			sgs:       1,
			decisions: []string{"aggregate/pushed/=1"},
			calls:     []string{"Query[agg=true] avg(foo) @ 1700000000000"},
		},
	}

	for _, test := range tests {
		name := test.expr
		if test.interval > 0 {
			name += " (range)"
		}
		if test.response != "" {
			name += " (" + test.response + ")"
		}
		if test.sgs == 1 {
			name += " (1 server_group)"
		}
		t.Run(name, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			sgs := 2
			if test.sgs != 0 {
				sgs = test.sgs
			}
			api := &recordingAPI{response: test.response}
			ps := newDecisionStorage(api, decisionSGs(sgs))

			start, end := time.Unix(decisionNow, 0), time.Unix(decisionNow, 0)
			if test.interval > 0 {
				start = time.Unix(decisionNow-600, 0)
			}
			stmt := &parser.EvalStmt{Expr: expr, Start: start, End: end, Interval: test.interval}

			decisions, calls, err := inspectDecisions(t, ps, api, expr, stmt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.decisions, decisions); diff != "" {
				t.Errorf("decisions mismatch:\n%s", diff)
			}
			if diff := cmp.Diff(test.calls, calls); diff != "" {
				t.Errorf("downstream requests mismatch:\n%s", diff)
			}
		})
	}
}

// TestNodeReplacerDecisions_QueryRangeAt pins the instant-query optimization in
// queryRangeAt: when the subtree pins evaluation with @, every step of a range
// query has the same answer, so one instant Query at the @ time replaces a
// QueryRange -- which is what keeps a pre-epoch sub-second start off the wire,
// where prometheus/common's model.Time.UnmarshalJSON mis-decodes it. A Call
// whose result depends on the evaluation time even under @ (see
// promql.AtModifierUnsafeFunctions) must NOT take that shortcut.
func TestNodeReplacerDecisions_QueryRangeAt(t *testing.T) {
	tests := []struct {
		expr  string
		calls []string
	}{
		{
			expr:  "rate(foo[1m] @ 100)",
			calls: []string{"Query[agg=false] rate(foo[1m] @ 100.000) @ 100000"},
		},
		{
			expr:  "timestamp(foo @ 100)",
			calls: []string{"QueryRange[agg=false] timestamp(foo @ 100.000) @ -1500 to 600000 step 1m0s"},
		},
		{
			expr:  "rate(foo[1m])",
			calls: []string{"QueryRange[agg=false] rate(foo[1m]) @ -1500 to 600000 step 1m0s"},
		},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			api := &recordingAPI{}
			ps := newDecisionStorage(api, decisionSGs(2))
			stmt := &parser.EvalStmt{
				Expr:     expr,
				Start:    time.Unix(0, -1500*int64(time.Millisecond)),
				End:      time.Unix(600, 0),
				Interval: time.Minute,
			}
			decisions, calls, err := inspectDecisions(t, ps, api, expr, stmt)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff([]string{"call/pushed/=1"}, decisions); diff != "" {
				t.Errorf("decisions mismatch:\n%s", diff)
			}
			if diff := cmp.Diff(test.calls, calls); diff != "" {
				t.Errorf("downstream requests mismatch:\n%s", diff)
			}
		})
	}
}

// TestNodeReplacerDecisions_Histogram covers the histogram gate: a
// histogram-bearing subtree is declined outright (reason=histogram) when every
// group can serve it losslessly, and errors when a group can't -- before any
// request goes out. The signal is inherited from the ancestor path too, so the
// selector below a histogram-only call is declined for the same reason.
func TestNodeReplacerDecisions_Histogram(t *testing.T) {
	strict := []*servergroup.ServerGroup{
		{Cfg: &servergroup.Config{Ordinal: 0, RemoteRead: true}},
		{Cfg: &servergroup.Config{Ordinal: 1, RemoteRead: false}},
	}
	lossyAccepted := []*servergroup.ServerGroup{
		{Cfg: &servergroup.Config{Ordinal: 0, RemoteRead: false,
			NativeHistogram: servergroup.NativeHistogramConfig{AllowLossy: true}}},
	}

	tests := []struct {
		name      string
		expr      string
		sgs       []*servergroup.ServerGroup
		wantErr   bool
		decisions []string
		calls     []string
	}{
		{
			name:      "histogram-only call, lossless everywhere",
			expr:      "sum(histogram_count(foo))",
			sgs:       decisionSGs(2),
			decisions: []string{"aggregate/fallback/histogram=1", "call/fallback/histogram=1", "vector_selector/fallback/histogram=1"},
			calls:     []string{},
		},
		{
			name:      "histogram-only call, group without remote_read",
			expr:      "sum(histogram_count(foo))",
			sgs:       strict,
			wantErr:   true,
			decisions: []string{},
			calls:     []string{},
		},
		{
			name:      "histogram-only call, fidelity loss accepted",
			expr:      "sum(histogram_count(foo))",
			sgs:       lossyAccepted,
			decisions: []string{"aggregate/fallback/histogram=1", "call/fallback/histogram=1", "vector_selector/fallback/histogram=1"},
			calls:     []string{},
		},
		{
			// histogram_quantile returns floats, so the response is faithful
			// over the JSON API and the pushdown stands even under strict.
			name:      "histogram_quantile pushes down",
			expr:      "histogram_quantile(0.5, foo)",
			sgs:       strict,
			decisions: []string{"call/pushed/=1"},
			calls:     []string{"Query[agg=false] histogram_quantile(0.5, foo) @ 1700000000000"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}
			api := &recordingAPI{}
			ps := newDecisionStorage(api, test.sgs)
			now := time.Unix(decisionNow, 0)
			decisions, calls, err := inspectDecisions(t, ps, api,
				expr, &parser.EvalStmt{Expr: expr, Start: now, End: now})
			if test.wantErr {
				if err == nil {
					t.Fatal("expected a native-histogram fidelity error, got nil")
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.decisions, decisions); diff != "" {
				t.Errorf("decisions mismatch:\n%s", diff)
			}
			if diff := cmp.Diff(test.calls, calls); diff != "" {
				t.Errorf("downstream requests mismatch:\n%s", diff)
			}
		})
	}
}

// TestNodeReplacerDecisions_RevisitAggregate is the AggregateExpr counterpart
// of TestPushdownMetrics_Revisit: parser.Walk descends into a replacement we
// already made, and that visit is not a decision -- no counter, no request.
func TestNodeReplacerDecisions_RevisitAggregate(t *testing.T) {
	api := &recordingAPI{}
	ps := newDecisionStorage(api, decisionSGs(2))
	agg := &parser.AggregateExpr{
		Op:   parser.SUM,
		Expr: &parser.VectorSelector{Name: "foo", UnexpandedSeriesSet: api.result()},
	}

	now := time.Unix(decisionNow, 0)
	deltas := pushdownDecisions(t)
	node, err := ps.NodeReplacer(context.Background(),
		&parser.EvalStmt{Expr: agg, Start: now, End: now}, agg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if node != nil {
		t.Fatalf("expected no replacement, got %s", node)
	}
	if got := deltas(); len(got) != 0 {
		t.Errorf("re-visit counted as a decision: %v", got)
	}
	if got := api.take(); len(got) != 0 {
		t.Errorf("re-visit issued downstream requests: %v", got)
	}
}

// TestNodeReplacerDecisions_WalkError checks the subtree walk's error is
// returned before any guard runs, and that an error is never counted as a
// decision.
func TestNodeReplacerDecisions_WalkError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, exprStr := range []string{"sum(foo)", "foo", "rate(foo[1m])"} {
		t.Run(exprStr, func(t *testing.T) {
			expr, err := parser.ParseExpr(exprStr)
			if err != nil {
				t.Fatal(err)
			}
			api := &recordingAPI{}
			ps := newDecisionStorage(api, decisionSGs(2))
			now := time.Unix(decisionNow, 0)

			deltas := pushdownDecisions(t)
			node, err := ps.NodeReplacer(ctx, &parser.EvalStmt{Expr: expr, Start: now, End: now}, expr, nil)
			if err == nil {
				t.Fatal("expected the canceled context to surface")
			}
			if node != nil {
				t.Errorf("expected no replacement, got %s", node)
			}
			if got := deltas(); len(got) != 0 {
				t.Errorf("error counted as a decision: %v", got)
			}
			if got := api.take(); len(got) != 0 {
				t.Errorf("requests issued after the walk failed: %v", got)
			}
		})
	}
}

// TestNodeReplacerDecisions_HandBuiltAggregations covers the aggregation arms
// the PromQL parser won't produce for us: count_values with a non-literal
// value-label parameter (the parser requires a string there), and the
// experimental limitk / limit_ratio ops.
func TestNodeReplacerDecisions_HandBuiltAggregations(t *testing.T) {
	tests := []struct {
		name   string
		op     parser.ItemType
		reason string
	}{
		{name: "count_values non-literal param", op: parser.COUNT_VALUES, reason: reasonNonLiteralParam},
		{name: "limitk", op: parser.LIMITK, reason: reasonNonReentrantAgg},
		{name: "limit_ratio", op: parser.LIMIT_RATIO, reason: reasonNonReentrantAgg},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			agg := &parser.AggregateExpr{
				Op:    test.op,
				Expr:  &parser.VectorSelector{Name: "foo"},
				Param: &parser.NumberLiteral{Val: 1},
			}
			api := &recordingAPI{}
			ps := newDecisionStorage(api, decisionSGs(2))
			now := time.Unix(decisionNow, 0)

			deltas := pushdownDecisions(t)
			node, err := ps.NodeReplacer(context.Background(),
				&parser.EvalStmt{Expr: agg, Start: now, End: now}, agg, nil)
			if err != nil {
				t.Fatal(err)
			}
			if node != nil {
				t.Fatalf("expected no replacement, got %s", node)
			}
			want := []string{fmt.Sprintf("aggregate/%s/%s=1", resultFallback, test.reason)}
			if diff := cmp.Diff(want, deltas()); diff != "" {
				t.Errorf("decisions mismatch:\n%s", diff)
			}
			if got := api.take(); len(got) != 0 {
				t.Errorf("unexpected downstream requests: %v", got)
			}
		})
	}
}
