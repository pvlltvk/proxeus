package proxystorage

import (
	"context"
	"strconv"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/pvlltvk/proxeus/pkg/promapi"
)

// dataStub answers every query/fetch with 2 series of 3 samples, so the
// series/sample counters have something to count.
type dataStub struct{ stubAPI }

func (a *dataStub) set() storage.SeriesSet {
	var out []storage.Series
	for _, inst := range []string{"a", "b"} {
		samples := []chunks.Sample{
			promapi.FloatSample(1000, 1),
			promapi.FloatSample(2000, 2),
			promapi.FloatSample(3000, 3),
		}
		out = append(out, promapi.NewSeries(labels.FromStrings(model.MetricNameLabel, "foo", "instance", inst), samples))
	}
	return promapi.NewSeriesSet(out, nil, nil)
}

func (a *dataStub) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return a.set()
}

func (a *dataStub) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	return a.set()
}

func (a *dataStub) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	return a.set()
}

// counterDeltas snapshots counters and returns a func reporting how much each
// one moved. The metrics are package-global, so every assertion here is on a
// delta rather than an absolute value.
func counterDeltas(t *testing.T, counters ...prometheus.Counter) func() []float64 {
	t.Helper()
	before := make([]float64, len(counters))
	for i, c := range counters {
		before[i] = testutil.ToFloat64(c)
	}
	return func() []float64 {
		deltas := make([]float64, len(counters))
		for i, c := range counters {
			deltas[i] = testutil.ToFloat64(c) - before[i]
		}
		return deltas
	}
}

func drain(t *testing.T, ss storage.SeriesSet) {
	t.Helper()
	for ss.Next() {
		it := ss.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone { //nolint: revive
		}
		if err := it.Err(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("series set error: %v", err)
	}
}

func TestPushdownMetrics(t *testing.T) {
	tests := []struct {
		expr   string
		node   string
		result string
		reason string
	}{
		// Reentrant aggregation: answered by the backends.
		{expr: "sum(foo)", node: "aggregate", result: resultPushed},
		// avg is rewritten into sum/count, which the engine pushes down on
		// the next pass -- still a replacement, so still "pushed".
		{expr: "avg(foo)", node: "aggregate", result: resultPushed},
		// Not reentrant: the engine has to see every series.
		{expr: "stddev(foo)", node: "aggregate", result: resultFallback, reason: reasonNonReentrantAgg},
		// Two selectors that may not live on the same backend.
		{expr: "rate(a[5m]) / rate(b[5m])", node: "binary", result: resultFallback, reason: reasonMultiVectorSelector},
		// Evaluated by the engine, never pushed.
		{expr: "absent(foo)", node: "call", result: resultFallback, reason: reasonUnsupportedFunc},
		// A bare range selector is already the smallest thing to ask for.
		{expr: "foo[5m]", node: "matrix_selector", result: resultFallback, reason: reasonUnsupported},
	}

	for _, test := range tests {
		t.Run(test.expr, func(t *testing.T) {
			ps, _ := newProxyStorage(t, &dataStub{})
			expr, err := parser.ParseExpr(test.expr)
			if err != nil {
				t.Fatal(err)
			}

			now := time.Unix(10, 0)
			deltas := counterDeltas(t, pushdownNodes.WithLabelValues(test.node, test.result, test.reason))
			if _, err := ps.NodeReplacer(context.Background(), &parser.EvalStmt{Expr: expr, Start: now, End: now}, expr, nil); err != nil {
				t.Fatal(err)
			}
			if got := deltas()[0]; got != 1 {
				t.Fatalf("pushdown_nodes{node=%s,result=%s,reason=%s}: got %v, want 1", test.node, test.result, test.reason, got)
			}
		})
	}
}

// TestPushdownMetrics_Revisit checks we don't count the re-visit of a node we
// already replaced (parser.Walk descends into the replacement) as a fallback.
func TestPushdownMetrics_Revisit(t *testing.T) {
	ps, _ := newProxyStorage(t, &dataStub{})
	vs := &parser.VectorSelector{
		Name:                "foo",
		UnexpandedSeriesSet: (&dataStub{}).set(),
	}

	now := time.Unix(10, 0)
	deltas := counterDeltas(t,
		pushdownNodes.WithLabelValues("vector_selector", resultFallback, reasonUnsupported),
		pushdownNodes.WithLabelValues("vector_selector", resultPushed, ""),
	)
	if _, err := ps.NodeReplacer(context.Background(), &parser.EvalStmt{Expr: vs, Start: now, End: now}, vs, nil); err != nil {
		t.Fatal(err)
	}
	if got := deltas(); got[0] != 0 || got[1] != 0 {
		t.Fatalf("re-visit counted: fallback=%v pushed=%v, want 0/0", got[0], got[1])
	}
}

// TestPushdownSeriesMetrics checks the pushdown fetch counts what the backend
// returned: 2 series of 3 samples.
func TestPushdownSeriesMetrics(t *testing.T) {
	ps, _ := newProxyStorage(t, &dataStub{})
	expr, err := parser.ParseExpr("sum(foo)")
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(10, 0)
	deltas := counterDeltas(t,
		backendSeries.WithLabelValues(pathPushdown),
		backendSamples.WithLabelValues(pathPushdown),
	)
	if _, err := ps.NodeReplacer(context.Background(), &parser.EvalStmt{Expr: expr, Start: now, End: now}, expr, nil); err != nil {
		t.Fatal(err)
	}
	// containsLossyHistogram drains the response, so no further reading is
	// needed for the counters to have moved.
	if got := deltas(); got[0] != 2 || got[1] != 6 {
		t.Fatalf("pushdown series/samples: got %v/%v, want 2/6", got[0], got[1])
	}
}

// TestRawFetchMetrics covers the local-evaluation path: Querier.Select hits
// the backends through GetValue.
func TestRawFetchMetrics(t *testing.T) {
	ps, _ := newProxyStorage(t, &dataStub{})
	q, err := ps.Querier(0, 10000)
	if err != nil {
		t.Fatal(err)
	}

	deltas := counterDeltas(t,
		rawSeriesFetches,
		backendSeries.WithLabelValues(pathRaw),
		backendSamples.WithLabelValues(pathRaw),
	)
	hints := &storage.SelectHints{Start: 0, End: 10000, Func: "rate"}
	drain(t, q.Select(context.Background(), false, hints, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")))

	if got := deltas(); got[0] != 1 || got[1] != 2 || got[2] != 6 {
		t.Fatalf("raw fetches/series/samples: got %v/%v/%v, want 1/2/6", got[0], got[1], got[2])
	}
}

// TestRawFetchMetrics_Metadata checks the /api/v1/series path (Select with a
// "series" hint) is not counted as a raw fetch — it carries no samples.
func TestRawFetchMetrics_Metadata(t *testing.T) {
	ps, _ := newProxyStorage(t, &dataStub{})
	q, err := ps.Querier(0, 10000)
	if err != nil {
		t.Fatal(err)
	}

	deltas := counterDeltas(t, rawSeriesFetches)
	hints := &storage.SelectHints{Start: 0, End: 10000, Func: "series"}
	drain(t, q.Select(context.Background(), false, hints, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")))

	if got := deltas()[0]; got != 0 {
		t.Fatalf("metadata Select counted as raw fetch: got %v, want 0", got)
	}
}

// TestCountingIteratorSeek checks a Seek that lands on an already-counted
// sample doesn't count it twice — the engine seeks once per step, which is
// often finer-grained than the samples themselves.
func TestCountingIteratorSeek(t *testing.T) {
	deltas := counterDeltas(t, backendSamples.WithLabelValues(pathRaw))

	ss := countSeriesSet((&dataStub{}).set(), pathRaw)
	if !ss.Next() {
		t.Fatal("expected a series")
	}
	it := ss.At().Iterator(nil)
	for _, ts := range []int64{500, 900, 1000, 1500, 2500, 3500} {
		it.Seek(ts)
	}

	// Seeks land on 1000, 1000, 1000, 2000, 3000, none -> 3 distinct samples.
	if got := deltas()[0]; got != 3 {
		t.Fatalf("samples counted: got %v, want 3", got)
	}
}

// seriesSetOf builds a set whose i-th series carries lengths[i] samples, one
// per second.
func seriesSetOf(lengths ...int) storage.SeriesSet {
	var out []storage.Series
	for i, n := range lengths {
		samples := make([]chunks.Sample, n)
		for j := range samples {
			samples[j] = promapi.FloatSample(int64(j+1)*1000, float64(j))
		}
		out = append(out, promapi.NewSeries(
			labels.FromStrings(model.MetricNameLabel, "foo", "instance", strconv.Itoa(i)), samples))
	}
	return promapi.NewSeriesSet(out, nil, nil)
}

// TestCountingBatching checks the batched sample counter (one Add per series,
// not per sample) still totals exactly what a full drain reads.
func TestCountingBatching(t *testing.T) {
	deltas := counterDeltas(t,
		backendSeries.WithLabelValues(pathRaw),
		backendSamples.WithLabelValues(pathRaw),
	)

	ss := countSeriesSet(seriesSetOf(1, 2, 5, 3), pathRaw)
	read := 0
	for ss.Next() {
		it := ss.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			read++
		}
	}
	if read != 11 {
		t.Fatalf("read %d samples, want 11", read)
	}
	if got := deltas(); got[0] != 4 || got[1] != float64(read) {
		t.Fatalf("series/samples: got %v/%v, want 4/%d", got[0], got[1], read)
	}
}

// TestCountingBatchingPartialSeries checks that abandoning a series mid-read
// and advancing the set banks what was read so far.
func TestCountingBatchingPartialSeries(t *testing.T) {
	deltas := counterDeltas(t, backendSamples.WithLabelValues(pathRaw))

	ss := countSeriesSet(seriesSetOf(5, 2), pathRaw)
	if !ss.Next() {
		t.Fatal("expected a first series")
	}
	it := ss.At().Iterator(nil)
	it.Next()
	it.Next()
	if got := deltas()[0]; got != 0 {
		t.Fatalf("samples banked before the series boundary: got %v, want 0", got)
	}

	// Moving on flushes the 2 samples read from the first series.
	if !ss.Next() {
		t.Fatal("expected a second series")
	}
	if got := deltas()[0]; got != 2 {
		t.Fatalf("samples after advancing: got %v, want 2", got)
	}

	// Reading the second one to exhaustion banks its 2 samples as well.
	it = ss.At().Iterator(nil)
	for it.Next() != chunkenc.ValNone { //nolint: revive
	}
	if got := deltas()[0]; got != 4 {
		t.Fatalf("samples after reading the second series: got %v, want 4", got)
	}
}
