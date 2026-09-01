package proxystorage

import (
	"context"
	"math"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/pvlltvk/proxeus/pkg/promclient"
)

// crossGroupDedupCollisions counts series collisions resolved by ordinal
// tie-break during cross-server_group dedup (B1). Label values are the winning
// and losing server_group names as defined in the proxeus configuration.
var crossGroupDedupCollisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxeus_cross_group_dedup_collisions_total",
		Help: "Number of cross-server_group series collisions resolved by ordinal tie-break.",
	},
	[]string{"winner", "loser"},
)

// crossGroupDedupMetadataCollisions counts collisions resolved while deduping
// metadata-style endpoints (currently only /api/v1/series) when
// cross_group_dedup_metadata is enabled (F2). The `endpoint` label allows
// future expansion to other endpoints without breaking dashboards.
var crossGroupDedupMetadataCollisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxeus_cross_group_dedup_metadata_collisions_total",
		Help: "Number of cross-server_group metadata collisions resolved by ordinal tie-break, by endpoint.",
	},
	[]string{"winner", "loser", "endpoint"},
)

// pushdownNodes counts the NodeReplacer decisions proxeus makes while
// preparing a query: one increment per visited AST node of a kind we consider
// for pushdown. `result` is "pushed" when the node was answered by (or
// rewritten so it will be answered by) the backends, "fallback" when the
// embedded engine has to evaluate it locally over raw series. `reason` is
// empty for "pushed" and one of the reason* constants below otherwise.
//
// Re-visits of a node we already replaced are not counted — parser.Walk
// descends into the replacement, so they are not decisions.
var pushdownNodes = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxeus_pushdown_nodes_total",
		Help: "Number of NodeReplacer pushdown decisions by AST node kind, result and fallback reason.",
	},
	[]string{"node", "result", "reason"},
)

// rawSeriesFetches counts the raw-series fetches (Querier.Select on the data
// path) that reach the backends because the subtree was not pushed down.
// Pushed-down selectors carry their data in UnexpandedSeriesSet and never go
// through Select. /federate reads through the same querier, so it counts here
// too.
var rawSeriesFetches = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "proxeus_raw_series_fetches_total",
		Help: "Number of raw series fetches sent to the backends for local evaluation.",
	},
)

// backendSeries and backendSamples count what comes back from the backends,
// split by how it was requested: "pushdown" for the NodeReplacer query path,
// "raw" for the local-evaluation fetch path. They are counted as the response
// is drained, so a set the engine abandons half-way is only counted up to
// where it was read. Samples are banked per series rather than per sample (see
// countingIterator), so an abandoned *last* series may not be counted at all.
// Metadata endpoints (/api/v1/series and friends) are not counted — they carry
// no samples.
var (
	backendSeries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxeus_backend_series_total",
			Help: "Number of series read from the backends, by fetch path.",
		},
		[]string{"path"},
	)

	backendSamples = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxeus_backend_samples_total",
			Help: "Number of samples (floats and histograms) read from the backends, by fetch path.",
		},
		[]string{"path"},
	)
)

// Label values for proxeus_pushdown_nodes_total's `result` and for the `path`
// label shared by proxeus_backend_{series,samples}_total.
const (
	resultPushed   = "pushed"
	resultFallback = "fallback"

	pathPushdown = "pushdown"
	pathRaw      = "raw"
)

// Fallback reasons — one per distinct NodeReplacer bail-out branch.
const (
	reasonSubqueryChild       = "subquery_child"        // the node sits below a SubqueryExpr, which already ran its own replacement
	reasonHistogram           = "histogram"             // histogram-bearing subtree: the JSON API would lose schema fidelity
	reasonNestedAggregate     = "nested_aggregate"      // an aggregation below us has its own combining rules
	reasonMultiVectorSelector = "multi_vector_selector" // >1 selector: they may not live on the same backend
	reasonOffsetMismatch      = "offset_mismatch"       // offsets in the subtree don't converge (or couldn't be read)
	reasonMatrixParent        = "matrix_parent"         // selector directly under a MatrixSelector: nothing smaller to ask for
	reasonLossyHistogram      = "lossy_histogram"       // the response carried native histograms, so we redo it locally
	reasonUnsupportedFunc     = "unsupported_func"      // absent/label_join/label_replace/info: handled by the engine
	reasonNonReentrantAgg     = "non_reentrant_agg"     // quantile/stddev/stdvar/limitk/limit_ratio can't be combined from partials
	reasonNoLiteralOperand    = "no_literal_operand"    // binary expr with no literal side
	reasonUnsupportedOperand  = "unsupported_operand"   // binary expr whose non-literal side isn't pushable
	reasonNoInnerPushdown     = "no_inner_pushdown"     // subquery whose inner expression yielded no replacement
	reasonUnsupported         = "unsupported"           // catch-all: nothing better to ask the backends for (e.g. MatrixSelector)
)

// pushdownNodeKind maps a node to its `node` label, or "" for the kinds
// NodeReplacer never pushes down (literals, parens, ...) — those decisions
// aren't counted.
func pushdownNodeKind(node parser.Node) string {
	switch node.(type) {
	case *parser.AggregateExpr:
		return "aggregate"
	case *parser.Call:
		return "call"
	case *parser.VectorSelector:
		return "vector_selector"
	case *parser.MatrixSelector:
		return "matrix_selector"
	case *parser.SubqueryExpr:
		return "subquery"
	case *parser.BinaryExpr:
		return "binary"
	}
	return ""
}

// pushdownAPI counts what the pushdown path pulls from the backends. Only the
// two query methods are wrapped; everything else is forwarded by the embedded
// API.
type pushdownAPI struct {
	promclient.API
}

func (a pushdownAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return countSeriesSet(a.API.Query(ctx, query, ts), pathPushdown)
}

func (a pushdownAPI) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	return countSeriesSet(a.API.QueryRange(ctx, query, r), pathPushdown)
}

// rawAPI counts the raw fetches the embedded engine makes when it evaluates
// locally. GetValue is only reached from ProxyQuerier.Select's data path, so
// one call is one raw fetch.
type rawAPI struct {
	promclient.API
}

func (a rawAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	rawSeriesFetches.Inc()
	return countSeriesSet(a.API.GetValue(ctx, start, end, matchers), pathRaw)
}

// countSeriesSet returns ss instrumented for the given fetch path. The counter
// children are resolved once per fetch, not per series.
func countSeriesSet(ss storage.SeriesSet, path string) storage.SeriesSet {
	return &countingSeriesSet{
		SeriesSet: ss,
		series:    backendSeries.WithLabelValues(path),
		samples:   backendSamples.WithLabelValues(path),
	}
}

// countingSeriesSet counts series and samples as the set is drained. It
// buffers no data: every method forwards and bumps a counter on the way. The
// sample counter is only touched at series boundaries — see countingIterator.
type countingSeriesSet struct {
	storage.SeriesSet
	series  prometheus.Counter
	samples prometheus.Counter
	// last is the most recent iterator handed out, so a consumer that moves
	// on to the next series without exhausting the current one still gets its
	// samples banked.
	last *countingIterator
}

func (s *countingSeriesSet) Next() bool {
	if s.last != nil {
		s.last.flush()
	}
	ok := s.SeriesSet.Next()
	if ok {
		s.series.Inc()
	}
	return ok
}

func (s *countingSeriesSet) At() storage.Series {
	return &countingSeries{Series: s.SeriesSet.At(), set: s}
}

type countingSeries struct {
	storage.Series
	set *countingSeriesSet
}

func (s *countingSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	c, ok := it.(*countingIterator)
	if ok {
		// Handed back for reuse, so the caller is done with it: bank
		// whatever it still holds before it's pointed at another series.
		c.flush()
	} else {
		// Keep the caller's reuse hint, just re-wrapped.
		c = &countingIterator{Iterator: it}
	}
	c.Iterator = s.Series.Iterator(c.Iterator)
	c.samples = s.set.samples
	c.lastT = math.MinInt64
	s.set.last = c
	return c
}

type countingIterator struct {
	chunkenc.Iterator
	samples prometheus.Counter
	// n accumulates the samples seen since the last flush. Counting into a
	// plain int and adding once per series keeps the shared counter's cache
	// line off the per-sample read path, where a long range query over raw
	// series would otherwise contend it across every concurrent query.
	n int64
	// lastT is the timestamp of the last counted sample. The engine seeks
	// forward once per step, which can land on the same sample repeatedly;
	// counting by timestamp keeps those from counting twice.
	lastT int64
}

func (i *countingIterator) Next() chunkenc.ValueType {
	return i.count(i.Iterator.Next())
}

func (i *countingIterator) Seek(t int64) chunkenc.ValueType {
	return i.count(i.Iterator.Seek(t))
}

func (i *countingIterator) count(vt chunkenc.ValueType) chunkenc.ValueType {
	if vt == chunkenc.ValNone {
		i.flush()
		return vt
	}
	if t := i.AtT(); t != i.lastT {
		i.lastT = t
		i.n++
	}
	return vt
}

func (i *countingIterator) flush() {
	if i.n > 0 {
		i.samples.Add(float64(i.n))
		i.n = 0
	}
}
