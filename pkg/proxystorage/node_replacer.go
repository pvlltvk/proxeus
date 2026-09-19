package proxystorage

import (
	"context"
	"math"
	"reflect"
	"slices"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promclient"
)

// NodeReplacer replaces promql Nodes with more efficient-to-fetch ones. This works by taking lower-layer
// chunks of the query, farming them out to prometheus hosts, then stitching the results back together.
// An example would be a sum, we can sum multiple sums and come up with the same result -- so we do.
// There are a few ground rules for this:
//   - Children cannot be AggregateExpr: aggregates have their own combining logic, so its not safe to send a subquery with additional aggregations
//   - offsets within the subtree must match: if they don't then we'll get mismatched data, so we wait until we are far enough down the tree that they converge
//   - Don't reduce accuracy/granularity: the intention of this is to get the correct data faster, meaning correctness overrules speed.
func (p *ProxyStorage) NodeReplacer(ctx context.Context, s *parser.EvalStmt, node parser.Node, path []parser.Node) (retNode parser.Node, retErr error) {
	r := &nodeReplacer{
		p:      p,
		ctx:    ctx,
		s:      s,
		node:   node,
		path:   path,
		reason: reasonUnsupported,
	}

	// Record the pushdown decision for this node (see pushdownNodes). Each
	// bail-out below sets reason; the initial value covers the branches with
	// nothing better to ask the backends for. revisit is set where we're
	// handed a node we already replaced -- parser.Walk descends into the
	// replacement, and those visits aren't decisions.
	if kind := pushdownNodeKind(node); kind != "" {
		defer func() {
			switch {
			case retErr != nil || r.revisit:
			case retNode != nil:
				pushdownNodes.WithLabelValues(kind, resultPushed, "").Inc()
			default:
				pushdownNodes.WithLabelValues(kind, resultFallback, r.reason).Inc()
			}
		}()
	}

	stop, err := r.prepare()
	if err != nil || stop {
		return nil, err
	}

	return r.replace()
}

// nodeReplacer is the per-call state of one NodeReplacer visit: the node under
// consideration, what the walk of its subtree found out, and the pushdown
// decision. reason is mutable because the deferred recorder in NodeReplacer
// reports whatever the guards or the switch arm below last set.
type nodeReplacer struct {
	p    *ProxyStorage
	ctx  context.Context
	s    *parser.EvalStmt
	node parser.Node
	path []parser.Node

	state  *proxyStorageState
	client pushdownAPI

	offset       time.Duration
	reqOffset    time.Duration
	synthOffset  time.Duration
	subtreeHasAt bool

	atTimestampFinder *promclient.TimestampFinder
	atUnsafeFinder    *promclient.BooleanFinder

	reason  string
	revisit bool
}

// prepare walks the subtree below node and applies the pushdown guards, in
// order. It returns stop=true when the node must not be replaced, with
// r.reason set to why; otherwise it fills in the state the switch arms read.
func (r *nodeReplacer) prepare() (bool, error) {
	// If we are a child of a subquery; we just skip replacement (since it already did a nodereplacer for those)
	for _, n := range r.path {
		if isSubQuery(n) {
			r.reason = reasonSubqueryChild
			return true, nil
		}
	}

	// If there is a child that is an aggregator we cannot do anything (as they have their own
	// rules around combining). We'll skip this node and let a lower layer take this on
	aggFinder := &promclient.BooleanFinder{Func: isAgg}
	offsetFinder := &promclient.OffsetFinder{}
	vecFinder := &promclient.BooleanFinder{Func: isVectorSelector}
	timestampFinder := &promclient.BooleanFinder{Func: hasTimestamp}
	// atTimestampFinder records the @ timestamp itself (in ms) so we can
	// issue an instant query at a guaranteed-safe time when pushing down
	// step-invariant subtrees — see queryRangeAt.
	r.atTimestampFinder = &promclient.TimestampFinder{}
	// atUnsafeFinder counts Call nodes whose result depends on the
	// evaluation timestamp regardless of any inner @; if any are present
	// queryRangeAt cannot use the instant-query optimization.
	r.atUnsafeFinder = &promclient.BooleanFinder{Func: isAtModifierUnsafeCall}
	// histFinder rides along on the same tree walk to detect histogram-
	// bearing subtrees: histogram-only function calls (always) plus
	// VectorSelectors whose metric name is histogram-typed per the
	// per-server-group metadata cache (when native_histogram.metadata_refresh
	// is configured).
	histFinder := &histogramFinder{isHistogramName: r.p.histogramNamePredicate()}

	visitor := promclient.NewMultiVisitor([]parser.Visitor{aggFinder, offsetFinder, vecFinder, timestampFinder, r.atTimestampFinder, r.atUnsafeFinder, histFinder})

	if _, err := parser.Walk(r.ctx, visitor, r.s, r.node, nil, nil); err != nil {
		return false, err
	}

	// Histogram-bearing queries lose schema fidelity over the HTTP API
	// (the JSON SampleHistogram shape collapses sparse spans into a flat
	// bucket list, drops empty buckets, etc.). Two-step handling:
	//   1. Fail loud if any targeted server group has neither remote_read
	//      nor native_histogram.allow_lossy — wrong data is worse than no
	//      data, so the default is to surface the misconfig.
	//   2. Otherwise opt out of pushdown so the embedded engine evaluates
	//      locally and fetches raw data through GetValue, which routes
	//      via remote_read where configured and preserves the original
	//      FloatHistogram end-to-end.
	// Ancestor-path inheritance: parser.Walk descends into children even
	// when NodeReplacer returns nil for the parent, so a histogram-only
	// call above us still propagates the histogram signal.
	if pathHasHistogramOnlyCall(r.path) || histFinder.found.Load() {
		if missing := r.p.strictMissingRemoteRead(); len(missing) > 0 {
			return false, histogramFidelityError(missing)
		}
		r.reason = reasonHistogram
		return true, nil
	}

	if aggFinder.Found > 0 {
		switch {
		// // If there was a single agg and that was us, then we're okay
		case (isAgg(r.node) || isBinaryExpr(r.node)) && aggFinder.Found == 1:
		// If the aggregations are in a SubQuery; we can allow Subquery to run through NodeReplacerZz
		case isSubQuery(r.node):
		default:
			r.reason = reasonNestedAggregate
			return true, nil
		}
	}

	// If there is more than 1 vector selector here and we are not a subquery
	// we can't combine as we don't know if those 2 selectors will for-sure
	// land on the same node
	if vecFinder.Found > 1 && !isSubQuery(r.node) {
		r.reason = reasonMultiVectorSelector
		return true, nil
	}

	// subtreeHasAt is true when at least one VectorSelector in this subtree has
	// an @ modifier. With @ in play we must NOT strip offsets or shift the
	// downstream request window: the downstream resolves `@ T offset O` into
	// sample[T-O] internally, so any rewrite that removes the offset or moves
	// the request range silently changes the lookup time.
	r.subtreeHasAt = timestampFinder.Found > 0

	// If the tree below us is not all the same offset, then we can't do anything below -- we'll need
	// to wait until further in execution where they all match
	//
	// If we couldn't find an offset, then something is wrong-- lets skip
	// Also if there was an error, skip
	if !offsetFinder.Found || offsetFinder.Error != nil {
		r.reason = reasonOffsetMismatch
		return true, nil
	}
	r.offset = offsetFinder.Offset

	// reqOffset is the time-shift applied to downstream request times so that
	// the engine, after restoring offsets on the synthesized VectorSelector,
	// looks up samples at the right timestamps. When the subtree has @, the
	// downstream already resolves @+offset, so we don't shift and the
	// synthesized node has no offset to re-apply.
	r.reqOffset = r.offset
	r.synthOffset = r.offset
	if r.subtreeHasAt {
		r.reqOffset = 0
		r.synthOffset = 0
	}

	r.state = r.p.GetState()
	// pushdownAPI counts the series/samples the pushdown fetches below pull
	// from the backends; every downstream request in the switch arms below
	// goes through it.
	r.client = pushdownAPI{r.state.client}

	return false, nil
}

// Function to recursivelt remove offset. This is needed as we're using
// the node API to String() the query to downstreams. Promql's iterators require
// that the time be the absolute time, whereas the API returns them based on the
// range you ask for (with the offset being implicit).
//
// When the subtree has an @ modifier we keep offsets in the string: see
// subtreeHasAt comment above.
func (r *nodeReplacer) removeOffset() error {
	if r.subtreeHasAt {
		return nil
	}
	_, err := parser.Walk(r.ctx, &promclient.OffsetRemover{}, r.s, r.node, nil, nil)
	return err
}

// queryRangeAt issues a step-aware downstream request for queryStr. When
// the subtree below us pins evaluation to a single timestamp via @, the
// result at every step is identical (step-invariant); in that case we
// issue a single instant Query at the @ timestamp and replicate the
// returned vector across each step in [s.Start, s.End]. This avoids
// sending QueryRange with a pre-epoch sub-second start time, which the
// upstream prometheus/common model.Time.UnmarshalJSON mis-decodes on
// the way back (see api_query.go hasNegativeFractionalSecond). When the
// subtree has no @, or it contains a Call whose result depends on the
// evaluation timestamp even with @ pinning (timestamp, predict_linear,
// time, etc. — see promql.AtModifierUnsafeFunctions), falls back to the
// regular QueryRange.
func (r *nodeReplacer) queryRangeAt(queryStr string) storage.SeriesSet {
	if r.subtreeHasAt && r.atTimestampFinder.Found && r.atUnsafeFinder.Found == 0 && r.s.Interval > 0 {
		at := timestamp.Time(r.atTimestampFinder.Timestamp)
		result := r.client.Query(r.ctx, queryStr, at)
		if err := result.Err(); err != nil {
			return result
		}
		// The instant query returns one sample per series at the @ time;
		// replicate each across the request's step grid. (Every series in
		// a SeriesSet is vector-shaped here — there is no Scalar/Matrix/
		// String ambiguity left at this layer.)
		return vectorToStepMatrix(result, r.s.Start.Add(-r.reqOffset), r.s.End.Add(-r.reqOffset), r.s.Interval)
	}
	return r.client.QueryRange(r.ctx, queryStr, v1.Range{
		Start: r.s.Start.Add(-r.reqOffset),
		End:   r.s.End.Add(-r.reqOffset),
		Step:  r.s.Interval,
	})
}

// replace dispatches on the node type: each arm either returns the replacement
// subtree or declines the pushdown, recording r.reason.
func (r *nodeReplacer) replace() (parser.Node, error) {
	switch n := r.node.(type) {
	case *parser.AggregateExpr:
		return r.replaceAggregate(n)

	// Call is for things such as rate() etc. This can be sent directly to the
	// prometheus node to answer
	case *parser.Call:
		logrus.Debugf("call %v %v", n, n.Type())

		// absent and absent_over_time are difficult to implement at this layer; and as such we won't touch them
		// we'll do our NodeReplace at another node in the tree.
		//
		// label_join / label_replace / info are evaluated by the engine via
		// dedicated evalLabel{Join,Replace,Info} dispatchers that bypass the
		// FunctionCalls table and call ev.errorf/ev.error with a precise,
		// caller-facing message (e.g. "vector cannot contain metrics with
		// the same labelset"). Pushing them to a single downstream means the
		// error round-trips through ErrorWrap chains (target=…, servergroup=…)
		// before reaching the engine, mangling the exact wording — fine for
		// production, fatal for eval_fail tests. Let the engine handle these
		// locally by fetching args[0] via Querier.Select.
		switch n.Func.Name {
		case "absent", "absent_over_time",
			"label_join", "label_replace", "info":
			r.reason = reasonUnsupportedFunc
			return nil, nil
		}

		// For all the Call's we actually will work on, we need to remove the offset
		_ = r.removeOffset()

		var result storage.SeriesSet
		if r.s.Interval > 0 {
			result = r.queryRangeAt(n.String())
		} else {
			result = r.client.Query(r.ctx, n.String(), r.s.Start.Add(-r.reqOffset))
		}

		result, lossy := containsLossyHistogram(result)
		if lossy {
			r.reason = reasonLossyHistogram
			return nil, nil
		}

		// For range queries, fill StaleNaN at step timestamps the downstream
		// did not return a value for. The engine's per-step VectorSelector
		// eval uses ev.lookbackDelta (5m default, not our ret.LookbackDelta
		// hint) when peeking at the previous sample, so an isolated step
		// sample from a sparse range output (e.g. present_over_time returning
		// 1 at one step only) otherwise bleeds forward into every later step
		// within the lookback window.
		if r.s.Interval > 0 {
			startMs := timestamp.FromTime(r.s.Start.Add(-r.reqOffset))
			endMs := timestamp.FromTime(r.s.End.Add(-r.reqOffset))
			result = fillStaleNaNGaps(result, startMs, endMs, int64(r.s.Interval/time.Millisecond))
		}

		ret := &parser.VectorSelector{OriginalOffset: r.synthOffset}
		if r.s.Interval > 0 {
			ret.LookbackDelta = r.s.Interval - time.Duration(1)
		}
		ret.UnexpandedSeriesSet = result

		// Some functions require specific handling which we'll catch here
		switch n.Func.Name {
		// the "scalar()" function is a bit tricky. It can return a scalar or a vector.
		// So to handle this instead of returning the vector directly (as its just the values selected)
		// we can set it as the args (the vector of data) and the promql engine handles the types properly
		case "scalar":
			n.Args[0] = ret
			return n, nil
		// the functions of sort() and sort_desc() need whole results to calculate.
		case "sort", "sort_desc":
			return &parser.Call{
				Func: n.Func,
				Args: []parser.Expr{ret},
			}, nil
		}

		return ret, nil

	// If we are simply fetching a Vector then we can fetch the data using the same step that
	// the query came in as (reducing the amount of data we need to fetch)
	case *parser.VectorSelector:
		// If the vector selector already has the data we can skip
		if n.UnexpandedSeriesSet != nil {
			r.revisit = true
			return nil, nil
		}

		// Check if this VectorSelector is below a MatrixSelector.
		// If we hit this someone is asking for a matrix directly, if so then we don't
		// have anyway to ask for less-- since this is exactly what they are asking for
		if len(r.path) > 0 {
			if _, ok := r.path[len(r.path)-1].(*parser.MatrixSelector); ok {
				r.reason = reasonMatrixParent
				return nil, nil
			}
		}

		logrus.Debugf("VectorSelector: %v", n)
		_ = r.removeOffset()

		var result storage.SeriesSet
		origLookback := n.LookbackDelta
		if r.s.Interval > 0 {
			n.LookbackDelta = r.s.Interval - time.Duration(1)
			result = r.client.QueryRange(r.ctx, n.String(), v1.Range{
				Start: r.s.Start.Add(-r.reqOffset),
				End:   r.s.End.Add(-r.reqOffset),
				Step:  r.s.Interval,
			})
		} else {
			result = r.client.Query(r.ctx, n.String(), r.s.Start.Add(-r.reqOffset))
		}

		if err := result.Err(); err != nil {
			return nil, err
		}
		result, lossy := containsLossyHistogram(result)
		if lossy {
			// We abandoned the pushdown — restore the original LookbackDelta
			// so the engine's local eval uses the default lookback when it
			// calls Querier.Select instead of the tighter step-minus-1
			// window we set above (which would otherwise drop boundary
			// samples like a load at t=0 with a range query starting at 0).
			n.LookbackDelta = origLookback
			r.reason = reasonLossyHistogram
			return nil, nil
		}

		if n.Timestamp != nil {
			// Downstream resolved @ T (and any offset) when evaluating
			// n.String(); the result is step-invariant. Replace with a flat
			// VectorSelector whose samples sit at the request timestamps so
			// the engine looks them up by ts directly instead of reapplying
			// @ T - offset to a sample set that's already pinned.
			//
			// Preserve Name and LabelMatchers: functions like absent() read
			// these from the (already-resolved) VectorSelector AST node to
			// synthesize output labels (createLabelsForAbsentFunction). The
			// matchers are inert for data lookup at this point — the
			// downstream already returned exactly the right series — but
			// dropping them would mean absent(foo{job="x"} @ T) returns
			// `{} 1` instead of `{job="x"} 1`.
			ret := &parser.VectorSelector{
				Name:           n.Name,
				LabelMatchers:  n.LabelMatchers,
				OriginalOffset: r.synthOffset,
			}
			if r.s.Interval > 0 {
				ret.LookbackDelta = r.s.Interval - time.Duration(1)
			}
			ret.UnexpandedSeriesSet = result
			return ret, nil
		}
		n.OriginalOffset = r.offset
		n.UnexpandedSeriesSet = result
		return n, nil

	// If we hit this someone is asking for a matrix directly, if so then we don't
	// have anyway to ask for less-- since this is exactly what they are asking for
	case *parser.MatrixSelector:
		// DO NOTHING

	// For Subquery Expressions; we basically want to replace them with our own statement (separate interval, step, etc.)
	// Note: since we are replacing this with another query we can get some value differences as this sends larger sub-queries
	// downstream (which may have access to less data, and promql has some weird heuristics on how it calculates values on step)
	case *parser.SubqueryExpr:
		logrus.Debugf("SubqueryExpr: %v", n)

		subEvalStmt := *r.s
		subEvalStmt.Expr = n.Expr

		// If the subquery has an @ modifier its evaluation is pinned to that
		// timestamp and the outer eval window is irrelevant.
		var subEnd time.Time
		if n.Timestamp != nil {
			subEnd = timestamp.Time(*n.Timestamp).Add(-n.Offset)
		} else {
			subEnd = r.s.End.Add(-n.Offset)
		}
		subEvalStmt.End = subEnd

		if n.Step == 0 {
			subEvalStmt.Interval = time.Duration(r.p.NoStepSubqueryIntervalFn(durationMilliseconds(n.Range))) * time.Millisecond
		} else {
			subEvalStmt.Interval = n.Step
		}

		var subStart time.Time
		if n.Timestamp != nil {
			subStart = subEnd.Add(-n.Range)
		} else {
			subStart = r.s.Start.Add(-n.Offset).Add(-n.Range)
		}
		subEvalStmt.Start = subStart.Truncate(subEvalStmt.Interval)
		if subEvalStmt.Start.Before(subStart) {
			subEvalStmt.Start = subEvalStmt.Start.Add(subEvalStmt.Interval)
		}

		newN, err := parser.Inspect(r.ctx, &subEvalStmt, func(parser.Node, []parser.Node) error { return nil }, r.p.NodeReplacer)
		if err != nil {
			return nil, err
		}

		if newN != nil {
			n.Expr = newN.(parser.Expr)
			return n, nil
		}
		r.reason = reasonNoInnerPushdown

	// BinaryExprs *can* be sent untouched to downstreams assuming there is no actual interaction between LHS/RHS
	// these are relatively rare -- as things like `sum(foo) > 2` would *not* be viable as `sum(foo)` could
	// potentially require multiple servergroups to generate the correct response.
	// From inspection there are only 3 specific types where this sort of replacement is "safe" (assuming one side is a literal)
	// 	`VectorSector`
	// 	`AggregateExpr` (Max, Min, TopK, BottomK only -- and only if re-combined)
	case *parser.BinaryExpr:
		logrus.Debugf("BinaryExpr: %v", n)

		// vectorBinaryExpr will send the node as a query to the downstream and return an expanded VectorSelector
		vectorBinaryExpr := func(vs *parser.VectorSelector) (parser.Node, error) {
			logrus.Debugf("BinaryExpr (VectorSelector + Literal): %v", n)
			_ = r.removeOffset()

			var result storage.SeriesSet
			if r.s.Interval > 0 {
				vs.LookbackDelta = r.s.Interval - time.Duration(1)
				result = r.client.QueryRange(r.ctx, n.String(), v1.Range{
					Start: r.s.Start.Add(-r.reqOffset),
					End:   r.s.End.Add(-r.reqOffset),
					Step:  r.s.Interval,
				})
			} else {
				result = r.client.Query(r.ctx, n.String(), r.s.Start.Add(-r.reqOffset))
			}

			if err := result.Err(); err != nil {
				return nil, err
			}
			result, lossy := containsLossyHistogram(result)
			if lossy {
				r.reason = reasonLossyHistogram
				return nil, nil
			}

			ret := &parser.VectorSelector{OriginalOffset: r.synthOffset}
			if r.s.Interval > 0 {
				ret.LookbackDelta = r.s.Interval - time.Duration(1)
			}
			ret.UnexpandedSeriesSet = result
			return ret, nil
		}

		// aggregateBinaryExpr will send the node as a query to the downstream and
		// replace the aggregate expr with the resulting data. This will cause the aggregation
		// (min, max, topk, bottomk) to be re-run against the expression.
		aggregateBinaryExpr := func(agg *parser.AggregateExpr) (parser.Node, error) {
			logrus.Debugf("BinaryExpr (AggregateExpr + Literal): %v", n)

			// cross_group_exact_aggregates: see the AggregateExpr case.
			if r.state.cfg.CrossGroupExactAggregates && len(r.state.sgs) > 1 {
				r.reason = reasonExactAggregates
				return nil, nil
			}

			// Same as the AggregateExpr case: per-group aggregate partials
			// must be unioned by the cross-group merge, not deduped.
			aggCtx := promclient.WithAggregatePushdown(r.ctx)

			_ = r.removeOffset()

			var result storage.SeriesSet

			if r.s.Interval > 0 {
				result = r.client.QueryRange(aggCtx, n.String(), v1.Range{
					Start: r.s.Start.Add(-r.reqOffset),
					End:   r.s.End.Add(-r.reqOffset),
					Step:  r.s.Interval,
				})
			} else {
				result = r.client.Query(aggCtx, n.String(), r.s.Start.Add(-r.reqOffset))
			}
			if err := result.Err(); err != nil {
				return nil, err
			}
			result, lossy := containsLossyHistogram(result)
			if lossy {
				r.reason = reasonLossyHistogram
				return nil, nil
			}

			ret := &parser.VectorSelector{OriginalOffset: r.synthOffset}
			if r.s.Interval > 0 {
				ret.LookbackDelta = r.s.Interval - time.Duration(1)
			}
			ret.UnexpandedSeriesSet = result

			agg.Expr = ret
			return agg, nil
		}

		// Only valid if the other side is either `NumberLiteral` or `StringLiteral`
		this := n.LHS
		other := n.RHS
		literal := promclient.ExprIsLiteral(promclient.UnwrapExpr(this))
		if !literal {
			this = n.RHS
			other = n.LHS
			literal = promclient.ExprIsLiteral(promclient.UnwrapExpr(this))
		}
		// If one side is a literal lets check
		r.reason = reasonNoLiteralOperand
		if literal {
			r.reason = reasonUnsupportedOperand
			switch otherTyped := other.(type) {
			case *parser.VectorSelector:
				return vectorBinaryExpr(otherTyped)
			case *parser.AggregateExpr:
				switch otherTyped.Op {
				case parser.MIN, parser.MAX, parser.TOPK, parser.BOTTOMK:
					return aggregateBinaryExpr(otherTyped)
				}
			}
		}

	default:
		logrus.Debugf("default %v %s", n, reflect.TypeOf(n))
	}
	return nil, nil
}

// Some AggregateExprs can be composed (meaning they are "reentrant". If the aggregation op
// is reentrant/composable then we'll do so, otherwise we let it fall through to normal query mechanisms
func (r *nodeReplacer) replaceAggregate(n *parser.AggregateExpr) (parser.Node, error) {
	// If the vector selector already has the data we can skip
	if vs, ok := n.Expr.(*parser.VectorSelector); ok {
		if vs.UnexpandedSeriesSet != nil {
			r.revisit = true
			return nil, nil
		}
	}

	logrus.Debugf("AggregateExpr %v %s", n, n.Op)

	// With cross_group_exact_aggregates the per-group partials are what
	// makes the aggregate double-count series that live in more than one
	// group: they are unioned, never deduped. Decline the pushdown so the
	// engine aggregates locally over the deduped raw series. A single
	// server_group sees the whole series set, so there is nothing to fix.
	if r.state.cfg.CrossGroupExactAggregates && len(r.state.sgs) > 1 {
		r.reason = reasonExactAggregates
		return nil, nil
	}

	// Mark the fan-out as an aggregation pushdown: each server_group
	// returns an aggregate partial that the engine re-combines, so the
	// cross-group merge must union the partials instead of deduping them
	// (the aggregation may have collapsed the labels dedup keys on).
	r.ctx = promclient.WithAggregatePushdown(r.ctx)

	var result storage.SeriesSet
	var lossy bool

	// queryAggregate sends the aggregation down as it stands and returns
	// the per-server_group partials for the engine to re-combine.
	queryAggregate := func() storage.SeriesSet {
		_ = r.removeOffset()

		if r.s.Interval > 0 {
			return r.client.QueryRange(r.ctx, n.String(), v1.Range{
				Start: r.s.Start.Add(-r.reqOffset),
				End:   r.s.End.Add(-r.reqOffset),
				Step:  r.s.Interval,
			})
		}
		return r.client.Query(r.ctx, n.String(), r.s.Start.Add(-r.reqOffset))
	}

	// Not all Aggregation functions are composable, so we'll do what we can
	switch n.Op {
	// All "reentrant" cases (meaning they can be done repeatedly and the outcome doesn't change)
	case parser.SUM, parser.MIN, parser.MAX, parser.TOPK, parser.BOTTOMK, parser.GROUP:
		result, lossy = containsLossyHistogram(queryAggregate())
		if lossy {
			r.reason = reasonLossyHistogram
			return nil, nil
		}

	// Convert avg into sum() / count()
	case parser.AVG:
		// With a single server_group there are no partials to weigh
		// against each other: the one backend sees the whole series set,
		// so avg is reentrant and goes down as it stands. That keeps the
		// backend's incremental mean, which -- unlike the sum() / count()
		// rewrite below -- doesn't overflow on values near MaxFloat64.
		if len(r.state.sgs) == 1 {
			result, lossy = containsLossyHistogram(queryAggregate())
			if lossy {
				r.reason = reasonLossyHistogram
				return nil, nil
			}
			break
		}

		nameIncluded := false
		for _, g := range n.Grouping {
			if g == model.MetricNameLabel {
				nameIncluded = true
			}
		}

		if nameIncluded {
			replacedGrouping := make([]string, len(n.Grouping))
			for i, g := range n.Grouping {
				if g == model.MetricNameLabel {
					replacedGrouping[i] = metricNameWorkaroundLabel
				} else {
					replacedGrouping[i] = g
				}
			}

			return &parser.AggregateExpr{
				Op: parser.MAX,
				Expr: promclient.PreserveLabel(&parser.BinaryExpr{
					Op: parser.DIV,
					LHS: &parser.AggregateExpr{
						Op:       parser.SUM,
						Expr:     promclient.PreserveLabel(promclient.CloneExpr(n.Expr), model.MetricNameLabel, metricNameWorkaroundLabel),
						Param:    n.Param,
						Grouping: replacedGrouping,
						Without:  n.Without,
					},

					RHS: &parser.AggregateExpr{
						Op:       parser.COUNT,
						Expr:     promclient.PreserveLabel(promclient.CloneExpr(n.Expr), model.MetricNameLabel, metricNameWorkaroundLabel),
						Param:    n.Param,
						Grouping: replacedGrouping,
						Without:  n.Without,
					},
					VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
				}, metricNameWorkaroundLabel, model.MetricNameLabel),
				Grouping: n.Grouping,
				Without:  n.Without,
			}, nil

		}

		// Replace with sum() / count()
		return &parser.BinaryExpr{
			Op: parser.DIV,
			LHS: &parser.AggregateExpr{
				Op:       parser.SUM,
				Expr:     promclient.CloneExpr(n.Expr),
				Param:    n.Param,
				Grouping: n.Grouping,
				Without:  n.Without,
			},

			RHS: &parser.AggregateExpr{
				Op:       parser.COUNT,
				Expr:     promclient.CloneExpr(n.Expr),
				Param:    n.Param,
				Grouping: n.Grouping,
				Without:  n.Without,
			},
			VectorMatching: &parser.VectorMatching{Card: parser.CardOneToOne},
		}, nil

	// For count we simply need to change this to a sum over the data we get back
	case parser.COUNT:
		result, lossy = containsLossyHistogram(queryAggregate())
		if lossy {
			r.reason = reasonLossyHistogram
			return nil, nil
		}
		n.Op = parser.SUM

		// To aggregate count_values we simply sum(count_values(key, metric)) by (key)
	case parser.COUNT_VALUES:

		// The value label is the aggregation's parameter; it may be
		// parenthesized (count_values((("v")), metric)). Anything else
		// isn't ours to rewrite.
		valueLabel, ok := unwrapParens(n.Param).(*parser.StringLiteral)
		if !ok {
			r.reason = reasonNonLiteralParam
			return nil, nil
		}

		// First we must fetch the data into a vectorselector
		result, lossy = containsLossyHistogram(queryAggregate())
		if lossy {
			r.reason = reasonLossyHistogram
			return nil, nil
		}

		ret := &parser.VectorSelector{OriginalOffset: r.synthOffset}
		if r.s.Interval > 0 {
			ret.LookbackDelta = r.s.Interval - time.Duration(1)
		}
		ret.UnexpandedSeriesSet = result

		// Replace with sum(count_values()) BY (label). With `by` the
		// value label has to join the grouping or the sum would collapse
		// the distinct values back together; with `without` the grouping
		// is an exclusion list -- the downstream already dropped those
		// labels, and adding the value label there would drop it too.
		grouping := slices.Clone(n.Grouping)
		if !n.Without {
			grouping = append(grouping, valueLabel.Val)
		}
		return &parser.AggregateExpr{
			Op:       parser.SUM,
			Expr:     ret,
			Grouping: grouping,
			Without:  n.Without,
		}, nil

	case parser.QUANTILE:
		// DO NOTHING
		// this caltulates an actual quantile over the resulting data
		// as such there is no way to reduce the load necessary here. If
		// the query is something like quantile(sum(foo)) then the inner aggregation
		// will reduce the required data
		r.reason = reasonNonReentrantAgg

	// Both of these cases require some mechanism of knowing what labels to do the aggregation on.
	// WIthout that knowledge we require pulling all of the data in, so we do nothing
	case parser.STDDEV:
		// DO NOTHING
		r.reason = reasonNonReentrantAgg
	case parser.STDVAR:
		// DO NOTHING
		r.reason = reasonNonReentrantAgg

	// limitk(k, expr) and limit_ratio(r, expr) are NOT reentrant: the
	// engine picks k (or floor(r*N)) series by hash from the *full*
	// input vector, so pushing the aggregation independently into N
	// upstreams and unioning the results would pick a different,
	// possibly inconsistent, subset than evaluating against the union
	// directly. We let the engine evaluate locally over the raw matrix
	// data fetched via Querier.Select so the hash-based selection sees
	// the complete series set. (Listed here explicitly rather than
	// relying on default fall-through so the non-reentrant decision is
	// visible alongside the reentrant case list above.)
	case parser.LIMITK, parser.LIMIT_RATIO:
		// DO NOTHING
		r.reason = reasonNonReentrantAgg

	}

	if result != nil {
		ret := &parser.VectorSelector{OriginalOffset: r.synthOffset}
		if r.s.Interval > 0 {
			ret.LookbackDelta = r.s.Interval - time.Duration(1)
		}
		ret.UnexpandedSeriesSet = result
		n.Expr = ret

		return n, nil
	}
	return nil, nil
}

func isAgg(node parser.Node) bool {
	_, ok := node.(*parser.AggregateExpr)
	return ok
}

func isSubQuery(node parser.Node) bool {
	_, ok := node.(*parser.SubqueryExpr)
	return ok
}

func isBinaryExpr(node parser.Node) bool {
	_, ok := node.(*parser.BinaryExpr)
	return ok
}

func isVectorSelector(node parser.Node) bool {
	_, ok := node.(*parser.VectorSelector)
	return ok
}

func hasTimestamp(node parser.Node) bool {
	if vs, ok := node.(*parser.VectorSelector); ok {
		return vs.Timestamp != nil
	}
	return false
}

// isAtModifierUnsafeCall flags Call nodes whose result is NOT
// step-invariant even when an inner @ modifier pins the input — e.g.
// timestamp() returns the evaluation-time timestamp, predict_linear
// extrapolates from evalTime, etc. Their presence in the subtree
// disqualifies the instant-query optimization in queryRangeAt above.
func isAtModifierUnsafeCall(node parser.Node) bool {
	c, ok := node.(*parser.Call)
	if !ok {
		return false
	}
	_, unsafe := promql.AtModifierUnsafeFunctions[c.Func.Name]
	return unsafe
}

// vectorToStepMatrix converts an instant-query SeriesSet (one sample per
// series at the @ time) into a range-query-shaped SeriesSet: each input
// series' single sample is replicated at every step time in [start, end]
// (inclusive of start, inclusive of end when (end-start) is a multiple of
// step). Histograms are replicated the same way. This is only valid when the
// underlying expression is step-invariant — currently used by the NodeReplacer
// when the subtree below has an @ modifier.
//
// The replicated FloatHistogram pointer is shared across steps; that is safe
// because promapi.NewSeries hands out copy-on-read iterators, so the engine
// can't observe an aliased histogram.
func vectorToStepMatrix(vec storage.SeriesSet, start, end time.Time, step time.Duration) storage.SeriesSet {
	if step <= 0 {
		return promapi.NewSeriesSet(nil, vec.Warnings(), vec.Err())
	}
	startMs := timestamp.FromTime(start)
	endMs := timestamp.FromTime(end)
	stepMs := int64(step / time.Millisecond)
	if stepMs <= 0 || endMs < startMs {
		return promapi.NewSeriesSet(nil, vec.Warnings(), vec.Err())
	}
	n := int((endMs-startMs)/stepMs) + 1
	var out []storage.Series
	for vec.Next() {
		src := vec.At()
		lbls := src.Labels().Copy()

		// Instant query → at most one sample per series. Read it (the last
		// one wins if, defensively, more than one is present).
		var (
			haveFloat bool
			fval      float64
			fh        *histogram.FloatHistogram
		)
		it := src.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				_, fval = it.At()
				haveFloat, fh = true, nil
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				_, fh = it.AtFloatHistogram(nil)
				haveFloat = false
			}
		}
		if err := it.Err(); err != nil {
			return promapi.NewSeriesSet(nil, vec.Warnings(), err)
		}

		samples := make([]chunks.Sample, 0, n)
		for i := 0; i < n; i++ {
			ts := startMs + int64(i)*stepMs
			switch {
			case fh != nil:
				samples = append(samples, promapi.HistogramSample(ts, fh))
			case haveFloat:
				samples = append(samples, promapi.FloatSample(ts, fval))
			}
		}
		out = append(out, promapi.NewSeries(lbls, samples))
	}
	return promapi.NewSeriesSet(out, vec.Warnings(), vec.Err())
}

// unwrapParens strips the ParenExpr layers the parser keeps around an
// expression, e.g. the `(("v"))` of count_values((("v")), metric).
func unwrapParens(e parser.Expr) parser.Expr {
	for {
		p, ok := e.(*parser.ParenExpr)
		if !ok {
			return e
		}
		e = p.Expr
	}
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

// fillStaleNaNGaps inserts StaleNaN markers at the step timestamps where the
// downstream's range response did not include a sample. We need this because
// proxeus substitutes a *parser.Call (e.g. present_over_time, last_over_time)
// with a synthetic VectorSelector whose UnexpandedSeriesSet exposes the
// downstream-computed samples. When the engine re-evaluates that VectorSelector
// it uses the engine-wide lookback (default 5m) to find samples — even though
// the substituted node represents the call's already-computed step-pinned
// output. Without explicit "no value at this step" markers, an isolated sample
// at one step T_k bleeds forward to every subsequent step within the lookback,
// turning sparse outputs (present_over_time returning 1 at only one step) into
// dense ones. StaleNaN at the empty step timestamps tells vectorSelectorSingle
// to bail out at that step (see promql.IsStaleNaN check in engine.go).
//
// startTs/endTs/interval are in milliseconds. startTs and endTs are inclusive.
// If interval is <= 0 (instant query) ss is returned unchanged. Otherwise the
// set is materialized into a fresh, re-iterable copy with the StaleNaN markers
// added — the source cursor is consumed, and the result still flows on to
// UnexpandedSeriesSet.
func fillStaleNaNGaps(ss storage.SeriesSet, startTs, endTs, interval int64) storage.SeriesSet {
	if interval <= 0 {
		return ss
	}
	stale := math.Float64frombits(value.StaleNaN)
	var out []storage.Series
	for ss.Next() {
		s := ss.At()
		lbls := s.Labels().Copy()
		var samples []chunks.Sample
		present := make(map[int64]struct{})
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				t, v := it.At()
				samples = append(samples, promapi.FloatSample(t, v))
				present[t] = struct{}{}
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				samples = append(samples, promapi.HistogramSample(t, fh))
				present[t] = struct{}{}
			}
		}
		if err := it.Err(); err != nil {
			return promapi.NewSeriesSet(nil, ss.Warnings(), err)
		}

		// Bound the fill defensively: a bogus start/end from upstream
		// shouldn't make us allocate a giant slice. If the eval window is
		// far larger than the actual returned data, skip the fill — the
		// existing samples stay correct (the engine's lookback can still
		// misbehave, but that's strictly no worse than before).
		expected := (endTs-startTs)/interval + 1
		if expected > 0 && expected <= int64(len(present))+10_000 {
			for ts := startTs; ts <= endTs; ts += interval {
				if _, ok := present[ts]; ok {
					continue
				}
				samples = append(samples, promapi.FloatSample(ts, stale))
			}
			// promapi.NewSeries' list iterator must walk samples in
			// timestamp order; the appended StaleNaN points can sit out of
			// order relative to the originals.
			sortSamplesByTime(samples)
		}
		out = append(out, promapi.NewSeries(lbls, samples))
	}
	return promapi.NewSeriesSet(out, ss.Warnings(), ss.Err())
}

// sortSamplesByTime sorts samples by timestamp using insertion sort — a step
// range is typically a handful of points, so a stdlib sort.Slice would pay
// disproportionate cost for the closure call.
func sortSamplesByTime(vs []chunks.Sample) {
	for i := 1; i < len(vs); i++ {
		for j := i; j > 0 && vs[j-1].T() > vs[j].T(); j-- {
			vs[j-1], vs[j] = vs[j], vs[j-1]
		}
	}
}
