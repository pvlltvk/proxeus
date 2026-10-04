package test

import "slices"

// Queries are written against `foo` (floats), `bar`, `meta` and `hist`; a query
// whose series a scenario lacks answers empty on both sides.
var (
	aggregateQueries = []string{
		"sum(foo)", "count(foo)", "avg(foo)", "min(foo)", "max(foo)", "group(foo)",
		"stddev(foo)", "stdvar(foo)", "quantile(0.5, foo)", `count_values("v", foo)`,
		"topk(1, foo)", "topk(2, foo)", "bottomk(1, foo)", "bottomk(2, foo)",
		"sum by (id) (foo)", "sum without (id, az) (foo)", "max by (id) (foo)", "topk by (id) (1, foo)",
		"sum(foo) / count(foo)", "max(foo) - min(foo)", "max(foo) + max(foo)",
	}

	literalQueries = []string{
		"max(foo) < 5", "max(foo) * -1", "min(foo) > 5", "max(foo) == bool 1",
		"topk(1, foo) * -1", "bottomk(1, foo) * -1", "min(foo) * -1", "max(foo) - 1",
		"-max(foo)", "5 - max(foo)", "1 / max(foo)", "max(foo) > bool 5", "min(foo) < bool 5",
		"topk(2, foo) > 5", "max by (id) (foo) * -1", "count(foo) * -1", "avg(foo) * -1",
		"sum(foo) * 2", "sum(foo) > 5", "count(foo) > 0", "sum(foo) > bool 5",
	}

	scalarQueries = []string{
		"scalar(foo)", "vector(scalar(foo))", `vector(scalar(foo{id="a"}))`, `vector(scalar(foo{id="b"}))`, "scalar(sum(foo))",
		"absent(foo)", `absent(foo{id="zzz"})`, "vector(1)", "foo > scalar(max(foo))",
		"time()", "abs(sum(foo))",
	}

	functionQueries = []string{
		"rate(foo[5m])", "increase(foo[5m])", "irate(foo[5m])", "delta(foo[5m])", "deriv(foo[5m])",
		"sum(rate(foo[5m]))", "sum(increase(foo[5m]))", "sum(irate(foo[5m]))", "sum(delta(foo[5m]))",
		"sum(deriv(foo[5m]))", "avg(rate(foo[5m]))", "max(rate(foo[5m]))", "sum by (id) (rate(foo[5m]))",
		"max(rate(foo[5m])) < 1", "sum_over_time(foo[5m])", "count_over_time(foo[5m])", "avg_over_time(foo[5m])",
		"max_over_time(foo[5m])", "last_over_time(foo[5m])", "changes(foo[5m])", "resets(foo[5m])",
		"quantile_over_time(0.5, foo[5m])", "absent_over_time(foo[5m])", "present_over_time(foo[5m])",
		"sum(count_over_time(foo[5m]))", `label_replace(foo, "x", "$1", "id", "(.*)")`,
		`sum by (x) (label_replace(foo, "x", "y", "id", ".*"))`, "round(foo, 0.5)", "sort(foo)", "sort_desc(foo)",
		"sort(sum by (id) (foo))",
	}

	// perSeriesQueries do not aggregate, so they stay meaningful in modes that
	// double count overlapping groups.
	perSeriesQueries = []string{
		"foo", "rate(foo[5m])", "increase(foo[5m])", "irate(foo[5m])", "delta(foo[5m])", "deriv(foo[5m])",
		"rate(foo[5m] offset 1m)", "changes(foo[5m])", "avg_over_time(foo[5m])", "max_over_time(foo[5m])",
		"foo offset 1m", "foo @ 120", "rate(foo[5m:1m])",
	}

	modifierQueries = []string{
		"foo offset 1m", "sum(foo offset 1m)", "max(foo offset 1m)", "rate(foo[5m] offset 1m)",
		"sum(rate(foo[5m] offset 1m))", "foo @ 120", "sum(foo @ 120)", "sum(rate(foo[5m] @ 300))",
		"timestamp(foo offset 1m)", "timestamp(foo)", "foo offset -1m",
		"predict_linear(foo[5m] offset 1m, 60)", "clamp_min(foo offset 1m, time())",
		"sum(predict_linear(foo[5m] offset 1m, 60))", "predict_linear(foo[5m], 60)", "clamp_max(foo, time())",
		"foo offset 1m + foo",
	}

	subqueryQueries = []string{
		"max_over_time(sum(foo)[5m:1m])", "sum_over_time(foo[5m:1m])", "rate(foo[5m:1m])",
		"max_over_time(rate(foo[5m])[5m:1m])", "sum(max_over_time(foo[5m:1m]))",
	}

	joinQueries = []string{
		"foo + foo", "foo / on() group_left sum(foo)", "foo / ignoring(id, az) group_left sum(foo)",
		"foo and foo", `foo unless foo{id="a"}`, "foo or bar", "foo + on(id) bar", "foo * on(id) group_left bar",
		"foo * on() group_left(team) meta", "sum(foo) + on() group_left meta", "foo > on(id) bar",
		"foo / sum(foo)", "foo - avg(foo)",
	}

	histogramQueries = []string{
		"hist", "sum(hist)", "count(hist)", "avg(hist)", "max(hist)", "topk(1, hist)",
		"histogram_quantile(0.5, hist)", "histogram_quantile(0.9, sum(hist))",
		"histogram_count(hist)", "histogram_sum(hist)", "histogram_avg(hist)", "histogram_fraction(0, 1, hist)",
		"sum(histogram_count(hist))", "hist offset 1m", "sum(hist offset 1m)",
		"rate(hist[5m])", "sum(rate(hist[5m]))", "histogram_quantile(0.9, sum(rate(hist[5m])))",
		"histogram_count(rate(hist[5m]))", "increase(hist[5m])", "sum(increase(hist[5m]))",
		"histogram_quantile(0.5, hist) * -1", "histogram_sum(hist) < 5", "sum(hist) * 2",
		"scalar(histogram_count(hist))", "vector(scalar(histogram_sum(hist)))",
		"sum_over_time(hist[5m:1m])", "histogram_sum(sum(hist))",
	}

	transitionFloatQueries = []string{
		"foo", "sum(foo)", "count(foo)", "foo offset 1m", "rate(foo[3m])", "sum(rate(foo[3m]))",
		"last_over_time(foo[3m])", "count_over_time(foo[3m])",
	}

	transitionHistogramQueries = []string{
		"histogram_count(foo)", "sum(histogram_count(foo))", "histogram_sum(foo)", "foo", "count(foo)",
	}

	// crossGroupJoinQueries join foo and bar series that live in different
	// groups.
	crossGroupJoinQueries = []string{"foo + bar", "foo + ignoring(az) bar", "foo or ignoring(az) bar", `foo{id="d"} * on(id) group_left bar`}

	// rangeExprs also run as range queries; a representative cut, not all.
	rangeExprs = toSet(
		"sum(foo)", "max(foo) < 5", "max(foo) * -1", "topk(1, foo) * -1", "sum(rate(foo[5m]))", "rate(foo[5m])",
		"vector(scalar(foo))", "foo", "hist", "sum(hist)", "histogram_quantile(0.5, hist)", "sum(foo offset 1m)",
		"predict_linear(foo[5m] offset 1m, 60)", "max_over_time(sum(foo)[5m:1m])", "foo + foo",
		"foo / on() group_left sum(foo)", "increase(foo[5m])", "histogram_count(foo)", "count(foo) > 0",
		"rate(hist[5m])", "sum(rate(hist[5m]))", "absent(foo)", "foo offset 1m",
	)
)

func toSet(s ...string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}

const (
	histA = `{{schema:0 sum:5 count:4 buckets:[1 2 1]}}`
	histB = `{{schema:0 sum:50 count:40 buckets:[10 20 10]}}`
)

// The winner has floats where the loser has histograms at the same timestamps.
var (
	transitionGroups = []string{
		"load 1m\n  foo{id=\"a\"} 1 2 3\n",
		"load 1m\n  foo{id=\"a\"} {{schema:0 sum:1 count:1 buckets:[1]}}x2\n",
	}
	transitionReference = "load 1m\n  foo{id=\"a\"} 1 2 3\n"
)

// One large group (a Thanos holding most of the infra) and smaller ones (VMs
// with mostly their own data). Each small group also scrapes one node endpoint
// the large group has, with values off enough that a double count or the wrong
// winner shows.
var (
	largeSmallGroups = []string{
		"load 1m\n  foo{id=\"a\",job=\"node\"} 0+10x10\n  foo{id=\"b\",job=\"node\"} 0+20x10\n  foo{id=\"c\",job=\"app\"} 5x10\n" +
			"  bar{id=\"c\",job=\"app\"} 100x10\n  bar{id=\"d\",job=\"vm1\"} 200x10\n  meta{team=\"x\"} 1x10\n",
		"load 1m\n  foo{id=\"a\",job=\"node\"} 0+11x10\n  foo{id=\"d\",job=\"vm1\"} 7x10\n",
		"load 1m\n  foo{id=\"b\",job=\"node\"} 0+25x10\n  foo{id=\"e\",job=\"vm2\"} 0+30x10\n",
	}
	largeSmallReference = "load 1m\n  foo{id=\"a\",job=\"node\"} 0+10x10\n  foo{id=\"b\",job=\"node\"} 0+20x10\n" +
		"  foo{id=\"c\",job=\"app\"} 5x10\n  foo{id=\"d\",job=\"vm1\"} 7x10\n  foo{id=\"e\",job=\"vm2\"} 0+30x10\n" +
		"  bar{id=\"c\",job=\"app\"} 100x10\n  bar{id=\"d\",job=\"vm1\"} 200x10\n  meta{team=\"x\"} 1x10\n"
)

// Group order is dedup priority. Both the series ids and the loser's values are
// chosen so a wrongly picked winner or double count changes the answer.
var diffScenarios = []diffScenario{
	{
		name: "disjoint",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 1x10\n  bar{id=\"a\"} 100x10\n",
			"load 1m\n  foo{id=\"b\"} 10x10\n  bar{id=\"b\"} 200x10\n  meta{team=\"x\"} 1x10\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 1x10\n  foo{id=\"b\"} 10x10\n  bar{id=\"a\"} 100x10\n  bar{id=\"b\"} 200x10\n  meta{team=\"x\"} 1x10\n",
		modes:     allDiffModes,
		instants:  []int64{300, 540},
		queries:   slices.Concat(aggregateQueries, literalQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries, joinQueries),
	},
	{
		name: "identical",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 0+20x10\n  foo{id=\"c\"} 5x10\n  bar{id=\"a\"} 100x10\n",
			"load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 0+20x10\n  foo{id=\"c\"} 5x10\n  bar{id=\"a\"} 100x10\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 0+20x10\n  foo{id=\"c\"} 5x10\n  bar{id=\"a\"} 100x10\n",
		modes:     exactModes,
		instants:  []int64{300, 540},
		queries:   slices.Concat(aggregateQueries, literalQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries, joinQueries),
	},
	{
		name: "unequal",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 5x10\n",
			"load 1m\n  foo{id=\"a\"} 0+30x10\n  foo{id=\"b\"} 7x10\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 5x10\n",
		modes:     exactModes,
		instants:  []int64{300, 540},
		queries:   slices.Concat(aggregateQueries, literalQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries, joinQueries),
	},
	{
		name: "asymmetric",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 0+20x10\n  foo{id=\"c\"} 0+30x10\n",
			"load 1m\n  foo{id=\"c\"} 0+30x10\n  foo{id=\"d\"} 0+40x10\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+10x10\n  foo{id=\"b\"} 0+20x10\n  foo{id=\"c\"} 0+30x10\n  foo{id=\"d\"} 0+40x10\n",
		modes:     exactModes,
		instants:  []int64{300, 540},
		queries:   slices.Concat(aggregateQueries, literalQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries, joinQueries),
	},
	{
		name: "seam",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0 60 120 _ _ _\n  foo{id=\"b\"} 0+30x5\n",
			"load 1m\n  foo{id=\"a\"} _ _ _ 180 240 300\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+60x5\n  foo{id=\"b\"} 0+30x5\n",
		modes:     seamModes,
		instants:  []int64{240, 300},
		queries:   slices.Concat(aggregateQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries),
	},
	{
		name: "seam_per_series",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0 60 120 _ _ _\n  foo{id=\"b\"} 0+30x5\n",
			"load 1m\n  foo{id=\"a\"} _ _ _ 180 240 300\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+60x5\n  foo{id=\"b\"} 0+30x5\n",
		modes:     []diffMode{modeDefault, modeRR},
		instants:  []int64{240, 300},
		queries:   perSeriesQueries,
	},
	{
		name: "hist_disjoint",
		groups: []string{
			"load 1m\n  hist{id=\"a\"} " + histA + "+" + histA + "x10\n",
			"load 1m\n  hist{id=\"b\"} " + histB + "+" + histB + "x10\n",
		},
		reference: "load 1m\n  hist{id=\"a\"} " + histA + "+" + histA + "x10\n  hist{id=\"b\"} " + histB + "+" + histB + "x10\n",
		modes:     histogramMode,
		instants:  []int64{300, 540},
		queries:   histogramQueries,
	},
	{
		name: "hist_seam",
		groups: []string{
			"load 1m\n  hist{id=\"a\"} " + histA + "+" + histA + "x2 _ _ _\n",
			"load 1m\n  hist{id=\"a\"} _ _ _ {{schema:0 sum:20 count:16 buckets:[4 8 4]}}+" + histA + "x2\n",
		},
		reference: "load 1m\n  hist{id=\"a\"} " + histA + "+" + histA + "x5\n",
		modes:     []diffMode{modeRRExact},
		instants:  []int64{240, 300},
		queries:   histogramQueries,
	},
	{
		name:      "float_hist_transition",
		groups:    transitionGroups,
		reference: transitionReference,
		modes:     exactModes,
		instants:  []int64{0, 60, 120},
		queries:   transitionFloatQueries,
	},
	{
		name:      "float_hist_transition_rr",
		groups:    transitionGroups,
		reference: transitionReference,
		modes:     []diffMode{modeRRExact},
		instants:  []int64{0, 60, 120},
		queries:   transitionHistogramQueries,
	},
	{
		name: "clock",
		groups: []string{
			"load 1m\n  foo{id=\"a\"} 0+60x20\n",
			"load 1m\n  foo{id=\"b\"} 0+120x20\n",
		},
		reference: "load 1m\n  foo{id=\"a\"} 0+60x20\n  foo{id=\"b\"} 0+120x20\n",
		modes:     allDiffModes,
		instants:  []int64{600, 840},
		queries:   slices.Concat(modifierQueries, functionQueries[:5], scalarQueries[len(scalarQueries)-2:]),
	},
	// The overlap is known and excluded from the small groups, which leaves
	// the groups disjoint: everything matches without exact mode.
	{
		name:           "large_small_excluded",
		groups:         largeSmallGroups,
		reference:      largeSmallReference,
		modes:          allDiffModes,
		instants:       []int64{300, 540},
		queries:        slices.Concat(aggregateQueries, literalQueries, scalarQueries, functionQueries, modifierQueries, subqueryQueries, joinQueries, crossGroupJoinQueries),
		injectMatchers: map[int][]string{1: {`job!="node"`}, 2: {`job!="node"`}},
	},
	// The same overlap left in: dedup still answers per-series queries
	// correctly (aggregates double count; see README).
	{
		name:      "large_small_overlap",
		groups:    largeSmallGroups,
		reference: largeSmallReference,
		modes:     []diffMode{modeDefault, modeRR},
		instants:  []int64{300, 540},
		queries:   perSeriesQueries,
	},
}

// knownDiv attributes diverging queries to a review finding or a documented
// limitation. A zero scenarios or modes list matches any.
type knownDiv struct {
	id        string
	scenarios []string
	modes     []string
	exprs     []string
}

var knownDivergences = []knownDiv{
	{
		// Each group's labels stay on its series, so a vector match between
		// series from different groups fails unless it ignores them. Documented.
		id:        "group label in cross-group matching",
		scenarios: []string{"large_small_excluded"},
		exprs:     []string{"foo + bar", "foo or bar"},
	},
	{
		// Without cross_group_exact_aggregates per-series calls and selectors are
		// evaluated in each group, so a series split at a migration seam is
		// evaluated per piece. Documented; exact mode is the answer.
		id:        "seam outside exact mode",
		scenarios: []string{"seam_per_series"},
		modes:     []string{"default", "rr"},
		exprs: []string{
			"avg_over_time(foo[5m])",
			"changes(foo[5m])",
			"delta(foo[5m])",
			"foo",
			"foo offset 1m",
			"increase(foo[5m])",
			"max_over_time(foo[5m])",
			"rate(foo[5m:1m])",
			"rate(foo[5m] offset 1m)",
			"rate(foo[5m])",
		},
	},
	{
		id:        "R8",
		scenarios: []string{"float_hist_transition"},
		modes:     []string{"exact", "rr_exact"},
		exprs: []string{
			"count_over_time(foo[3m])",
			"rate(foo[3m])",
			"sum(rate(foo[3m]))",
		},
	},
}

func knownDivergence(scenario, mode, expr string) (string, bool) {
	for _, k := range knownDivergences {
		if (len(k.scenarios) == 0 || slices.Contains(k.scenarios, scenario)) &&
			(len(k.modes) == 0 || slices.Contains(k.modes, mode)) &&
			slices.Contains(k.exprs, expr) {
			return k.id, true
		}
	}
	return "", false
}
