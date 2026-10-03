package test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
)

// Differential harness: every query runs through proxeus (federated
// ProxyStorage + NodeReplacer + cross-group dedup) and through the plain engine
// over ONE store holding the union the federation is supposed to represent.
// Results are compared with the per-group `az` label stripped (formatMetric),
// since the reference has no external labels. Divergences the pushdown is known
// to cause are listed in knownDivergences; everything else must match.

type diffMode struct {
	name       string
	remoteRead bool
	exact      bool
	fillGaps   bool
}

var (
	modeDefault = diffMode{name: "default", fillGaps: true}
	modeExact   = diffMode{name: "exact", exact: true, fillGaps: true}
	modeNoFill  = diffMode{name: "nofill", exact: true}
	modeRR      = diffMode{name: "rr", remoteRead: true, fillGaps: true}
	modeRRExact = diffMode{name: "rr_exact", remoteRead: true, exact: true, fillGaps: true}

	// Without cross_group_exact_aggregates an aggregate over overlapping or
	// seamed groups is documented to double count, so those scenarios only run
	// the modes that define a reference.
	allDiffModes  = []diffMode{modeDefault, modeExact, modeNoFill, modeRR, modeRRExact}
	exactModes    = []diffMode{modeExact, modeNoFill, modeRRExact}
	seamModes     = []diffMode{modeExact, modeRRExact}
	histogramMode = []diffMode{modeRR, modeRRExact}
)

type diffScenario struct {
	name string
	// groups holds the load data per server group, in dedup priority order.
	groups []string
	// reference is what one complete store would hold.
	reference string
	modes     []diffMode
	// instants are unix seconds; the range query spans first to last at 1m.
	instants []int64
	queries  []string
}

func diffConfig(addrs []string, m diffMode) string {
	var b strings.Builder
	fmt.Fprintf(&b, "proxeus:\n  cross_group_dedup: true\n  cross_group_exact_aggregates: %t\n  cross_group_dedup_fill_gaps: %t\n  server_groups:\n",
		m.exact, m.fillGaps)
	for i, addr := range addrs {
		fmt.Fprintf(&b, "    - static_configs:\n        - targets:\n          - %s\n      labels:\n        az: g%d\n      remote_read: %t\n",
			addr, i+1, m.remoteRead)
	}
	return b.String()
}

// diffEval renders the result of one query as comparable text. A failed query is
// an outcome too: both sides erroring counts as a match.
func diffEval(q storage.Queryable, replacer parser.NodeReplacer, expr string, instant int64, rangeEnd int64) string {
	eng := exactAggregatesEngine()
	eng.NodeReplacer = replacer
	var (
		query promql.Query
		err   error
	)
	if rangeEnd == 0 {
		query, err = eng.NewInstantQuery(context.Background(), q, nil, expr, time.Unix(instant, 0))
	} else {
		query, err = eng.NewRangeQuery(context.Background(), q, nil, expr, time.Unix(instant, 0), time.Unix(rangeEnd, 0), time.Minute)
	}
	if err != nil {
		return "error"
	}
	defer query.Close()
	res := query.Exec(context.Background())
	if res.Err != nil {
		return "error"
	}

	var lines []string
	switch v := res.Value.(type) {
	case promql.Vector:
		for _, s := range v {
			val := fmt.Sprint(roundFloat(s.F))
			if s.H != nil {
				val = formatHistogram(s.H)
			}
			lines = append(lines, formatMetric(s.Metric.Map())+" "+val)
		}
	case promql.Scalar:
		lines = append(lines, fmt.Sprint(roundFloat(v.V)))
	case promql.Matrix:
		for _, s := range v {
			var vals []string
			for _, f := range s.Floats {
				vals = append(vals, fmt.Sprintf("%d:%v", f.T, roundFloat(f.F)))
			}
			for _, h := range s.Histograms {
				vals = append(vals, fmt.Sprintf("%d:%s", h.T, formatHistogram(h.H)))
			}
			sort.Strings(vals)
			lines = append(lines, formatMetric(s.Metric.Map())+" "+strings.Join(vals, " "))
		}
	default:
		return fmt.Sprintf("unexpected %T", res.Value)
	}
	if !strings.HasPrefix(expr, "sort") {
		sort.Strings(lines)
	}
	return strings.Join(lines, "\n")
}

func formatHistogram(h *histogram.FloatHistogram) string {
	var b strings.Builder
	fmt.Fprintf(&b, "{count:%v sum:%v schema:%d zt:%v zc:%v", roundFloat(h.Count), roundFloat(h.Sum), h.Schema, roundFloat(h.ZeroThreshold), roundFloat(h.ZeroCount))
	for _, side := range []struct {
		sign string
		it   histogram.BucketIterator[float64]
	}{{"+", h.PositiveBucketIterator()}, {"-", h.NegativeBucketIterator()}} {
		for side.it.Next() {
			bk := side.it.At()
			fmt.Fprintf(&b, " %s[%v,%v]:%v", side.sign, roundFloat(bk.Lower), roundFloat(bk.Upper), roundFloat(bk.Count))
		}
	}
	b.WriteString("}")
	return b.String()
}

// diffQueryOutcome compares one query at every instant and, for rangeExprs, as
// a range query. It returns the first divergence, or "" when all agree.
func diffQueryOutcome(ref storage.Queryable, sc diffScenario, ps storage.Queryable, replacer parser.NodeReplacer, expr string) string {
	for _, ts := range sc.instants {
		want := diffEval(ref, nil, expr, ts, 0)
		if got := diffEval(ps, replacer, expr, ts, 0); got != want {
			return fmt.Sprintf("instant t=%d\nreference:\n%s\nproxeus:\n%s", ts, want, got)
		}
	}
	if rangeExprs[expr] {
		start, end := sc.instants[0], sc.instants[len(sc.instants)-1]
		want := diffEval(ref, nil, expr, start, end)
		if got := diffEval(ps, replacer, expr, start, end); got != want {
			return fmt.Sprintf("range t=%d..%d step 1m\nreference:\n%s\nproxeus:\n%s", start, end, want, got)
		}
	}
	return ""
}

func TestDifferential(t *testing.T) {
	for _, sc := range diffScenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			ref := promqltest.LoadedStorage(t, sc.reference)
			t.Cleanup(func() { ref.Close() })

			addrs := make([]string, len(sc.groups))
			for i, data := range sc.groups {
				store := promqltest.LoadedStorage(t, data)
				t.Cleanup(func() { store.Close() })
				srv, addr, stop := startAPIForTest(store)
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					_ = srv.Shutdown(ctx)
					<-stop
				})
				addrs[i] = addr
			}

			for _, mode := range sc.modes {
				t.Run(mode.name, func(t *testing.T) {
					// Applying a config waits out the discovery manager's 5s first
					// update, so the modes overlap that wait.
					t.Parallel()
					ps := getProxyStorage(diffConfig(addrs, mode))
					for _, expr := range sc.queries {
						t.Run(expr, func(t *testing.T) {
							diff := diffQueryOutcome(ref, sc, ps, ps.NodeReplacer, expr)
							id, known := knownDivergence(sc.name, mode.name, expr)
							switch {
							case known && diff == "":
								t.Errorf("%s now matches reference -- remove from known divergences", id)
							case known:
								t.Logf("known divergence %s", id)
							case diff != "":
								t.Errorf("DIV %s/%s %s\n%s", sc.name, mode.name, expr, diff)
							}
						})
					}
				})
			}
		})
	}
}
