package test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/proxystorage"
)

// HTTP-level coverage for cross_group_exact_aggregates: two server_groups in
// front of the SAME backend storage, so every series exists in both groups --
// the 100%-overlap case the flag is for. `az` is the per-group external label
// dedup ignores. The unit layer in pkg/proxystorage/exact_aggregates_test.go
// pins the pushdown decision; this file checks the answers the engine actually
// produces once the pushdown is declined.

const rawExactAggregatesConfig = `
proxeus:
  cross_group_dedup: true
  cross_group_exact_aggregates: %s
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        az: a
    - static_configs:
        - targets:
          - %s
      labels:
        az: b
`

const rawExactAggregatesConfigRR = `
proxeus:
  cross_group_dedup: true
  cross_group_exact_aggregates: %s
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        az: a
      remote_read: true
    - static_configs:
        - targets:
          - %s
      labels:
        az: b
      remote_read: true
`

const exactAggregatesData = `
load 1m
  http_requests{job="api", instance="0", group="prod"}   0+10x30
  http_requests{job="api", instance="1", group="canary"} 0+20x30
  http_requests{job="app", instance="0", group="prod"}   0+30x30
  http_requests{job="app", instance="1", group="canary"} 0+40x30
  up{job="api", instance="0"} 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
  up{job="api", instance="1"} 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
  up{job="app", instance="0"} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
`

// exactAggregatesTestbed stands up one backend storage, two prometheus API
// servers over it and a ProxyStorage in front of both. The returned storage is
// the single source of truth results are compared against: with both groups
// serving it, an exact aggregate over the two groups has to equal the
// aggregate one backend computes on its own.
func exactAggregatesTestbed(t *testing.T, cfgTemplate string, exact bool) (*proxystorage.ProxyStorage, storage.Storage) {
	t.Helper()

	store := promqltest.LoadedStorage(t, exactAggregatesData)
	t.Cleanup(func() { store.Close() })

	srvA, addrA, stopA := startAPIForTest(store)
	srvB, addrB, stopB := startAPIForTest(store)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srvA.Shutdown(ctx)
		_ = srvB.Shutdown(ctx)
		<-stopA
		<-stopB
	})

	return getProxyStorage(fmt.Sprintf(cfgTemplate, boolStr(exact), addrA, addrB)), store
}

// parseProxeusConfig parses a config the way getProxyStorage does, for the
// reload path where the ProxyStorage already exists.
func parseProxeusConfig(t *testing.T, cfg string) *proxyconfig.Config {
	t.Helper()
	parsed := &proxyconfig.Config{}
	if err := yaml.Unmarshal([]byte(cfg), parsed); err != nil {
		t.Fatalf("parsing config: %v", err)
	}
	return parsed
}

func exactAggregatesEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Timeout:                  time.Minute,
		MaxSamples:               50000000,
		NoStepSubqueryIntervalFn: func(int64) int64 { return (1 * time.Minute).Milliseconds() },
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		EnableDelayedNameRemoval: true,
	})
}

// evalInstant runs expr against q. With a replacer this is a proxeus query;
// with nil it is the plain engine over the backend storage, i.e. the answer
// proxeus has to match.
func evalInstant(t *testing.T, q storage.Queryable, replacer parser.NodeReplacer, expr string, ts time.Time) promql.Vector {
	t.Helper()
	eng := exactAggregatesEngine()
	eng.NodeReplacer = replacer
	query, err := eng.NewInstantQuery(context.Background(), q, nil, expr, ts)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	defer query.Close()
	res := query.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("%s: %v", expr, res.Err)
	}
	vec, err := res.Vector()
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	return vec
}

func evalRange(t *testing.T, q storage.Queryable, replacer parser.NodeReplacer, expr string, start, end time.Time, step time.Duration) promql.Matrix {
	t.Helper()
	eng := exactAggregatesEngine()
	eng.NodeReplacer = replacer
	query, err := eng.NewRangeQuery(context.Background(), q, nil, expr, start, end, step)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	defer query.Close()
	res := query.Exec(context.Background())
	if res.Err != nil {
		t.Fatalf("%s: %v", expr, res.Err)
	}
	m, err := res.Matrix()
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	return m
}

// formatMetric renders a labelset without the per-group external label: dedup
// hands the winning series on as it arrived, so a result that keeps its input
// labels carries az="a" the backend-only answer has no reason to have.
func formatMetric(m map[string]string) string {
	delete(m, "az")
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+m[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatVector(vec promql.Vector) string {
	out := make([]string, 0, len(vec))
	for _, s := range vec {
		out = append(out, fmt.Sprintf("%s %v", formatMetric(s.Metric.Map()), roundFloat(s.F)))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

func formatMatrix(m promql.Matrix) string {
	out := make([]string, 0, len(m))
	for _, s := range m {
		vals := make([]string, 0, len(s.Floats))
		for _, f := range s.Floats {
			vals = append(vals, fmt.Sprintf("%d:%v", f.T, roundFloat(f.F)))
		}
		out = append(out, fmt.Sprintf("%s %s", formatMetric(s.Metric.Map()), strings.Join(vals, " ")))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// roundFloat trims the float noise the two evaluation paths accumulate
// differently (the sum()/count() avg rewrite against the engine's incremental
// mean, for example).
func roundFloat(f float64) float64 {
	if math.IsNaN(f) {
		return math.NaN()
	}
	return math.Round(f*1e9) / 1e9
}

// exactAggregateShapes are the query shapes the flag has to get right over
// overlapping groups: flat, grouped, nested, composed, inside a subquery, on
// both sides of a binary expr, and with offset / @.
var exactAggregateShapes = []string{
	"count(up)",
	"sum(up)",
	"avg(up)",
	"min(up)",
	"max(up)",
	"group(up)",
	"sum by (job) (http_requests)",
	"sum without (instance) (http_requests)",
	"count by (job) (up)",
	"avg by (group) (http_requests)",
	"count(count by (instance) (up))",
	"sum(count by (job) (http_requests))",
	"sum(rate(http_requests[5m]))",
	"sum by (job) (rate(http_requests[5m]))",
	"avg(rate(http_requests[5m]))",
	"ceil(sum(http_requests))",
	"sort(sum by (job) (http_requests))",
	"topk(2, http_requests)",
	"bottomk(1, http_requests)",
	"topk(1, sum by (instance) (http_requests))",
	"quantile(0.5, http_requests)",
	"stddev(http_requests)",
	`count_values("v", up)`,
	"max_over_time(sum(http_requests)[10m:1m])",
	"sum(http_requests) + sum(http_requests)",
	"sum(http_requests) / count(http_requests)",
	"sum by (job) (http_requests) / sum by (job) (http_requests)",
	"min(http_requests) > 100",
	"topk(1, http_requests) > 100",
	"topk(2, http_requests) > 100",
	"bottomk(2, http_requests) > 100",
	"sum(http_requests) * 2",
	"sum(http_requests) > bool 100",
	"sum(http_requests offset 5m)",
	"sum(http_requests @ 600)",
	"sum(sum_over_time(up[5m]))",
	"count(up == 1)",
	"sum(up) without ()",
}

// TestExactAggregates_ShapesMatchSingleBackend is the acceptance property: with
// the flag on, every shape over two fully overlapping server_groups answers
// exactly what the single backend behind them answers.
func TestExactAggregates_ShapesMatchSingleBackend(t *testing.T) {
	for _, tc := range []struct {
		name     string
		template string
	}{
		{name: "http", template: rawExactAggregatesConfig},
		{name: "remote_read", template: rawExactAggregatesConfigRR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps, store := exactAggregatesTestbed(t, tc.template, true)
			now := time.Unix(900, 0)
			start, end := time.Unix(600, 0), time.Unix(900, 0)

			for _, expr := range exactAggregateShapes {
				t.Run(expr, func(t *testing.T) {
					want := formatVector(evalInstant(t, store, nil, expr, now))
					got := formatVector(evalInstant(t, ps, ps.NodeReplacer, expr, now))
					if got != want {
						t.Errorf("instant %s\nproxeus:\n%s\nbackend:\n%s", expr, got, want)
					}

					wantM := formatMatrix(evalRange(t, store, nil, expr, start, end, time.Minute))
					gotM := formatMatrix(evalRange(t, ps, ps.NodeReplacer, expr, start, end, time.Minute))
					if gotM != wantM {
						t.Errorf("range %s\nproxeus:\n%s\nbackend:\n%s", expr, gotM, wantM)
					}
				})
			}
		})
	}
}

// pushdownDecisions returns the proxeus_pushdown_nodes_total children that
// moved since it was called, as "node/result/reason" keys. Decisions, not
// values: the point is which branch NodeReplacer took.
func pushdownDecisions(t *testing.T) func() map[string]float64 {
	t.Helper()
	before := gatherPushdownNodes(t)
	return func() map[string]float64 {
		deltas := map[string]float64{}
		for k, v := range gatherPushdownNodes(t) {
			if d := v - before[k]; d != 0 {
				deltas[k] = d
			}
		}
		return deltas
	}
}

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
		for _, m := range mf.Metric {
			lbls := map[string]string{}
			for _, l := range m.Label {
				lbls[l.GetName()] = l.GetValue()
			}
			key := fmt.Sprintf("%s/%s/%s", lbls["node"], lbls["result"], lbls["reason"])
			out[key] = m.GetCounter().GetValue()
		}
	}
	return out
}

// TestExactAggregates_FallbackDedupsRawFanOut checks the mechanism rather than
// the answer: the aggregation is declined for the stated reason and the raw
// fan-out that replaces it goes through cross-group dedup (which is what makes
// the aggregate exact), for nested / composed / subquery / binary shapes too.
func TestExactAggregates_FallbackDedupsRawFanOut(t *testing.T) {
	ps, _ := exactAggregatesTestbed(t, rawExactAggregatesConfig, true)
	now := time.Unix(900, 0)

	for _, tc := range []struct {
		expr string
		want []string // decisions that must be recorded
	}{
		{expr: "count(up)", want: []string{"aggregate/fallback/exact_aggregates"}},
		{expr: "sum by (job) (http_requests)", want: []string{"aggregate/fallback/exact_aggregates"}},
		{expr: "count(count by (instance) (up))", want: []string{"aggregate/fallback/exact_aggregates", "aggregate/fallback/nested_aggregate"}},
		{expr: "sum(rate(http_requests[5m]))", want: []string{"aggregate/fallback/exact_aggregates"}},
		{expr: "max_over_time(sum(http_requests)[10m:1m])", want: []string{"aggregate/fallback/exact_aggregates"}},
		{expr: "min(http_requests) > 100", want: []string{"aggregate/fallback/exact_aggregates", "binary/fallback/exact_aggregates"}},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			decisions := pushdownDecisions(t)
			collisionsBefore := counterValue(t, "proxeus_cross_group_dedup_collisions_total")

			evalInstant(t, ps, ps.NodeReplacer, tc.expr, now)

			got := decisions()
			for _, want := range tc.want {
				if got[want] == 0 {
					t.Errorf("%s: decision %q not recorded, got %v", tc.expr, want, got)
				}
			}
			for key := range got {
				if strings.HasPrefix(key, "aggregate/pushed") {
					t.Errorf("%s: aggregation was pushed down anyway: %v", tc.expr, got)
				}
			}
			// The aggregation now runs over the raw fan-out, and both groups
			// hold the same series, so dedup has to resolve a collision for
			// every one of them. Without that the aggregate double-counts.
			if delta := counterValue(t, "proxeus_cross_group_dedup_collisions_total") - collisionsBefore; delta == 0 {
				t.Errorf("%s: no cross-group dedup collisions resolved, so the raw fan-out was not deduped", tc.expr)
			}
		})
	}
}

// TestExactAggregates_FlagOffKeepsPushdown pins the other half of the
// contract: with cross_group_dedup on but the flag off, aggregations are still
// pushed down. Gating on the wrong flag would be invisible in the answers of
// an overlap-free deployment and shows up only here.
func TestExactAggregates_FlagOffKeepsPushdown(t *testing.T) {
	ps, _ := exactAggregatesTestbed(t, rawExactAggregatesConfig, false)
	now := time.Unix(900, 0)

	for _, expr := range []string{"count(up)", "sum by (job) (http_requests)", "min(http_requests) > 100"} {
		t.Run(expr, func(t *testing.T) {
			decisions := pushdownDecisions(t)
			evalInstant(t, ps, ps.NodeReplacer, expr, now)

			got := decisions()
			pushed := false
			for key, v := range got {
				if strings.Contains(key, "exact_aggregates") {
					t.Errorf("%s: declined for exact_aggregates with the flag off: %v", expr, got)
				}
				if v > 0 && (strings.HasPrefix(key, "aggregate/pushed") || strings.HasPrefix(key, "binary/pushed")) {
					pushed = true
				}
			}
			if !pushed {
				t.Errorf("%s: aggregation was not pushed down with the flag off: %v", expr, got)
			}
		})
	}
}

// TestExactAggregates_ConfigReloadFlipsDecision covers the SIGHUP path: the
// flag lives in the state ApplyConfig swaps in, so a reload has to change the
// decision without a restart.
func TestExactAggregates_ConfigReloadFlipsDecision(t *testing.T) {
	store := promqltest.LoadedStorage(t, exactAggregatesData)
	defer store.Close()

	srvA, addrA, stopA := startAPIForTest(store)
	srvB, addrB, stopB := startAPIForTest(store)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srvA.Shutdown(ctx)
		_ = srvB.Shutdown(ctx)
		<-stopA
		<-stopB
	}()

	ps := getProxyStorage(fmt.Sprintf(rawExactAggregatesConfig, "false", addrA, addrB))
	now := time.Unix(900, 0)

	if got := formatVector(evalInstant(t, ps, ps.NodeReplacer, "count(up)", now)); got != "{} 6" {
		t.Fatalf("flag off: count(up) = %q, want the documented double count %q", got, "{} 6")
	}

	if err := ps.ApplyConfig(parseProxeusConfig(t, fmt.Sprintf(rawExactAggregatesConfig, "true", addrA, addrB))); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := formatVector(evalInstant(t, ps, ps.NodeReplacer, "count(up)", now)); got != "{} 3" {
		t.Fatalf("after reload with the flag on: count(up) = %q, want %q", got, "{} 3")
	}

	if err := ps.ApplyConfig(parseProxeusConfig(t, fmt.Sprintf(rawExactAggregatesConfig, "false", addrA, addrB))); err != nil {
		t.Fatalf("reload back: %v", err)
	}
	if got := formatVector(evalInstant(t, ps, ps.NodeReplacer, "count(up)", now)); got != "{} 6" {
		t.Fatalf("after reloading the flag back off: count(up) = %q, want %q", got, "{} 6")
	}
}
