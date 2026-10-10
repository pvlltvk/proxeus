package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"

	"github.com/pvlltvk/proxeus/pkg/proxystorage"
)

const rawIgnoreLabelsConfig = `
proxeus:
  cross_group_dedup: true
  cross_group_dedup_ignore_labels: %s
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        backend: thanos
        cluster: thanos-sandbox
    - static_configs:
        - targets:
          - %s
      labels:
        backend: vm
        cluster: vm-sandbox
`

const dataWithExtras = `
load 1m
	node_cpu_seconds_total{instance="host1", cpu="0", mode="idle", receive_replica="0", tenant_id="default-tenant"} 0+1x5
	node_cpu_seconds_total{instance="host1", cpu="0", mode="user", receive_replica="0", tenant_id="default-tenant"} 0+2x5
`

const dataWithoutExtras = `
load 1m
	node_cpu_seconds_total{instance="host1", cpu="0", mode="idle"} 0+1x5
	node_cpu_seconds_total{instance="host1", cpu="0", mode="user"} 0+2x5
`

// Unlike exactTestbed, each group gets its own store, so one side's
// series can carry labels the other's don't.
func ignoreLabelsTestbed(t *testing.T, ignoreLabelsYAML string) *proxystorage.ProxyStorage {
	t.Helper()

	storeA := promqltest.LoadedStorage(t, dataWithExtras)
	t.Cleanup(func() { storeA.Close() })
	storeB := promqltest.LoadedStorage(t, dataWithoutExtras)
	t.Cleanup(func() { storeB.Close() })

	srvA, addrA, stopA := startAPIForTest(storeA)
	srvB, addrB, stopB := startAPIForTest(storeB)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srvA.Shutdown(ctx)
		_ = srvB.Shutdown(ctx)
		<-stopA
		<-stopB
	})

	cfg := fmt.Sprintf(rawIgnoreLabelsConfig, ignoreLabelsYAML, addrA, addrB)
	return getProxyStorage(cfg)
}

func TestCrossGroupDedupIgnoreLabels_JoinNoLongerErrors(t *testing.T) {
	expr := `node_cpu_seconds_total{mode="idle"} / on(instance, cpu) node_cpu_seconds_total{mode="user"}`
	now := time.Unix(240, 0)

	t.Run("without ignore list: ambiguous match", func(t *testing.T) {
		ps := ignoreLabelsTestbed(t, "[]")
		_, err := evalInstantErr(t, ps, expr, now)
		if err == nil {
			t.Fatalf("expected the join to fail without cross_group_dedup_ignore_labels (duplicate series per match group), got no error")
		}
		if !strings.Contains(err.Error(), "duplicate") && !strings.Contains(err.Error(), "multiple matches") {
			t.Fatalf("expected an ambiguous-match/duplicate-series error, got: %v", err)
		}
	})

	t.Run("with ignore list: join answers, one series per target", func(t *testing.T) {
		ps := ignoreLabelsTestbed(t, "[receive_replica, tenant_id]")
		vec, err := evalInstantErr(t, ps, expr, now)
		if err != nil {
			t.Fatalf("join failed even with cross_group_dedup_ignore_labels set: %v", err)
		}
		if len(vec) != 1 {
			t.Fatalf("expected exactly one series (host1/cpu0 idle-over-user ratio), got %d: %v", len(vec), vec)
		}
		if got, want := vec[0].F, 0.5; got != want {
			t.Fatalf("idle/user ratio = %v, want %v", got, want)
		}
	})
}

func TestCrossGroupDedupIgnoreLabels_RawSelectorCollapses(t *testing.T) {
	now := time.Unix(240, 0)

	t.Run("without ignore list: two series", func(t *testing.T) {
		ps := ignoreLabelsTestbed(t, "[]")
		vec, err := evalInstantErr(t, ps, `node_cpu_seconds_total{mode="idle"}`, now)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(vec) != 2 {
			t.Fatalf("expected 2 series (extras keep them apart), got %d: %v", len(vec), vec)
		}
	})

	t.Run("with ignore list: one series, winner's extras kept", func(t *testing.T) {
		ps := ignoreLabelsTestbed(t, "[receive_replica, tenant_id]")
		vec, err := evalInstantErr(t, ps, `node_cpu_seconds_total{mode="idle"}`, now)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(vec) != 1 {
			t.Fatalf("expected exactly 1 series (collapsed), got %d: %v", len(vec), vec)
		}
		m := vec[0].Metric.Map()
		if m["backend"] != "thanos" {
			t.Fatalf("expected the lower-ordinal group (thanos) to win, got backend=%q", m["backend"])
		}
		if m["receive_replica"] != "0" || m["tenant_id"] != "default-tenant" {
			t.Fatalf("winner must keep its own extra labels on output, got %v", m)
		}
	})
}

func TestCrossGroupDedupIgnoreLabels_ReloadFlipsIdentity(t *testing.T) {
	storeA := promqltest.LoadedStorage(t, dataWithExtras)
	defer storeA.Close()
	storeB := promqltest.LoadedStorage(t, dataWithoutExtras)
	defer storeB.Close()

	srvA, addrA, stopA := startAPIForTest(storeA)
	srvB, addrB, stopB := startAPIForTest(storeB)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srvA.Shutdown(ctx)
		_ = srvB.Shutdown(ctx)
		<-stopA
		<-stopB
	}()

	now := time.Unix(240, 0)
	expr := `node_cpu_seconds_total{mode="idle"}`

	ps := getProxyStorage(fmt.Sprintf(rawIgnoreLabelsConfig, "[]", addrA, addrB))

	vec, err := evalInstantErr(t, ps, expr, now)
	if err != nil {
		t.Fatalf("query before reload: %v", err)
	}
	if len(vec) != 2 {
		t.Fatalf("before setting the ignore list: expected 2 series, got %d: %v", len(vec), vec)
	}

	if err := ps.ApplyConfig(parseProxeusConfig(t, fmt.Sprintf(rawIgnoreLabelsConfig, "[receive_replica, tenant_id]", addrA, addrB))); err != nil {
		t.Fatalf("reload: %v", err)
	}

	vec, err = evalInstantErr(t, ps, expr, now)
	if err != nil {
		t.Fatalf("query after reload: %v", err)
	}
	if len(vec) != 1 {
		t.Fatalf("after a reload setting the ignore list: expected 1 series, got %d: %v -- the SIGHUP path is not wiring cross_group_dedup_ignore_labels through", len(vec), vec)
	}

	if err := ps.ApplyConfig(parseProxeusConfig(t, fmt.Sprintf(rawIgnoreLabelsConfig, "[]", addrA, addrB))); err != nil {
		t.Fatalf("reload back: %v", err)
	}
	vec, err = evalInstantErr(t, ps, expr, now)
	if err != nil {
		t.Fatalf("query after reloading back: %v", err)
	}
	if len(vec) != 2 {
		t.Fatalf("after reloading the ignore list back off: expected 2 series, got %d: %v", len(vec), vec)
	}
}

func evalInstantErr(t *testing.T, ps *proxystorage.ProxyStorage, expr string, ts time.Time) (promql.Vector, error) {
	t.Helper()
	eng := exactEngine()
	eng.NodeReplacer = ps.NodeReplacer
	query, err := eng.NewInstantQuery(context.Background(), ps, nil, expr, ts)
	if err != nil {
		return nil, err
	}
	defer query.Close()
	res := query.Exec(context.Background())
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Vector()
}
