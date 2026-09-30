package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"

	"github.com/pvlltvk/proxeus/pkg/proxystorage"
)

const rawFillGapsConfig = `
proxeus:
  cross_group_dedup: true
  cross_group_dedup_ignore_labels: [receive_replica]
  cross_group_dedup_fill_gaps: %s
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        backend: first
    - static_configs:
        - targets:
          - %s
      labels:
        backend: second
`

// The hole is longer than the 5m lookback, so its middle is empty even though
// the engine carries the last value forward.
const dataWithHole = `
load 1m
	node_cpu_seconds_total{instance="host1", receive_replica="0"} 1 1 1 1 1 _ _ _ _ _ _ _ _ _ _ 1 1 1 1 1
`

const dataComplete = `
load 1m
	node_cpu_seconds_total{instance="host1", receive_replica="0"} 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
`

func fillGapsTestbed(t *testing.T, fillGaps bool) *proxystorage.ProxyStorage {
	t.Helper()

	storeA := promqltest.LoadedStorage(t, dataWithHole)
	t.Cleanup(func() { storeA.Close() })
	storeB := promqltest.LoadedStorage(t, dataComplete)
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

	return getProxyStorage(fmt.Sprintf(rawFillGapsConfig, boolStr(fillGaps), addrA, addrB))
}

func TestCrossGroupDedupFillGaps_RangeQueryHasNoHole(t *testing.T) {
	const steps = 20
	start := time.Unix(0, 0)
	end := time.Unix((steps-1)*60, 0)
	step := time.Minute

	t.Run("fill_gaps off: the hole passes through", func(t *testing.T) {
		ps := fillGapsTestbed(t, false)
		mat := evalRange(t, ps, ps.NodeReplacer, `node_cpu_seconds_total`, start, end, step)
		if len(mat) != 1 {
			t.Fatalf("expected 1 series, got %d: %v", len(mat), mat)
		}
		if len(mat[0].Floats) >= steps {
			t.Fatalf("expected fewer than %d points (a hole in the raw answer), got %d: %v", steps, len(mat[0].Floats), mat[0].Floats)
		}
	})

	t.Run("fill_gaps on: the hole is filled from the other group", func(t *testing.T) {
		ps := fillGapsTestbed(t, true)
		mat := evalRange(t, ps, ps.NodeReplacer, `node_cpu_seconds_total`, start, end, step)
		if len(mat) != 1 {
			t.Fatalf("expected 1 series, got %d: %v", len(mat), mat)
		}
		if len(mat[0].Floats) != steps {
			t.Fatalf("expected all %d points (hole filled), got %d: %v", steps, len(mat[0].Floats), mat[0].Floats)
		}
		for i, f := range mat[0].Floats {
			if f.F != 1 {
				t.Fatalf("point %d = %v, want 1: %v", i, f.F, mat[0].Floats)
			}
		}
		if m := mat[0].Metric.Map(); m["backend"] != "first" {
			t.Fatalf("winner's own labels must be kept, got %v", m)
		}
	})
}
