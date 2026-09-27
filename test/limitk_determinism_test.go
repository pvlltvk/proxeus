package test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/teststorage"
)

// limitk keeps the first k series in Select order, so the cross-group merge
// must hand the engine a stable order across runs, with or without dedup.
func TestLimitkDeterminism_CrossGroupMerge(t *testing.T) {
	const iterations = 40

	for _, dedup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cross_group_dedup=%v", dedup), func(t *testing.T) {
			backendA := teststorage.New(t)
			defer backendA.Close()
			backendB := teststorage.New(t)
			defer backendB.Close()

			// Identical series on both groups, so dedup has collisions to resolve.
			seedLimitkData(t, backendA)
			seedLimitkData(t, backendB)

			srvA, addrA, stopA := startAPIForTest(backendA)
			srvB, addrB, stopB := startAPIForTest(backendB)
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = srvA.Shutdown(ctx)
				_ = srvB.Shutdown(ctx)
				<-stopA
				<-stopB
			}()

			cfg := fmt.Sprintf(`
proxeus:
  cross_group_dedup: %t
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
`, dedup, addrA, addrB)

			ps := getProxyStorage(cfg)
			eng := testAPIEngine()
			eng.NodeReplacer = ps.NodeReplacer

			query := `limitk(2, http_requests_total)`
			now := time.Unix(100, 0)

			run := func() string {
				t.Helper()
				q, err := eng.NewInstantQuery(context.Background(), ps, nil, query, now)
				if err != nil {
					t.Fatalf("NewInstantQuery: %v", err)
				}
				defer q.Close()
				res := q.Exec(context.Background())
				if res.Err != nil {
					t.Fatalf("query exec: %v", res.Err)
				}
				vec, ok := res.Value.(promql.Vector)
				if !ok {
					t.Fatalf("expected a Vector result, got %T", res.Value)
				}
				b, err := json.Marshal(vec)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				return string(b)
			}

			ref := run()
			for i := 1; i < iterations; i++ {
				got := run()
				if got != ref {
					t.Fatalf("iteration %d: limitk result not byte-stable\ngot:  %s\nwant: %s", i, got, ref)
				}
			}
		})
	}
}

// seedLimitkData loads 5 series of http_requests_total into st, so
// limitk(2, ...) has more than k candidates to choose from.
func seedLimitkData(t *testing.T, st *teststorage.TestStorage) {
	t.Helper()
	app := st.Appender(context.Background())
	for i := 0; i < 5; i++ {
		lbls := labels.FromStrings("__name__", "http_requests_total", "instance", fmt.Sprintf("i%d", i))
		if _, err := app.Append(0, lbls, 100000, float64(i)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := app.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
