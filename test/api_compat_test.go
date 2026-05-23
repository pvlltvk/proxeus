package test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/promqltest"
)

// HTTP-level coverage for F2: cross-backend dedup of /api/v1/series when
// cross_group_dedup_metadata is enabled. The unit layer in
// pkg/promclient/cross_group_multi_api_test.go pins MultiAPI behavior; this
// test confirms the wiring all the way through the Prom v1 HTTP API.
//
// Setup: two in-process backends share the same teststorage, so each returns
// the same underlying series. Promxy stamps az=a / az=b via AddLabelClient,
// producing duplicate series modulo az. Deterministic dedup should collapse
// them to one row per logical series.

const seriesDedupData = `
load 1m
  up{job="x"} 1 1 1 1 1
  up{job="y"} 1 1 1 1 1
`

func TestSeriesDedup_HTTP(t *testing.T) {
	cases := []struct {
		name             string
		dedupMetadata    bool
		wantRows         int
		wantAzValues     map[string]struct{} // values of "az" label expected in response
		wantCounterDelta float64
	}{
		{
			name:             "dedup_off — both backends visible (4 rows)",
			dedupMetadata:    false,
			wantRows:         4,
			wantAzValues:     map[string]struct{}{"a": {}, "b": {}},
			wantCounterDelta: 0,
		},
		{
			name:             "dedup_on — collapsed to 2 rows, lowest ordinal wins",
			dedupMetadata:    true,
			wantRows:         2,
			wantAzValues:     map[string]struct{}{"a": {}}, // lowest-ordinal group keeps its label
			wantCounterDelta: 2,                            // one collision per logical series (up{job=x}, up{job=y})
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := promqltest.LoadedStorage(t, seriesDedupData)
			defer store.Close()

			backendA, stopA := startAPIForTest(store, ":18083")
			backendB, stopB := startAPIForTest(store, ":18085")
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				backendA.Shutdown(ctx)
				backendB.Shutdown(ctx)
				<-stopA
				<-stopB
			}()

			cfg := `
promxy:
  cross_group_dedup: true
  cross_group_dedup_metadata: ` + boolStr(tc.dedupMetadata) + `
  server_groups:
    - static_configs:
        - targets:
          - localhost:18083
      labels:
        az: a
    - static_configs:
        - targets:
          - localhost:18085
      labels:
        az: b
`
			ps := getProxyStorage(cfg)

			proxySrv, stopP := startAPIForTest(ps, ":18091")
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				proxySrv.Shutdown(ctx)
				<-stopP
			}()

			counterBefore := counterValue(t, "promxy_cross_group_dedup_metadata_collisions_total")

			q := url.Values{}
			q.Set("match[]", "up")
			q.Set("start", "0")
			q.Set("end", "300")

			resp, err := http.Get("http://localhost:18091/api/v1/series?" + q.Encode())
			if err != nil {
				t.Fatalf("GET /api/v1/series: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status=%d, body=%s", resp.StatusCode, body)
			}

			var env struct {
				Status string              `json:"status"`
				Data   []map[string]string `json:"data"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if env.Status != "success" {
				t.Errorf("status = %q, want success", env.Status)
			}
			if len(env.Data) != tc.wantRows {
				t.Errorf("got %d rows, want %d; data=%+v", len(env.Data), tc.wantRows, env.Data)
			}

			gotAz := map[string]struct{}{}
			for _, row := range env.Data {
				if v, ok := row["az"]; ok {
					gotAz[v] = struct{}{}
				}
			}
			if !sameKeySet(gotAz, tc.wantAzValues) {
				t.Errorf("az label values = %v, want %v", gotAz, tc.wantAzValues)
			}

			counterAfter := counterValue(t, "promxy_cross_group_dedup_metadata_collisions_total")
			if got := counterAfter - counterBefore; got != tc.wantCounterDelta {
				t.Errorf("collision counter delta = %v, want %v", got, tc.wantCounterDelta)
			}
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func sameKeySet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// counterValue sums all child counter values for the named metric across labels.
func counterValue(t *testing.T, name string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			if c := m.GetCounter(); c != nil {
				total += c.GetValue()
			}
		}
	}
	return total
}

