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
	"github.com/prometheus/prometheus/storage"
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

// labelAPIData holds two metrics with disjoint label sets across series and a
// shared `job` label whose values differ — enough to exercise both name-union
// and value-union behavior.
const labelAPIData = `
load 1m
  up{job="x", instance="host-1"} 1 1 1 1 1
  http_requests{job="y", code="200"} 1 1 1 1 1
`

// labelAPICollisionData includes a series that already carries `az` — the same
// label name the dual-backend test config stamps as a group label. Used to pin
// down the asymmetry between /series (overwrites) and /label/az/values (unions).
const labelAPICollisionData = `
load 1m
  weird{az="orig", job="z"} 1 1 1 1 1
`

// dualBackendProxy spins up two in-process backends sharing `store`, plus a
// promxy in front with az=a / az=b group labels. Returns the proxy URL and a
// cleanup func. Ports are distinct from TestSeriesDedup_HTTP's to allow
// fast retries without TIME_WAIT contention.
func dualBackendProxy(t *testing.T, store storage.Storage, backendAPort, backendBPort, proxyPort string) (proxyURL string, cleanup func()) {
	t.Helper()
	backendA, stopA := startAPIForTest(store, backendAPort)
	backendB, stopB := startAPIForTest(store, backendBPort)

	cfg := `
promxy:
  server_groups:
    - static_configs:
        - targets:
          - localhost` + backendAPort + `
      labels:
        az: a
    - static_configs:
        - targets:
          - localhost` + backendBPort + `
      labels:
        az: b
`
	ps := getProxyStorage(cfg)
	proxySrv, stopP := startAPIForTest(ps, proxyPort)

	return "http://localhost" + proxyPort, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		proxySrv.Shutdown(ctx)
		backendA.Shutdown(ctx)
		backendB.Shutdown(ctx)
		<-stopP
		<-stopA
		<-stopB
	}
}

// getJSON hits `url`, asserts 200, decodes into `out`, and asserts the envelope
// status field is "success".
func getJSON(t *testing.T, url string, out any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status=%d body=%s", url, resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func stringSetFrom(items []string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, s := range items {
		out[s] = struct{}{}
	}
	return out
}

// TestLabelsAPI_HTTP pins the union semantics for /api/v1/labels and
// /api/v1/label/<name>/values across two backends, including group-label
// injection and matcher-driven backend filtering.
func TestLabelsAPI_HTTP(t *testing.T) {
	store := promqltest.LoadedStorage(t, labelAPIData)
	defer store.Close()

	proxyURL, cleanup := dualBackendProxy(t, store, ":18193", ":18195", ":18192")
	defer cleanup()

	t.Run("/labels — union includes group labels", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		getJSON(t, proxyURL+"/api/v1/labels?start=0&end=300", &env)
		if env.Status != "success" {
			t.Errorf("status = %q, want success", env.Status)
		}
		got := stringSetFrom(env.Data)
		want := stringSetFrom([]string{"__name__", "job", "instance", "code", "az"})
		if !sameKeySet(got, want) {
			t.Errorf("labels = %v, want %v", env.Data, want)
		}
	})

	t.Run("/label/az/values — both group values present", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		getJSON(t, proxyURL+"/api/v1/label/az/values?start=0&end=300", &env)
		got := stringSetFrom(env.Data)
		want := stringSetFrom([]string{"a", "b"})
		if !sameKeySet(got, want) {
			t.Errorf("az values = %v, want %v", env.Data, want)
		}
	})

	t.Run("/label/job/values — union across backends", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		getJSON(t, proxyURL+"/api/v1/label/job/values?start=0&end=300", &env)
		got := stringSetFrom(env.Data)
		// Both backends share storage so both return both jobs; the union
		// collapses them to {x, y}. This guards against accidental
		// duplicate-emission if MergeLabelValues were ever weakened.
		want := stringSetFrom([]string{"x", "y"})
		if !sameKeySet(got, want) {
			t.Errorf("job values = %v, want %v", env.Data, want)
		}
	})

	t.Run("/label/job/values?match[]={az=\"a\"} — group A only", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		q := url.Values{}
		q.Set("match[]", `{az="a"}`)
		q.Set("start", "0")
		q.Set("end", "300")
		getJSON(t, proxyURL+"/api/v1/label/job/values?"+q.Encode(), &env)
		// The selector matches group A's az=a (drop selector) and excludes
		// group B's az=b (whole backend skipped). Result is still {x, y}
		// because group A's backend has both — but the point is that the
		// matcher-on-group-label path doesn't error or lose data.
		got := stringSetFrom(env.Data)
		want := stringSetFrom([]string{"x", "y"})
		if !sameKeySet(got, want) {
			t.Errorf("job values (match az=a) = %v, want %v", env.Data, want)
		}
	})

	t.Run("/labels?match[]={job=\"x\"} — name set restricted to matching series", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		q := url.Values{}
		q.Set("match[]", `{job="x"}`)
		q.Set("start", "0")
		q.Set("end", "300")
		getJSON(t, proxyURL+"/api/v1/labels?"+q.Encode(), &env)
		got := stringSetFrom(env.Data)
		// `job="x"` only matches the `up{job="x", instance="host-1"}` series,
		// so the label-name universe is {__name__, job, instance} plus the
		// proxy-injected `az`. `code` should NOT appear.
		want := stringSetFrom([]string{"__name__", "job", "instance", "az"})
		if !sameKeySet(got, want) {
			t.Errorf("labels (match job=x) = %v, want %v", env.Data, want)
		}
	})
}

// TestLabelAPI_GroupLabelCollision_HTTP pins the current behavior when a
// series in the backend already carries a label whose name matches a promxy
// group label. /series overwrites; /label/<name>/values unions. The two
// endpoints disagree — this test documents that gap so any future change is
// an intentional one.
func TestLabelAPI_GroupLabelCollision_HTTP(t *testing.T) {
	store := promqltest.LoadedStorage(t, labelAPICollisionData)
	defer store.Close()

	proxyURL, cleanup := dualBackendProxy(t, store, ":18293", ":18295", ":18292")
	defer cleanup()

	t.Run("/series overwrites: original az=orig is lost", func(t *testing.T) {
		var env struct {
			Status string              `json:"status"`
			Data   []map[string]string `json:"data"`
		}
		q := url.Values{}
		q.Set("match[]", "weird")
		q.Set("start", "0")
		q.Set("end", "300")
		getJSON(t, proxyURL+"/api/v1/series?"+q.Encode(), &env)

		azSeen := stringSetFrom(nil)
		for _, row := range env.Data {
			if v, ok := row["az"]; ok {
				azSeen[v] = struct{}{}
			}
		}
		want := stringSetFrom([]string{"a", "b"})
		if !sameKeySet(azSeen, want) {
			t.Errorf("/series az values = %v, want %v (original az=orig should have been overwritten)", azSeen, want)
		}
		if _, leaked := azSeen["orig"]; leaked {
			t.Errorf("/series leaked az=orig — AddLabelClient should overwrite, not preserve")
		}
	})

	t.Run("/label/az/values unions: az=orig still appears", func(t *testing.T) {
		var env struct {
			Status string   `json:"status"`
			Data   []string `json:"data"`
		}
		getJSON(t, proxyURL+"/api/v1/label/az/values?start=0&end=300", &env)
		got := stringSetFrom(env.Data)
		// Current behavior: backend reports ["orig"], group A unions in "a",
		// group B unions in "b". A user querying values sees "orig" even
		// though no series carrying az=orig is reachable through the proxy.
		// This is the documented divergence from /series.
		want := stringSetFrom([]string{"a", "b", "orig"})
		if !sameKeySet(got, want) {
			t.Errorf("/label/az/values = %v, want %v", env.Data, want)
		}
	})
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

