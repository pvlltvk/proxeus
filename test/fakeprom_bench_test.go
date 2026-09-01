package test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/fakeprom"
	"github.com/pvlltvk/proxeus/pkg/promapi"
)

// End-to-end query_range benchmarks: N synthetic Prometheus backends (see
// pkg/fakeprom) behind a real proxeus ProxyStorage and prometheus v1 API, driven
// over HTTP. Everything runs in one process, so the numbers include the
// backends' own JSON encoding -- but a fakeprom is orders of magnitude cheaper
// than the Thanos/VictoriaMetrics sandboxes we used before, which saturated at
// ~9 req/s and made proxeus itself unmeasurable.
//
//	go test -run=^$ -bench=BenchmarkFakepromQueryRange -benchmem ./test/
//
// Knobs (all optional):
//
//	PROXEUS_BENCH_RANGE        query range length            (default 1h)
//	PROXEUS_BENCH_STEP         query resolution step         (default 15s)
//	PROXEUS_BENCH_OVERLAP      fraction of series shared between backends (default 0.5)
//	PROXEUS_BENCH_MAX_SAMPLES  per-request sample ceiling; cases whose
//	                           worst-case response exceeds it are skipped
//	                           (default 8000000)
type benchParams struct {
	rangeLen   time.Duration
	step       time.Duration
	overlap    float64
	maxSamples int
}

func loadBenchParams(b *testing.B) benchParams {
	b.Helper()
	p := benchParams{rangeLen: time.Hour, step: 15 * time.Second, overlap: 0.5, maxSamples: 8000000}
	if v := os.Getenv("PROXEUS_BENCH_RANGE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			b.Fatalf("PROXEUS_BENCH_RANGE: %v", err)
		}
		p.rangeLen = d
	}
	if v := os.Getenv("PROXEUS_BENCH_STEP"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			b.Fatalf("PROXEUS_BENCH_STEP: %v", err)
		}
		p.step = d
	}
	if v := os.Getenv("PROXEUS_BENCH_OVERLAP"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			b.Fatalf("PROXEUS_BENCH_OVERLAP: %v", err)
		}
		p.overlap = f
	}
	if v := os.Getenv("PROXEUS_BENCH_MAX_SAMPLES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			b.Fatalf("PROXEUS_BENCH_MAX_SAMPLES: %v", err)
		}
		p.maxSamples = n
	}
	return p
}

const benchMetric = "bench_metric"

var benchQueries = []struct {
	name  string
	query string
}{
	// A raw selector: every backend streams its full series set through
	// proxeus's merge.
	{"raw", benchMetric},
	// An aggregation: proxeus pushes it down and each backend answers with a
	// single series, which the engine re-combines.
	{"pushdown", "sum(rate(" + benchMetric + "[5m]))"},
}

func BenchmarkFakepromQueryRange(b *testing.B) {
	// ApplyConfig logs at info level; those lines interleave with the
	// benchmark output and break its line format.
	logrus.SetLevel(logrus.WarnLevel)

	p := loadBenchParams(b)
	steps := int(p.rangeLen/p.step) + 1

	for _, series := range []int{1000, 10000, 100000} {
		for _, groups := range []int{1, 2} {
			for _, dedup := range []bool{false, true} {
				// cross_group_dedup is a no-op with a single group.
				if dedup && groups == 1 {
					continue
				}
				for _, q := range benchQueries {
					name := fmt.Sprintf("series=%d/groups=%d/dedup=%t/query=%s", series, groups, dedup, q.name)
					b.Run(name, func(b *testing.B) {
						if q.name == "raw" {
							if worst := series * groups * steps; worst > p.maxSamples {
								b.Skipf("worst-case response is %d samples > PROXEUS_BENCH_MAX_SAMPLES=%d", worst, p.maxSamples)
							}
						}
						benchQueryRange(b, p, series, groups, dedup, q.query)
					})
				}
			}
		}
	}
}

func benchQueryRange(b *testing.B, p benchParams, series, groups int, dedup bool, query string) {
	targets := make([]string, groups)
	backends := make([]*httptest.Server, groups)
	defer func() {
		for _, backend := range backends {
			backend.Close()
		}
	}()
	for i := range targets {
		backends[i] = httptest.NewServer(fakeprom.New(fakeprom.Config{
			Series:          series,
			Instance:        i,
			OverlapFraction: p.overlap,
			MetricName:      benchMetric,
		}))
		targets[i] = backends[i].Listener.Addr().String()
	}

	ps := getProxyStorage(benchPSConfig(targets, dedup))
	defer ps.Close()
	eng := testAPIEngine()
	eng.NodeReplacer = ps.NodeReplacer

	proxy := httptest.NewServer(newAPIHandler(ps, eng, "127.0.0.1:0"))
	defer proxy.Close()

	// A fixed, whole-second aligned window keeps every iteration identical.
	end := time.Unix(1600000000, 0)
	args := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(end.Add(-p.rangeLen).Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {strconv.FormatFloat(p.step.Seconds(), 'f', -1, 64)},
	}
	reqURL := proxy.URL + "/api/v1/query_range?" + args.Encode()

	// Warm up once and count what a response actually carries, so the
	// samples/s metric below is measured rather than assumed.
	gotSeries, gotSamples := countResponse(b, reqURL)
	if gotSeries == 0 {
		b.Fatalf("warmup returned no series")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(reqURL)
		if err != nil {
			b.Fatalf("query_range: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			b.Fatalf("query_range: status %d: %.200s", resp.StatusCode, body)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatalf("reading body: %v", err)
		}
		resp.Body.Close()
	}
	b.StopTimer()

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(gotSamples*b.N)/elapsed, "samples/s")
	b.ReportMetric(float64(gotSeries*b.N)/elapsed, "series/s")
}

// countResponse issues one request and reports how many series and samples the
// response carries, decoding it the same way proxeus decodes its backends.
func countResponse(b *testing.B, reqURL string) (series, samples int) {
	b.Helper()
	resp, err := http.Get(reqURL)
	if err != nil {
		b.Fatalf("query_range: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		b.Fatalf("reading body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		b.Fatalf("query_range: status %d: %.200s", resp.StatusCode, body)
	}

	ss := promapi.DecodeSeriesSet(body)
	for ss.Next() {
		series++
		it := ss.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			samples++
		}
	}
	if err := ss.Err(); err != nil {
		b.Fatalf("decoding response: %v", err)
	}
	return series, samples
}

// benchPSConfig is the proxeus config for one server_group per target, each
// tagged with its own external label so cross-group dedup has something to key
// on.
func benchPSConfig(targets []string, dedup bool) string {
	var sb strings.Builder
	sb.WriteString("proxeus:\n")
	if dedup {
		sb.WriteString("  cross_group_dedup: true\n")
	}
	sb.WriteString("  server_groups:\n")
	for i, target := range targets {
		fmt.Fprintf(&sb, `    - static_configs:
        - targets:
          - %s
      labels:
        backend: b%d
`, target, i)
	}
	return sb.String()
}
