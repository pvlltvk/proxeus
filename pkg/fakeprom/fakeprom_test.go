package fakeprom

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
)

// matrixResponse / vectorResponse / stringsResponse decode API bodies through
// prometheus/common's own types, so a shape the real client can't read fails
// the test.
type matrixResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string       `json:"resultType"`
		Result     model.Matrix `json:"result"`
	} `json:"data"`
}

type vectorResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string       `json:"resultType"`
		Result     model.Vector `json:"result"`
	} `json:"data"`
}

type stringsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

type seriesResponse struct {
	Status string           `json:"status"`
	Data   []model.LabelSet `json:"data"`
}

func do(t *testing.T, h http.Handler, path string, args url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(args.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body.String())
	}
	return rec
}

func decodeMatrix(t *testing.T, rec *httptest.ResponseRecorder) model.Matrix {
	t.Helper()
	var resp matrixResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v (body %.200s)", err, rec.Body.String())
	}
	if resp.Status != "success" || resp.Data.ResultType != "matrix" {
		t.Fatalf("unexpected envelope: status=%q resultType=%q", resp.Status, resp.Data.ResultType)
	}
	return resp.Data.Result
}

func rangeArgs(start, end time.Time, step time.Duration, query string) url.Values {
	return url.Values{
		"query": {query},
		"start": {formatTime(start)},
		"end":   {formatTime(end)},
		"step":  {step.String()},
	}
}

func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func TestQueryRange(t *testing.T) {
	start := time.Unix(1600000000, 0)

	for _, tc := range []struct {
		name        string
		cfg         Config
		query       string
		rangeLen    time.Duration
		step        time.Duration
		wantSeries  int
		wantSamples int
	}{
		{
			name:        "full series set",
			cfg:         Config{Series: 7},
			query:       `fake_metric{job="fake"}`,
			rangeLen:    time.Hour,
			step:        15 * time.Second,
			wantSeries:  7,
			wantSamples: 241,
		},
		{
			name:        "aggregation collapses to one series",
			cfg:         Config{Series: 7},
			query:       `sum(rate(fake_metric[5m]))`,
			rangeLen:    time.Hour,
			step:        time.Minute,
			wantSeries:  1,
			wantSamples: 61,
		},
		{
			name:        "sample cap",
			cfg:         Config{Series: 3, MaxSamplesPerSeries: 10},
			query:       "fake_metric",
			rangeLen:    time.Hour,
			step:        15 * time.Second,
			wantSeries:  3,
			wantSamples: 10,
		},
		{
			name:        "single step",
			cfg:         Config{Series: 2},
			query:       "fake_metric",
			rangeLen:    0,
			step:        15 * time.Second,
			wantSeries:  2,
			wantSamples: 1,
		},
		{
			name:       "no series",
			cfg:        Config{},
			query:      "fake_metric",
			rangeLen:   time.Hour,
			step:       time.Minute,
			wantSeries: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New(tc.cfg)
			end := start.Add(tc.rangeLen)
			m := decodeMatrix(t, do(t, h, "/api/v1/query_range", rangeArgs(start, end, tc.step, tc.query)))

			if len(m) != tc.wantSeries {
				t.Fatalf("series: got %d want %d", len(m), tc.wantSeries)
			}
			for _, ss := range m {
				if len(ss.Values) != tc.wantSamples {
					t.Fatalf("%s: samples: got %d want %d", ss.Metric, len(ss.Values), tc.wantSamples)
				}
				for i, v := range ss.Values {
					want := model.Time(start.Add(time.Duration(i) * tc.step).UnixMilli())
					if v.Timestamp != want {
						t.Fatalf("%s: sample %d: ts %d want %d", ss.Metric, i, v.Timestamp, want)
					}
				}
			}
		})
	}
}

func TestQueryRangeLabelShape(t *testing.T) {
	h := New(Config{Series: 40, MetricName: "custom_metric"})
	start := time.Unix(1600000000, 0)
	m := decodeMatrix(t, do(t, h, "/api/v1/query_range", rangeArgs(start, start, time.Minute, "custom_metric")))

	seen := map[string]bool{}
	for _, ss := range m {
		if got := string(ss.Metric[model.MetricNameLabel]); got != "custom_metric" {
			t.Fatalf("__name__: got %q", got)
		}
		if len(ss.Metric) != len(labelNames) {
			t.Fatalf("%s: got %d labels want %d", ss.Metric, len(ss.Metric), len(labelNames))
		}
		if seen[ss.Metric.String()] {
			t.Fatalf("duplicate series %s", ss.Metric)
		}
		seen[ss.Metric.String()] = true
	}
	if len(seen) != 40 {
		t.Fatalf("distinct series: got %d want 40", len(seen))
	}
}

func TestInstantQuery(t *testing.T) {
	h := New(Config{Series: 5})
	ts := time.Unix(1600000000, 0)
	args := url.Values{"query": {"fake_metric"}, "time": {formatTime(ts)}}

	var resp vectorResponse
	rec := do(t, h, "/api/v1/query", args)
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Data.ResultType != "vector" || len(resp.Data.Result) != 5 {
		t.Fatalf("got resultType=%q %d samples", resp.Data.ResultType, len(resp.Data.Result))
	}
	for _, s := range resp.Data.Result {
		if s.Timestamp != model.Time(ts.UnixMilli()) {
			t.Fatalf("%s: ts %d want %d", s.Metric, s.Timestamp, ts.UnixMilli())
		}
	}

	// A range selector is answered with a matrix of raw samples, as
	// Prometheus does — this is proxeus's raw-data fetch path.
	args.Set("query", `{__name__="fake_metric"}[60s]`)
	m := decodeMatrix(t, do(t, h, "/api/v1/query", args))
	if len(m) != 5 {
		t.Fatalf("matrix series: got %d want 5", len(m))
	}
	if got := len(m[0].Values); got != 5 {
		t.Fatalf("raw samples over 60s at %s: got %d want 5", rawStep, got)
	}
}

// TestOverlapExact is the contract proxeus's cross-group dedup relies on: two
// instances sharing exactly round(OverlapFraction*Series) series, identical in
// both labels and values, and nothing else.
func TestOverlapExact(t *testing.T) {
	for _, tc := range []struct {
		overlap    float64
		series     int
		wantShared int
	}{
		{0, 100, 0},
		{0.25, 100, 25},
		{1, 100, 100},
		{0.33, 99, 33},
	} {
		t.Run(fmt.Sprintf("overlap=%v/series=%d", tc.overlap, tc.series), func(t *testing.T) {
			start := time.Unix(1600000000, 0)
			args := rangeArgs(start, start.Add(time.Minute), 30*time.Second, "fake_metric")

			byMetric := make([]map[string]string, 2)
			for inst := 0; inst < 2; inst++ {
				h := New(Config{Series: tc.series, Instance: inst, OverlapFraction: tc.overlap})
				m := decodeMatrix(t, do(t, h, "/api/v1/query_range", args))
				if len(m) != tc.series {
					t.Fatalf("instance %d: got %d series want %d", inst, len(m), tc.series)
				}
				byMetric[inst] = make(map[string]string, len(m))
				for _, ss := range m {
					byMetric[inst][ss.Metric.String()] = ss.Values[0].Value.String() + " " + ss.Values[1].Value.String()
				}
			}

			shared := 0
			for metric, values := range byMetric[0] {
				other, ok := byMetric[1][metric]
				if !ok {
					continue
				}
				shared++
				if other != values {
					t.Fatalf("shared series %s: values %q vs %q", metric, values, other)
				}
			}
			if shared != tc.wantShared {
				t.Fatalf("shared series: got %d want %d", shared, tc.wantShared)
			}
		})
	}
}

func TestGzip(t *testing.T) {
	h := New(Config{Series: 20})
	start := time.Unix(1600000000, 0)
	args := rangeArgs(start, start.Add(time.Hour), 15*time.Second, "fake_metric")

	plain := do(t, h, "/api/v1/query_range", args)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/query_range", strings.NewReader(args.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding: got %q want gzip", got)
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	body, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if string(body) != plain.Body.String() {
		t.Fatalf("gzipped body differs from plain body")
	}
	if rec.Body.Len() >= len(body) {
		t.Fatalf("gzipped body (%d) not smaller than plain (%d)", rec.Body.Len(), len(body))
	}
}

// TestMetadataEndpointsAgree checks that /series, /labels and
// /label/<name>/values describe the same series set.
func TestMetadataEndpointsAgree(t *testing.T) {
	h := New(Config{Series: 50, Instance: 3, OverlapFraction: 0.5})

	var series seriesResponse
	if err := json.Unmarshal(do(t, h, "/api/v1/series", nil).Body.Bytes(), &series); err != nil {
		t.Fatalf("decode series: %v", err)
	}
	if len(series.Data) != 50 {
		t.Fatalf("series: got %d want 50", len(series.Data))
	}

	want := map[string]map[string]bool{}
	for _, ls := range series.Data {
		for name, value := range ls {
			if want[string(name)] == nil {
				want[string(name)] = map[string]bool{}
			}
			want[string(name)][string(value)] = true
		}
	}

	var names stringsResponse
	if err := json.Unmarshal(do(t, h, "/api/v1/labels", nil).Body.Bytes(), &names); err != nil {
		t.Fatalf("decode labels: %v", err)
	}
	if len(names.Data) != len(want) {
		t.Fatalf("label names: got %v want %d names", names.Data, len(want))
	}

	for _, name := range names.Data {
		var values stringsResponse
		rec := do(t, h, "/api/v1/label/"+name+"/values", nil)
		if err := json.Unmarshal(rec.Body.Bytes(), &values); err != nil {
			t.Fatalf("decode values of %s: %v", name, err)
		}
		if len(values.Data) != len(want[name]) {
			t.Fatalf("%s: got %d values want %d", name, len(values.Data), len(want[name]))
		}
		for _, v := range values.Data {
			if !want[name][v] {
				t.Fatalf("%s: value %q not present in any series", name, v)
			}
		}
	}
}

func TestIsAggregation(t *testing.T) {
	for query, want := range map[string]bool{
		`sum(rate(fake_metric[5m]))`: true,
		`sum by (job) (fake_metric)`: true,
		`  count(fake_metric)`:       true,
		`topk(5, fake_metric)`:       true,
		`fake_metric`:                false,
		`sum_total{job="fake"}`:      false,
		`rate(fake_metric[5m])`:      false,
		`fake_metric / sum(other)`:   false,
		`histogram_quantile(0.9, x)`: false,
		`{__name__="fake_metric"}`:   false,
		``:                           false,
	} {
		if got := isAggregation(query); got != want {
			t.Errorf("isAggregation(%q) = %v want %v", query, got, want)
		}
	}
}

func TestBadParams(t *testing.T) {
	h := New(Config{Series: 1})
	for _, args := range []url.Values{
		{"query": {"fake_metric"}, "start": {"0"}, "end": {"60"}, "step": {"0"}},
		{"query": {"fake_metric"}, "start": {"60"}, "end": {"0"}, "step": {"15"}},
		{"query": {"fake_metric"}, "start": {"nope"}, "end": {"60"}, "step": {"15"}},
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/query_range", strings.NewReader(args.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%v: status %d want 400", args, rec.Code)
		}
		var resp struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || resp.Status != "error" {
			t.Fatalf("%v: body %q (err %v)", args, rec.Body.String(), err)
		}
	}
}

func TestHealthAndBuildinfo(t *testing.T) {
	h := New(Config{Series: 1})
	for _, path := range []string{"/-/healthy", "/-/ready"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", path, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/status/buildinfo", nil))
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode buildinfo: %v", err)
	}
	if resp.Status != "success" || resp.Data.Version == "" {
		t.Fatalf("buildinfo: %s", rec.Body.String())
	}
}

func TestLatency(t *testing.T) {
	h := New(Config{Series: 1, Latency: 20 * time.Millisecond})
	start := time.Now()
	do(t, h, "/api/v1/query_range", rangeArgs(time.Unix(0, 0), time.Unix(60, 0), time.Minute, "fake_metric"))
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("response took %s, want at least the configured 20ms", elapsed)
	}
}

// TestConcurrentRequests exercises the pooled encoders (and their reused gzip
// writers) from several goroutines at once; run under -race it is the check
// that pooling did not introduce sharing bugs.
func TestConcurrentRequests(t *testing.T) {
	h := New(Config{Series: 30})
	start := time.Unix(1600000000, 0)
	args := rangeArgs(start, start.Add(10*time.Minute), 15*time.Second, "fake_metric")
	want := do(t, h, "/api/v1/query_range", args).Body.String()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(gzipped bool) {
			defer wg.Done()
			for n := 0; n < 5; n++ {
				req := httptest.NewRequest(http.MethodPost, "/api/v1/query_range", strings.NewReader(args.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				if gzipped {
					req.Header.Set("Accept-Encoding", "gzip")
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)

				var body io.Reader = rec.Body
				if gzipped {
					zr, err := gzip.NewReader(rec.Body)
					if err != nil {
						t.Errorf("gzip reader: %v", err)
						return
					}
					body = zr
				}
				got, err := io.ReadAll(body)
				if err != nil {
					t.Errorf("read body: %v", err)
					return
				}
				if string(got) != want {
					t.Errorf("response differs from the sequential one")
					return
				}
			}
		}(i%2 == 0)
	}
	wg.Wait()
}
