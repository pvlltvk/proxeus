package promclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/pvlltvk/proxeus/pkg/promapi"
)

func TestDecodeExportSeriesSet(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		want    map[string]string
		wantErr string
	}{
		{
			name: "one line per series",
			body: `{"metric":{"__name__":"up","job":"a"},"values":[1,0.5],"timestamps":[1725000000000,1725000015000]}
{"metric":{"__name__":"up","job":"b"},"values":[0],"timestamps":[1725000000000]}
`,
			want: map[string]string{
				`{__name__="up", job="a"}`: "1725000000000=1 1725000015000=0.5 ",
				`{__name__="up", job="b"}`: "1725000000000=0 ",
			},
		},
		{
			name: "several blocks of the same series, out of order",
			body: `{"metric":{"job":"a","__name__":"up"},"values":[3],"timestamps":[1725000030000]}
{"metric":{"__name__":"up","job":"a"},"values":[1,2],"timestamps":[1725000000000,1725000015000]}
`,
			want: map[string]string{
				`{__name__="up", job="a"}`: "1725000000000=1 1725000015000=2 1725000030000=3 ",
			},
		},
		{
			name: "duplicate timestamps keep the last exported",
			body: `{"metric":{"__name__":"up"},"values":[1,2],"timestamps":[1725000000000,1725000000000]}
{"metric":{"__name__":"up"},"values":[3],"timestamps":[1725000000000]}
`,
			want: map[string]string{
				`{__name__="up"}`: "1725000000000=3 ",
			},
		},
		{
			name: "special values",
			body: `{"metric":{"__name__":"up"},"values":[null,"Infinity","-Infinity","NaN"],"timestamps":[1,2,3,4]}
`,
			want: map[string]string{
				`{__name__="up"}`: "1=NaN 2=+Inf 3=-Inf 4=NaN ",
			},
		},
		{
			name: "blank lines are skipped",
			body: "\n{\"metric\":{\"__name__\":\"up\"},\"values\":[1],\"timestamps\":[5]}\n\n",
			want: map[string]string{`{__name__="up"}`: "5=1 "},
		},
		{
			name: "empty body",
			body: "",
			want: map[string]string{},
		},
		{
			name:    "malformed line",
			body:    `{"metric":{"__name__":"up"},"values":[1,`,
			wantErr: "malformed export line",
		},
		{
			name:    "values and timestamps disagree",
			body:    `{"metric":{"__name__":"up"},"values":[1,2],"timestamps":[1]}`,
			wantErr: "2 values but 1 timestamps",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ss := decodeExportSeriesSet(strings.NewReader(tc.body))
			if tc.wantErr != "" {
				if err := ss.Err(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if got := dumpSS(t, ss); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("series mismatch\nexpected=%v\nactual=%v", tc.want, got)
			}
			if err := ss.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestDecodeExportSeriesSetLargeLine covers a line well past bufio.Scanner's
// default 64KiB limit -- a block of a busy series easily exceeds it.
func TestDecodeExportSeriesSetLargeLine(t *testing.T) {
	const samples = 20000
	body := buildExportBody(1, samples)
	if len(body) < 64<<10 {
		t.Fatalf("test body is only %d bytes, too small to cover the scanner limit", len(body))
	}

	ss := decodeExportSeriesSet(bytes.NewReader(body))
	if !ss.Next() {
		t.Fatalf("no series decoded: %v", ss.Err())
	}
	n := 0
	it := ss.At().Iterator(nil)
	for it.Next() != chunkenc.ValNone {
		n++
	}
	if n != samples {
		t.Fatalf("samples: got %d want %d", n, samples)
	}
	if ss.Next() {
		t.Fatal("expected exactly one series")
	}
}

// TestVMExportGetValueRequest checks the export call: endpoint, selector, the
// window PromAPIV1.GetValue would have asked for, and the VM-specific params.
func TestVMExportGetValueRequest(t *testing.T) {
	var got url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		got = r.Form
		got.Set("__path__", r.URL.Path)
		_, _ = w.Write([]byte(`{"metric":{"__name__":"up"},"values":[1],"timestamps":[1725000000000]}` + "\n"))
	}))
	defer srv.Close()

	client, err := api.NewClient(api.Config{Address: srv.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	a := &PromAPIVMExport{
		API:            &PromAPIV1{API: v1.NewAPI(client), Client: client},
		Client:         client,
		MaxRowsPerLine: 1000,
	}

	start := time.Unix(1725000000, 0)
	end := time.Unix(1725003600, 0)
	ss := a.GetValue(context.Background(), start, end, []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"),
		labels.MustNewMatcher(labels.MatchRegexp, "job", "a.*"),
	})
	if err := ss.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := map[string]string{`{__name__="up"}`: "1725000000000=1 "}; !reflect.DeepEqual(dumpSS(t, ss), want) {
		t.Fatalf("series mismatch, want %v", want)
	}

	want := url.Values{
		"__path__":          {epVMExport},
		"match[]":           {`{__name__="up",job=~"a.*"}`},
		"start":             {"1724999999"}, // end - (3600s + 1s of rounding slack)
		"end":               {"1725003600"},
		"reduce_mem_usage":  {"1"},
		"max_rows_per_line": {"1000"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("request mismatch\nexpected=%v\nactual=%v", want, got)
	}
}

func TestVMExportGetValueError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"status":"error","errorType":"422","error":"cannot parse match[]"}`))
	}))
	defer srv.Close()

	client, err := api.NewClient(api.Config{Address: srv.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	a := &PromAPIVMExport{API: &PromAPIV1{API: v1.NewAPI(client), Client: client}, Client: client}

	ss := a.GetValue(context.Background(), time.Unix(0, 0), time.Unix(60, 0), nil)
	if err := ss.Err(); err == nil || !strings.Contains(err.Error(), "cannot parse match[]") {
		t.Fatalf("expected the downstream error to surface, got %v", err)
	}
}

// TestVMExportMatchesQueryPath is the correctness contract: for the same data,
// the export path must hand back exactly what the /api/v1/query range-selector
// path does -- even though export splits series over blocks, emits them in no
// particular order and encodes special values differently.
func TestVMExportMatchesQueryPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == epVMExport {
			_, _ = w.Write([]byte(exportBody))
			return
		}
		_, _ = w.Write([]byte(matrixBody))
	}))
	defer srv.Close()

	client, err := api.NewClient(api.Config{Address: srv.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	queryAPI := &PromAPIV1{API: v1.NewAPI(client), Client: client}
	exportAPI := &PromAPIVMExport{API: queryAPI, Client: client}

	start, end := time.Unix(1725000000, 0), time.Unix(1725000030, 0)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")}

	want := dumpSS(t, queryAPI.GetValue(context.Background(), start, end, matchers))
	got := dumpSS(t, exportAPI.GetValue(context.Background(), start, end, matchers))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("export/query mismatch\nquery =%v\nexport=%v", want, got)
	}
}

// The same three series, as the v1 API serves them (one matrix, sorted) and as
// VictoriaMetrics exports them (NDJSON, split into blocks, unordered, NaN as
// null and the infinities as strings).
const (
	matrixBody = `{"status":"success","data":{"resultType":"matrix","result":[` +
		`{"metric":{"__name__":"up","job":"a"},"values":[[1725000000,"1"],[1725000015,"0.5"],[1725000030,"NaN"]]},` +
		`{"metric":{"__name__":"up","job":"b"},"values":[[1725000000,"+Inf"],[1725000015,"-Inf"]]},` +
		`{"metric":{"__name__":"up","job":"c"},"values":[[1725000030,"7"]]}` +
		`]}}`

	exportBody = `{"metric":{"__name__":"up","job":"b"},"values":["Infinity","-Infinity"],"timestamps":[1725000000000,1725000015000]}
{"metric":{"__name__":"up","job":"a"},"values":[null],"timestamps":[1725000030000]}
{"metric":{"__name__":"up","job":"c"},"values":[7],"timestamps":[1725000030000]}
{"metric":{"__name__":"up","job":"a"},"values":[1,0.5],"timestamps":[1725000000000,1725000015000]}
`
)

// buildExportBody renders n series of m samples as export NDJSON; the matching
// matrix JSON is buildMatrixBodyForExport below.
func buildExportBody(n, m int) []byte {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"metric":{"__name__":"m","instance":"inst-%d","job":"bench","series":"%d"},"values":[`, i%500, i)
		for j := 0; j < m; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatFloat(float64(i)+float64(j)/4, 'f', -1, 64))
		}
		b.WriteString(`],"timestamps":[`)
		for j := 0; j < m; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.FormatInt(1700000000000+int64(j)*15000, 10))
		}
		b.WriteString("]}\n")
	}
	return []byte(b.String())
}

func buildMatrixBodyForExport(n, m int) []byte {
	var b strings.Builder
	b.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"metric":{"__name__":"m","instance":"inst-%d","job":"bench","series":"%d"},"values":[`, i%500, i)
		for j := 0; j < m; j++ {
			if j > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, `[%d,"%s"]`, 1700000000+int64(j)*15, strconv.FormatFloat(float64(i)+float64(j)/4, 'f', -1, 64))
		}
		b.WriteString(`]}`)
	}
	b.WriteString(`]}}`)
	return []byte(b.String())
}

const (
	benchSeries  = 500
	benchSamples = 100
)

func BenchmarkDecodeExportSeriesSet(b *testing.B) {
	body := buildExportBody(benchSeries, benchSamples)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss := decodeExportSeriesSet(bytes.NewReader(body))
		for ss.Next() {
			_ = ss.At().Labels()
		}
	}
}

// BenchmarkDecodeMatrixSeriesSet decodes the same series and samples from the
// matrix JSON the /api/v1/query raw path returns -- the baseline the export
// path is meant to beat.
func BenchmarkDecodeMatrixSeriesSet(b *testing.B) {
	body := buildMatrixBodyForExport(benchSeries, benchSamples)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss := promapi.DecodeSeriesSet(body)
		for ss.Next() {
			_ = ss.At().Labels()
		}
	}
}
