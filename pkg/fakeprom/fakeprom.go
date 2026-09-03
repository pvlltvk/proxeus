// Package fakeprom implements a synthetic Prometheus HTTP API backend.
//
// It exists so proxeus can be load-tested on its own: real Thanos /
// VictoriaMetrics sandboxes saturate long before proxeus does, so the numbers
// measured through them describe the backend, not the proxy. fakeprom answers
// every query from a deterministic generator and streams the JSON response
// straight to the client (no model.Matrix is ever built), so it stays cheap and
// flat in memory at any cardinality.
//
// It is deliberately *not* a Prometheus: PromQL is not parsed (see
// isAggregation), matchers on /api/v1/series and the label endpoints are
// ignored, and every query returns the full configured series set.
package fakeprom

import (
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// defaultMetricName is the __name__ used when Config.MetricName is empty.
const defaultMetricName = "fake_metric"

// rawStep is the spacing of the samples returned for an instant query over a
// range selector (`metric[1h]`) — the shape proxeus asks for when it falls back
// to fetching raw data. It stands in for the backend's scrape interval.
const rawStep = 15 * time.Second

// Config configures the synthetic backend. The zero value serves an empty
// backend; only Series is really required.
type Config struct {
	// Series is the number of series the backend pretends to hold. Every
	// query returns all of them (or one, for an aggregation).
	Series int

	// Instance identifies this backend among several fakeproms. It only
	// affects the `instance` label of the non-shared series, see
	// OverlapFraction.
	Instance int

	// OverlapFraction is the fraction of Series that is *identical* across
	// instances: two fakeproms with the same Series and OverlapFraction but
	// different Instance serve exactly round(OverlapFraction*Series) series
	// with byte-identical labels and values, and disjoint label sets for the
	// rest (the `instance` label of a non-shared series carries the instance
	// id). That is what proxeus's cross-group dedup keys on. Clamped to
	// [0, 1] by New.
	OverlapFraction float64

	// MetricName is the __name__ of every generated series. Defaults to
	// fake_metric.
	MetricName string

	// Latency, when set, is slept before each API response, to model a
	// backend that is not instantaneous.
	Latency time.Duration

	// MaxSamplesPerSeries caps how many samples a range response carries per
	// series, bounding the response size for wide ranges / small steps.
	// 0 means uncapped.
	MaxSamplesPerSeries int
}

type handler struct {
	cfg Config
	// shared is the number of leading series indexes that are instance
	// independent, i.e. round(OverlapFraction*Series).
	shared int
}

// New returns the synthetic backend as an http.Handler.
func New(cfg Config) http.Handler {
	if cfg.MetricName == "" {
		cfg.MetricName = defaultMetricName
	}
	cfg.OverlapFraction = math.Min(math.Max(cfg.OverlapFraction, 0), 1)
	if cfg.Series < 0 {
		cfg.Series = 0
	}
	h := &handler{
		cfg:    cfg,
		shared: int(math.Round(cfg.OverlapFraction * float64(cfg.Series))),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", h.query)
	mux.HandleFunc("/api/v1/query_range", h.queryRange)
	mux.HandleFunc("/api/v1/series", h.series)
	mux.HandleFunc("/api/v1/labels", h.labels)
	mux.HandleFunc("/api/v1/label/{name}/values", h.labelValues)
	mux.HandleFunc("/api/v1/status/buildinfo", h.buildinfo)
	mux.HandleFunc("/-/healthy", h.healthy)
	mux.HandleFunc("/-/ready", h.healthy)
	return mux
}

func (h *handler) queryRange(w http.ResponseWriter, r *http.Request) {
	h.delay()
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	start, err := parseTimeParam(r, "start")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	end, err := parseTimeParam(r, "end")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	step, err := parseDurationParam(r, "step")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	if step <= 0 {
		writeAPIError(w, http.StatusBadRequest, "bad_data", "zero or negative query resolution step widths are not accepted")
		return
	}
	if end < start {
		writeAPIError(w, http.StatusBadRequest, "bad_data", "end timestamp must not be before start time")
		return
	}

	e := newEncoder(w, r)
	defer e.close()
	e.writeString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	h.writeMatrix(e, r.FormValue("query"), start, end, step)
	e.writeString(`]}}`)
}

func (h *handler) query(w http.ResponseWriter, r *http.Request) {
	h.delay()
	if err := r.ParseForm(); err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	ts, err := parseTimeParam(r, "time")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	if r.FormValue("time") == "" {
		ts = time.Now().UnixMilli()
	}
	query := r.FormValue("query")

	e := newEncoder(w, r)
	defer e.close()

	// An instant query over a range selector returns a matrix, exactly like
	// Prometheus does — that is how proxeus fetches raw samples when it
	// cannot push the query down.
	if window, ok := trailingRange(query); ok {
		e.writeString(`{"status":"success","data":{"resultType":"matrix","result":[`)
		h.writeMatrix(e, query, ts-window.Milliseconds(), ts, rawStep.Milliseconds())
		e.writeString(`]}}`)
		return
	}

	e.writeString(`{"status":"success","data":{"resultType":"vector","result":[`)
	if isAggregation(query) {
		h.writeVectorSeries(e, aggIndex, ts)
	} else {
		for i := 0; i < h.cfg.Series; i++ {
			if i > 0 {
				e.writeString(",")
			}
			h.writeVectorSeries(e, i, ts)
		}
	}
	e.writeString(`]}}`)
}

func (h *handler) series(w http.ResponseWriter, r *http.Request) {
	h.delay()
	e := newEncoder(w, r)
	defer e.close()
	e.writeString(`{"status":"success","data":[`)
	for i := 0; i < h.cfg.Series; i++ {
		if i > 0 {
			e.writeString(",")
		}
		e.buf = h.appendMetric(e.buf, i)
		e.maybeFlush()
	}
	e.writeString(`]}`)
}

func (h *handler) labels(w http.ResponseWriter, r *http.Request) {
	h.delay()
	if h.cfg.Series == 0 {
		writeStrings(w, r, nil)
		return
	}
	writeStrings(w, r, labelNames)
}

func (h *handler) labelValues(w http.ResponseWriter, r *http.Request) {
	h.delay()
	writeStrings(w, r, h.valuesOf(r.PathValue("name")))
}

// valuesOf returns the values the given label takes over the generated series,
// in sorted order (as Prometheus does).
func (h *handler) valuesOf(name string) []string {
	if h.cfg.Series == 0 {
		return nil
	}
	switch name {
	case "__name__":
		return []string{h.cfg.MetricName}
	case "job":
		return []string{"fake"}
	case "env":
		return []string{"prod"}
	case "region":
		return prefixedInts("r-", min(regionCount, h.cfg.Series))
	case "shard":
		return prefixedInts("", min(shardCount, h.cfg.Series))
	case "instance":
		out := make([]string, h.cfg.Series)
		for i := range out {
			out[i] = string(h.appendInstance(nil, i))
		}
		slices.Sort(out)
		return out
	default:
		return nil
	}
}

func (h *handler) buildinfo(w http.ResponseWriter, r *http.Request) {
	h.delay()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"status":"success","data":{"version":"3.5.0","revision":"fakeprom","branch":"fakeprom","buildUser":"fakeprom","buildDate":"","goVersion":"fakeprom"}}`)
}

func (h *handler) healthy(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.WriteString(w, "Fakeprom is Healthy.\n")
}

func (h *handler) delay() {
	if h.cfg.Latency > 0 {
		time.Sleep(h.cfg.Latency)
	}
}

// aggIndex is the series index used for the single series returned for an
// aggregation query.
const aggIndex = -1

// writeMatrix streams the matrix `result` array (without the enclosing
// brackets) for the given query over [start, end] at step, all in milliseconds.
func (h *handler) writeMatrix(e *encoder, query string, start, end, step int64) {
	if isAggregation(query) {
		h.writeMatrixSeries(e, aggIndex, start, end, step)
		return
	}
	for i := 0; i < h.cfg.Series; i++ {
		if i > 0 {
			e.writeString(",")
		}
		h.writeMatrixSeries(e, i, start, end, step)
	}
}

func (h *handler) writeMatrixSeries(e *encoder, i int, start, end, step int64) {
	e.writeString(`{"metric":`)
	e.buf = h.appendMetric(e.buf, i)
	e.writeString(`,"values":[`)
	n := 0
	for ts := start; ts <= end; ts += step {
		if h.cfg.MaxSamplesPerSeries > 0 && n >= h.cfg.MaxSamplesPerSeries {
			break
		}
		if n > 0 {
			e.buf = append(e.buf, ',')
		}
		e.buf = appendSample(e.buf, ts, sampleValue(i, ts))
		e.maybeFlush()
		n++
	}
	e.writeString(`]}`)
}

func (h *handler) writeVectorSeries(e *encoder, i int, ts int64) {
	e.writeString(`{"metric":`)
	e.buf = h.appendMetric(e.buf, i)
	e.writeString(`,"value":`)
	e.buf = appendSample(e.buf, ts, sampleValue(i, ts))
	e.writeString(`}`)
}

const (
	shardCount  = 16
	regionCount = 4
)

// labelNames are the labels every generated series carries, sorted.
var labelNames = []string{"__name__", "env", "instance", "job", "region", "shard"}

// appendMetric appends series i's labels as a JSON object. Index aggIndex is
// the aggregation result, which carries no labels (as `sum(...)` in Prometheus
// does not). No escaping is needed: every generated name and value is plain
// ASCII.
func (h *handler) appendMetric(b []byte, i int) []byte {
	if i == aggIndex {
		return append(b, '{', '}')
	}
	b = append(b, `{"__name__":"`...)
	b = append(b, h.cfg.MetricName...)
	b = append(b, `","env":"prod","instance":"`...)
	b = h.appendInstance(b, i)
	b = append(b, `","job":"fake","region":"r-`...)
	b = strconv.AppendInt(b, int64(i%regionCount), 10)
	b = append(b, `","shard":"`...)
	b = strconv.AppendInt(b, int64(i%shardCount), 10)
	b = append(b, `"}`...)
	return b
}

// appendInstance appends series i's `instance` value: "i-<i>" for the shared
// series (identical on every instance) and "i-<instance>-<i>" for the rest
// (disjoint between instances).
func (h *handler) appendInstance(b []byte, i int) []byte {
	b = append(b, "i-"...)
	if i >= h.shared {
		b = strconv.AppendInt(b, int64(h.cfg.Instance), 10)
		b = append(b, '-')
	}
	return strconv.AppendInt(b, int64(i), 10)
}

// sampleValue is the value of series i at ts (milliseconds). It deliberately
// does not depend on the instance id, so shared series carry identical values
// across instances.
func sampleValue(i int, ts int64) float64 {
	if i == aggIndex {
		i = 0
	}
	return float64(i%1000) + float64((ts/1000)%60)/4
}

// appendSample appends a `[<unix-seconds>, "<value>"]` pair.
func appendSample(b []byte, ts int64, v float64) []byte {
	b = append(b, '[')
	b = strconv.AppendFloat(b, float64(ts)/1000, 'f', -1, 64)
	b = append(b, ',', '"')
	b = strconv.AppendFloat(b, v, 'f', -1, 64)
	return append(b, '"', ']')
}

// aggregations are the PromQL aggregation operators isAggregation recognizes.
var aggregations = map[string]struct{}{
	"sum": {}, "count": {}, "avg": {}, "min": {}, "max": {},
	"group": {}, "topk": {}, "bottomk": {}, "stddev": {}, "stdvar": {},
	"quantile": {}, "count_values": {},
}

// isAggregation reports whether the query looks like a top-level aggregation,
// in which case the backend answers with a single series — the shape a
// Prometheus would return, and what proxeus's aggregation pushdown expects.
//
// This is a heuristic, not a parser: it takes the leading PromQL identifier and
// looks it up in the aggregation-operator set. So `sum(rate(x[5m]))`,
// `sum by (job) (x)` and `count(x)` aggregate, while `x`, `sum_total{job="a"}`
// and `rate(x[5m])` return the full series set. An aggregation appearing
// anywhere but at the start of the query is not recognized.
func isAggregation(query string) bool {
	query = strings.TrimLeft(query, " \t\n")
	end := 0
	for end < len(query) && isIdentChar(query[end]) {
		end++
	}
	_, ok := aggregations[query[:end]]
	return ok
}

func isIdentChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == ':'
}

// trailingRange returns the window of a query ending in a range selector
// (`foo[1h]`, `{__name__="foo"}[3601s]`), which Prometheus answers with a
// matrix of raw samples.
func trailingRange(query string) (time.Duration, bool) {
	query = strings.TrimRight(query, " \t\n")
	if !strings.HasSuffix(query, "]") {
		return 0, false
	}
	open := strings.LastIndexByte(query, '[')
	if open < 0 {
		return 0, false
	}
	d, err := time.ParseDuration(query[open+1 : len(query)-1])
	if err != nil || d <= 0 {
		return 0, false
	}
	return d, true
}
