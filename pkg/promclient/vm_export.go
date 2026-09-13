package promclient

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/api"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

const epVMExport = "/api/v1/export"

// exportMaxLineBytes caps a single NDJSON line. One line carries one series
// over the queried range, so a long range at a short scrape interval is what
// drives the size; `export_max_rows_per_line` splits such a series across
// several lines and is the way out if one ever exceeds this.
const exportMaxLineBytes = 16 << 20

var exportJSON = jsoniter.ConfigCompatibleWithStandardLibrary

// PromAPIVMExport implements raw fetches (GetValue) against VictoriaMetrics'
// /api/v1/export endpoint, which streams NDJSON (one line per series block)
// instead of the quoted-string matrix JSON of /api/v1/query. Everything else is
// served by the embedded API.
//
// Selected by `victoriametrics: {raw_fetch: export}`; VictoriaMetrics does not
// implement remote_read, so this is the only cheap raw path it offers.
type PromAPIVMExport struct {
	API
	// Client is the transport the export call is made on (the same one the
	// embedded API uses, so path_prefix, auth, headers and dialect query
	// params all apply).
	Client api.Client
	// MaxRowsPerLine is sent as `max_rows_per_line`; 0 leaves the line size to
	// the downstream.
	MaxRowsPerLine int
}

// GetValue loads the raw data for a given set of matchers in the time range.
//
// NOTE: /api/v1/export applies neither the lookback-delta nor
// -search.latencyOffset, so the freshest ~30s may contain samples that
// VictoriaMetrics' own /api/v1/query would still hide.
func (p *PromAPIVMExport) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	selector, err := promhttputil.MatcherToString(matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	u := p.Client.URL(epVMExport, nil)
	args := u.Query()
	args.Set("match[]", selector)
	// The same window PromAPIV1.GetValue asks for through its range selector:
	// end minus the range, where the added second deals with float rounding
	// when casting to int64. Export's start is inclusive (a range selector's is
	// not), so this is a superset by at most the sample on the boundary.
	seconds := int64(end.Sub(start).Seconds()) + 1
	args.Set("start", formatAPITime(end.Add(-time.Duration(seconds)*time.Second)))
	args.Set("end", formatAPITime(end))
	// reduce_mem_usage=1 is deliberately NOT sent. It switches VictoriaMetrics
	// to streaming raw blocks straight from storage, which skips the
	// deduplication its select path applies (-dedup.minScrapeInterval): under HA
	// ingestion that returns every replica's samples, so count_over_time, rate
	// and friends would disagree with the /api/v1/query this fetch stands in
	// for. Leaving it off keeps export on the same select path as a query, for
	// the same vmselect memory a query over that range already costs.
	if p.MaxRowsPerLine > 0 {
		args.Set("max_rows_per_line", strconv.Itoa(p.MaxRowsPerLine))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(args.Encode()))
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, body, err := p.Client.Do(ctx, req)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if resp.StatusCode != http.StatusOK {
		// export doesn't answer errors in the {"status":"error"} envelope the
		// v1 API uses, so surface the body as-is (bounded: a proxy in between
		// may answer with a whole HTML page).
		msg := strings.TrimSpace(string(body))
		if len(msg) > 256 {
			msg = msg[:256] + "..."
		}
		return storage.ErrSeriesSet(fmt.Errorf("export returned %s: %s", resp.Status, msg))
	}
	return decodeExportSeriesSet(bytes.NewReader(body))
}

// exportSeries accumulates the samples of one labelset across the lines it was
// exported in.
type exportSeries struct {
	lbls    labels.Labels
	samples []chunks.Sample
}

// decodeExportSeriesSet decodes a VictoriaMetrics /api/v1/export NDJSON stream:
//
//	{"metric":{...},"values":[1,2],"timestamps":[1725000000000,1725000015000]}
//
// One labelset spans several lines when max_rows_per_line splits it, so lines
// are grouped by labelset and their samples merged. The result is ordered by
// labels, since the order series come back in is unspecified.
func decodeExportSeriesSet(r io.Reader) storage.SeriesSet {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64<<10), exportMaxLineBytes)

	var (
		series  []*exportSeries
		byLabel = map[string]*exportSeries{}
		builder labels.ScratchBuilder
		keyBuf  []byte
	)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		lbls, samples, err := decodeExportLine(line, &builder)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		keyBuf = lbls.Bytes(keyBuf[:0])
		s, ok := byLabel[string(keyBuf)]
		if !ok {
			s = &exportSeries{lbls: lbls}
			byLabel[string(keyBuf)] = s
			series = append(series, s)
		}
		s.samples = append(s.samples, samples...)
	}
	if err := scanner.Err(); err != nil {
		return storage.ErrSeriesSet(fmt.Errorf("reading export response: %w", err))
	}

	sort.Slice(series, func(i, j int) bool { return labels.Compare(series[i].lbls, series[j].lbls) < 0 })
	out := make([]storage.Series, len(series))
	for i, s := range series {
		out[i] = promapi.NewSeries(s.lbls, sortExportSamples(s.samples))
	}
	return promapi.NewSeriesSet(out, nil, nil)
}

// decodeExportLine decodes one NDJSON line into its labelset and samples. The
// builder is reused across lines.
func decodeExportLine(line []byte, builder *labels.ScratchBuilder) (labels.Labels, []chunks.Sample, error) {
	iter := exportJSON.BorrowIterator(line)
	defer exportJSON.ReturnIterator(iter)

	builder.Reset()
	var (
		values     []float64
		timestamps []int64
	)
	for k := iter.ReadObject(); k != ""; k = iter.ReadObject() {
		switch k {
		case "metric":
			for name := iter.ReadObject(); name != ""; name = iter.ReadObject() {
				builder.Add(name, iter.ReadString())
			}
		case "values":
			for iter.ReadArray() {
				values = append(values, readExportValue(iter))
			}
		case "timestamps":
			for iter.ReadArray() {
				timestamps = append(timestamps, iter.ReadInt64())
			}
		default:
			iter.Skip()
		}
	}
	if iter.Error != nil && !errors.Is(iter.Error, io.EOF) {
		return labels.EmptyLabels(), nil, fmt.Errorf("malformed export line: %w", iter.Error)
	}
	// ReadObject stops at the closing brace without consuming what follows, so
	// a line holding more than one JSON value (concatenated objects, or plain
	// trailing garbage) would otherwise decode its first object and silently
	// drop the rest. WhatIsNext reports io.EOF once the line is genuinely
	// exhausted; anything else means there was more to read.
	if iter.Error == nil {
		iter.WhatIsNext()
		if iter.Error == nil {
			return labels.EmptyLabels(), nil, errors.New("malformed export line: trailing data after the closing brace")
		}
	}
	if len(values) != len(timestamps) {
		return labels.EmptyLabels(), nil, fmt.Errorf("malformed export line: %d values but %d timestamps", len(values), len(timestamps))
	}

	// Export timestamps are milliseconds, which is what storage.Series wants.
	samples := make([]chunks.Sample, len(values))
	for i, v := range values {
		samples[i] = promapi.FloatSample(timestamps[i], v)
	}
	builder.Sort()
	return builder.Labels(), samples, nil
}

// readExportValue reads one entry of the `values` array. Most are plain JSON
// numbers, but VictoriaMetrics encodes the values JSON can't hold specially
// (see convertValueToSpecialJSON in its export template): NaN as null, the
// infinities as "Infinity" / "-Infinity". A NaN here is an ordinary NaN, not a
// staleness marker -- export carries no staleness information.
func readExportValue(iter *jsoniter.Iterator) float64 {
	switch iter.WhatIsNext() {
	case jsoniter.NilValue:
		iter.ReadNil()
		return math.NaN()
	case jsoniter.StringValue:
		s := iter.ReadString()
		f, err := strconv.ParseFloat(s, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			// A magnitude ParseFloat can't represent still resolves to the
			// correct ±Inf; only a syntactically bad string is fatal.
			iter.ReportError("readExportValue", err.Error())
		}
		return f
	default:
		return iter.ReadFloat64()
	}
}

// sortExportSamples orders a series' samples by timestamp and collapses
// duplicate timestamps to the last one exported. VictoriaMetrics returns a
// series in time order, but max_rows_per_line splits one across lines whose
// relative order is unspecified, and the storage iterators require strictly
// increasing timestamps, so neither property is assumed here. The sort is
// stable so "last exported" is well defined.
func sortExportSamples(samples []chunks.Sample) []chunks.Sample {
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].T() < samples[j].T() })
	out := samples[:0]
	for i, s := range samples {
		if i+1 < len(samples) && samples[i+1].T() == s.T() {
			continue
		}
		out = append(out, s)
	}
	return out
}
