package mcp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/pvlltvk/proxeus/pkg/proxeusui"
)

// defaultLookback is the window a range query covers when start_time is not given.
const defaultLookback = 5 * time.Minute

// defaultRangeQueryDataPoints is how many points a range query aims for when
// step is not given; the step is derived from the range to hit it.
const defaultRangeQueryDataPoints = 250

// maxEpochSeconds is where a Unix epoch in seconds stops fitting in a
// time.Time's int64 nanoseconds (year 2262). A millisecond epoch is well past
// it and would otherwise wrap into 1677.
const maxEpochSeconds = 9.2e9

// maxErrorDetail caps the API error detail quoted back to the model:
// intermediaries answer with whole HTML pages.
const maxErrorDetail = 512

func (s *Server) query(ctx context.Context, _ *mcp.CallToolRequest, in queryInput) (*mcp.CallToolResult, queryOutput, error) {
	ts, err := parseTime(in.Timestamp, time.Now())
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	val, warnings, err := s.api.Query(ctx, in.Query, ts)
	if err != nil {
		return nil, queryOutput{}, apiError("failed to execute instant query", err)
	}
	return nil, s.toQueryOutput(val, warnings, in.TruncationLimit), nil
}

func (s *Server) rangeQuery(ctx context.Context, _ *mcp.CallToolRequest, in rangeQueryInput) (*mcp.CallToolResult, queryOutput, error) {
	end, err := parseTime(in.EndTime, time.Now())
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("failed to parse end_time: %w", err)
	}
	start, err := parseTime(in.StartTime, end.Add(-defaultLookback))
	if err != nil {
		return nil, queryOutput{}, fmt.Errorf("failed to parse start_time: %w", err)
	}
	step, err := parseStep(in.Step, start, end)
	if err != nil {
		return nil, queryOutput{}, err
	}

	val, warnings, err := s.api.QueryRange(ctx, in.Query, v1.Range{Start: start, End: end, Step: step})
	if err != nil {
		return nil, queryOutput{}, apiError("failed to execute range query", err)
	}
	return nil, s.toQueryOutput(val, warnings, in.TruncationLimit), nil
}

func (s *Server) series(ctx context.Context, _ *mcp.CallToolRequest, in seriesInput) (*mcp.CallToolResult, seriesOutput, error) {
	start, end, err := parseTimeRange(in.openTimeRangeInput)
	if err != nil {
		return nil, seriesOutput{}, err
	}

	lsets, warnings, err := s.api.Series(ctx, in.Matches, start, end)
	if err != nil {
		return nil, seriesOutput{}, apiError("failed to get series", err)
	}

	kept, tr := truncateSlice(lsets, s.seriesLimit(in.TruncationLimit))
	out := seriesOutput{Series: make([]map[string]string, len(kept)), Warnings: warnings, truncation: tr}
	for i, lset := range kept {
		out.Series[i] = labelSetToMap(lset)
	}
	return nil, out, nil
}

func (s *Server) labelNames(ctx context.Context, _ *mcp.CallToolRequest, in labelNamesInput) (*mcp.CallToolResult, labelNamesOutput, error) {
	start, end, err := parseTimeRange(in.openTimeRangeInput)
	if err != nil {
		return nil, labelNamesOutput{}, err
	}

	names, warnings, err := s.api.LabelNames(ctx, in.Matches, start, end)
	if err != nil {
		return nil, labelNamesOutput{}, apiError("failed to get label names", err)
	}

	kept, tr := truncateSlice(names, s.seriesLimit(in.TruncationLimit))
	return nil, labelNamesOutput{Names: kept, Warnings: warnings, truncation: tr}, nil
}

func (s *Server) labelValues(ctx context.Context, _ *mcp.CallToolRequest, in labelValuesInput) (*mcp.CallToolResult, labelValuesOutput, error) {
	if in.Label == "" {
		return nil, labelValuesOutput{}, errors.New("label parameter is required")
	}
	start, end, err := parseTimeRange(in.openTimeRangeInput)
	if err != nil {
		return nil, labelValuesOutput{}, err
	}

	values, warnings, err := s.api.LabelValues(ctx, in.Label, in.Matches, start, end)
	if err != nil {
		return nil, labelValuesOutput{}, apiError("failed to get label values", err)
	}

	kept, tr := truncateSlice(values, s.seriesLimit(in.TruncationLimit))
	out := labelValuesOutput{Values: make([]string, len(kept)), Warnings: warnings, truncation: tr}
	for i, v := range kept {
		out.Values[i] = string(v)
	}
	return nil, out, nil
}

func (s *Server) metricMetadata(ctx context.Context, _ *mcp.CallToolRequest, in metricMetadataInput) (*mcp.CallToolResult, metricMetadataOutput, error) {
	// The limit argument is forwarded to the backends as-is; the server-wide
	// cap is applied to whatever comes back.
	md, err := s.cfg.Metadata(ctx, in.Metric, in.Limit)
	if err != nil {
		return nil, metricMetadataOutput{}, apiError("failed to get metric metadata", err)
	}

	kept, tr := truncateSlice(slices.Sorted(maps.Keys(md)), s.cfg.MaxSeries)
	out := metricMetadataOutput{Metadata: make(map[string][]metadataEntry, len(kept)), truncation: tr}
	for _, name := range kept {
		entries := make([]metadataEntry, len(md[name]))
		for i, m := range md[name] {
			entries[i] = metadataEntry{Type: string(m.Type), Help: m.Help, Unit: m.Unit}
		}
		out.Metadata[name] = entries
	}
	return nil, out, nil
}

func (s *Server) buildInfo(ctx context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, buildInfoOutput, error) {
	bi, err := s.api.Buildinfo(ctx)
	if err != nil {
		return nil, buildInfoOutput{}, apiError("failed to get build info", err)
	}
	return nil, buildInfoOutput{
		Version:   bi.Version,
		Revision:  bi.Revision,
		Branch:    bi.Branch,
		BuildUser: bi.BuildUser,
		BuildDate: bi.BuildDate,
		GoVersion: bi.GoVersion,
	}, nil
}

func (s *Server) listServerGroups(_ context.Context, _ *mcp.CallToolRequest, _ emptyInput) (*mcp.CallToolResult, serverGroupsOutput, error) {
	inv := s.cfg.Inventory()

	out := serverGroupsOutput{
		GeneratedAt: inv.GeneratedAt.UTC().Format(time.RFC3339),
		Groups:      make([]serverGroupInfo, 0, len(inv.Groups)),
	}
	for _, g := range inv.Groups {
		gi := serverGroupInfo{
			Name:        g.Name,
			BackendType: string(proxeusui.BackendUnknown),
			Labels:      g.Labels,
			TimeRange:   g.TimeRange,
			RemoteRead:  g.RemoteRead,
			Targets:     make([]serverGroupTarget, 0, len(g.Targets)),
		}
		for _, t := range g.Targets {
			// The inventory carries the backend type per target because that
			// is how the UI renders it; it is a property of the group.
			gi.BackendType = string(t.BackendType)
			target := serverGroupTarget{
				URL:       t.URL,
				Healthy:   t.Healthy,
				LastError: t.LastError,
				Version:   t.Version,
			}
			if !t.LastProbeAt.IsZero() {
				target.LastProbeAt = t.LastProbeAt.UTC().Format(time.RFC3339)
			}
			gi.Targets = append(gi.Targets, target)
		}
		out.Groups = append(out.Groups, gi)
	}
	return nil, out, nil
}

// toQueryOutput converts an API value into the query tools' output, applying
// the series cap and then the sample cap.
func (s *Server) toQueryOutput(val model.Value, warnings v1.Warnings, argLimit int) queryOutput {
	kept, tr := truncateSlice(flattenValue(val), s.seriesLimit(argLimit))
	kept, samplesBefore := capSamples(kept, s.cfg.MaxSamples)
	if samplesBefore > 0 {
		tr.Truncated = true
		tr.Returned = len(kept)
		tr.Note = appendNote(tr.Note, fmt.Sprintf(
			"only %d of %d samples are shown (server-wide cap), leaving %d series; shorten the range, widen the step or aggregate",
			s.cfg.MaxSamples, samplesBefore, len(kept)))
	}
	return queryOutput{
		ResultType: val.Type().String(),
		Result:     kept,
		Warnings:   warnings,
		truncation: tr,
	}
}

// flattenValue turns any result type into a flat list of series. Scalars and
// strings become a single series with no labels.
func flattenValue(val model.Value) []seriesResult {
	switch v := val.(type) {
	case model.Vector:
		out := make([]seriesResult, len(v))
		for i, sample := range v {
			point := samplePoint{Timestamp: seconds(sample.Timestamp), Value: sample.Value.String()}
			if sample.Histogram != nil {
				point.Value = sample.Histogram.String()
			}
			out[i] = seriesResult{Metric: metricToMap(sample.Metric), Values: []samplePoint{point}}
		}
		return out
	case model.Matrix:
		out := make([]seriesResult, len(v))
		for i, stream := range v {
			points := make([]samplePoint, 0, len(stream.Values)+len(stream.Histograms))
			for _, p := range stream.Values {
				points = append(points, samplePoint{Timestamp: seconds(p.Timestamp), Value: p.Value.String()})
			}
			for _, p := range stream.Histograms {
				points = append(points, samplePoint{Timestamp: seconds(p.Timestamp), Value: p.Histogram.String()})
			}
			if len(stream.Histograms) > 0 {
				slices.SortFunc(points, func(a, b samplePoint) int { return cmp.Compare(a.Timestamp, b.Timestamp) })
			}
			out[i] = seriesResult{Metric: metricToMap(stream.Metric), Values: points}
		}
		return out
	case *model.Scalar:
		return []seriesResult{{Metric: map[string]string{}, Values: []samplePoint{{Timestamp: seconds(v.Timestamp), Value: v.Value.String()}}}}
	case *model.String:
		return []seriesResult{{Metric: map[string]string{}, Values: []samplePoint{{Timestamp: seconds(v.Timestamp), Value: v.Value}}}}
	}
	return nil
}

// truncateSlice cuts vals down to limit entries (0 means uncapped) and reports
// what was left out.
func truncateSlice[T any](vals []T, limit int) ([]T, truncation) {
	if limit > 0 && len(vals) > limit {
		return vals[:limit], truncation{
			Truncated:             true,
			Returned:              limit,
			TotalBeforeTruncation: len(vals),
			Note:                  fmt.Sprintf("only the first %d of %d entries are shown; narrow the matchers or aggregate to see the rest", limit, len(vals)),
		}
	}
	return vals, truncation{Returned: len(vals), TotalBeforeTruncation: len(vals)}
}

// capSamples trims series so that at most limit samples are returned in total
// (0 means uncapped), dropping whole series once the budget runs out and
// cutting the one that straddles it. It returns the number of samples there
// were before the cut, or 0 when nothing was dropped.
func capSamples(series []seriesResult, limit int) ([]seriesResult, int) {
	total := 0
	for i := range series {
		total += len(series[i].Values)
	}
	if limit <= 0 || total <= limit {
		return series, 0
	}

	budget := limit
	for i := range series {
		if n := len(series[i].Values); n <= budget {
			budget -= n
			continue
		}
		if budget == 0 {
			return series[:i], total
		}
		series[i].Values = series[i].Values[:budget]
		return series[:i+1], total
	}
	return series, 0
}

// appendNote joins the truncation notes of the caps that fired.
func appendNote(existing, note string) string {
	if existing == "" {
		return note
	}
	return existing + " " + note
}

// parseTime accepts a Unix epoch (seconds, possibly fractional), an RFC3339
// timestamp, or a duration relative to now ("5m", "1h30m"). An empty string
// returns def.
func parseTime(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	if epoch, err := strconv.ParseFloat(s, 64); err == nil {
		if math.IsNaN(epoch) || math.IsInf(epoch, 0) || math.Abs(epoch) > maxEpochSeconds {
			return time.Time{}, fmt.Errorf("%q is out of range for a timestamp: pass Unix epoch seconds (not milliseconds) or an RFC3339 timestamp", s)
		}
		return time.Unix(0, int64(epoch*float64(time.Second))), nil
	}
	if ts, err := time.Parse(time.RFC3339, s); err == nil {
		return ts, nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Now().Add(-time.Duration(d)), nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as a Unix timestamp, an RFC3339 timestamp or a duration", s)
}

// parseTimeRange parses the optional start/end of the metadata tools. Unset
// bounds stay zero, which the Prometheus API reads as "no bound".
func parseTimeRange(tr openTimeRangeInput) (start, end time.Time, err error) {
	if start, err = parseTime(tr.StartTime, time.Time{}); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to parse start_time: %w", err)
	}
	if end, err = parseTime(tr.EndTime, time.Time{}); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("failed to parse end_time: %w", err)
	}
	return start, end, nil
}

// parseStep parses the range query step, defaulting to whatever spreads
// defaultRangeQueryDataPoints points over the queried range.
func parseStep(step string, start, end time.Time) (time.Duration, error) {
	if step == "" {
		return time.Duration(math.Max(math.Floor(end.Sub(start).Seconds()/defaultRangeQueryDataPoints), 1)) * time.Second, nil
	}
	d, err := model.ParseDuration(step)
	if err != nil {
		return 0, fmt.Errorf("failed to parse step: %w", err)
	}
	if d <= 0 {
		return 0, errors.New("step must be a positive duration (e.g. '30s', '5m', '1h', '1d')")
	}
	return time.Duration(d), nil
}

// apiError reports a failed API call. Errors the API itself returned (bad
// PromQL, too many samples, a backend that is down) carry their message
// through; transport failures are wrapped as-is.
func apiError(what string, err error) error {
	var apiErr *v1.Error
	if errors.As(err, &apiErr) {
		msg := apiErr.Msg
		if apiErr.Detail != "" {
			msg += ": " + clip(apiErr.Detail, maxErrorDetail)
		}
		return fmt.Errorf("%s: %s", what, msg)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// clip shortens s to at most limit bytes, dropping any partial trailing rune.
func clip(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return strings.ToValidUTF8(s[:limit], "") + "... (truncated)"
}

func metricToMap(m model.Metric) map[string]string {
	out := make(map[string]string, len(m))
	for name, value := range m {
		out[string(name)] = string(value)
	}
	return out
}

func labelSetToMap(lset model.LabelSet) map[string]string {
	out := make(map[string]string, len(lset))
	for name, value := range lset {
		out[string(name)] = string(value)
	}
	return out
}

func seconds(t model.Time) float64 { return float64(t) / 1000 }
