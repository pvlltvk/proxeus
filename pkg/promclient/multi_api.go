package promclient

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"

	"github.com/jacksontj/promxy/pkg/promhttputil"
)

// Since these error types magically add in their own prefixes, we need to get
// the prefix so it doesn't get added twice
var (
	timeoutPrefix  = promql.ErrQueryTimeout("").Error()
	canceledPrefix = promql.ErrQueryCanceled("").Error()
)

// NormalizePromError converts the errors that the prometheus API client returns
// into errors that the prometheus API server actually handles and returns proper
// error codes for
func NormalizePromError(err error) error {
	type result struct {
		ErrorType promhttputil.ErrorType `json:"errorType,omitempty"`
		Error     string                 `json:"error,omitempty"`
	}

	if typedErr, ok := err.(*v1.Error); ok {
		res := &result{}
		// The prometheus client does a terrible job of handling and returning errors
		// so we need to do the work ourselves.
		// The `Detail` is actually just the body of the response so we need
		// to unmarshal that so we can see what happened
		if err := json.Unmarshal([]byte(typedErr.Detail), res); err != nil {
			// If the body can't be unmarshaled, return the original error
			return typedErr
		}

		// Now we want to switch for any errors that the API server will handle differently
		switch res.ErrorType {
		case promhttputil.ErrorTimeout:
			return promql.ErrQueryTimeout(strings.TrimPrefix(res.Error, timeoutPrefix))
		case promhttputil.ErrorCanceled:
			return promql.ErrQueryCanceled(strings.TrimPrefix(res.Error, canceledPrefix))
		}
	}

	// If all else fails, return the original error
	return err
}

// MultiAPIMetricFunc defines a method where a client can record metrics about
// the specific API calls made through this multi client
type MultiAPIMetricFunc func(i int, api, status string, took float64)

// OrdinalValue pairs one backend's value result with its source ordinal (its
// index within the apis slice / YAML server_group order).
type OrdinalValue struct {
	Value   model.Value
	Ordinal int
}

// OrdinalSeries pairs one backend's Series() result with its source ordinal.
type OrdinalSeries struct {
	Series  []model.LabelSet
	Ordinal int
}

// MergeFunc folds the successful per-backend values into a single result. The
// inputs are provided sorted by ascending ordinal, so a fold preserves
// YAML/server_group priority regardless of the order the backends actually
// responded in. The default implementation (NewMultiAPI) does within-group HA
// merging and ignores ordinals; the cross-group implementation does
// deterministic n-way dedup keyed on ordinal.
//
// Collection order is intentionally decoupled from merge order: MultiAPI gathers
// responses as they arrive (no head-of-line blocking, partial-response aware on
// cancellation) but always merges in ascending-ordinal order here.
type MergeFunc func(results []OrdinalValue) (model.Value, error)

// MergeSeriesFunc folds the successful per-backend Series() results into one
// labelset slice, inputs sorted by ascending ordinal. The default set-unions
// (MergeLabelSets); cross-group callers override it for reduced-fingerprint
// dedup.
type MergeSeriesFunc func(results []OrdinalSeries) []model.LabelSet

// NewMustMultiAPI returns a MultiAPI
func NewMustMultiAPI(apis []API, antiAffinity model.Time, metricFunc MultiAPIMetricFunc, requiredCount int, preferMax bool) *MultiAPI {
	a, err := NewMultiAPI(apis, antiAffinity, metricFunc, requiredCount, preferMax)
	if err != nil {
		panic(err)
	}
	return a
}

// NewMultiAPI returns a MultiAPI
func NewMultiAPI(apis []API, antiAffinity model.Time, metricFunc MultiAPIMetricFunc, requiredCount int, preferMax bool) (*MultiAPI, error) {
	fingerprintCounts := make(map[model.Fingerprint]int)
	apiFingerprints := make([]model.Fingerprint, len(apis))
	for i, api := range apis {
		var fingerprint model.Fingerprint
		if apiLabels, ok := api.(APILabels); ok {
			if keys := apiLabels.Key(); keys != nil {
				fingerprint = keys.FastFingerprint()
			}
		}
		apiFingerprints[i] = fingerprint
		fingerprintCounts[fingerprint]++
	}

	for _, v := range fingerprintCounts {
		if v < requiredCount {
			return nil, fmt.Errorf("unable to create multiAPI as the APIs aren't unique")
		}
	}

	m := &MultiAPI{
		apis:            apis,
		apiFingerprints: apiFingerprints,
		antiAffinity:    antiAffinity,
		metricFunc:      metricFunc,
		requiredCount:   requiredCount,
		preferMax:       preferMax,
	}
	// Default merge: within-group HA semantics; ordinals are ignored. Inputs are
	// sorted by ascending ordinal, so this left-fold matches the historical
	// index-order merge exactly.
	m.mergeFn = func(results []OrdinalValue) (model.Value, error) {
		var merged model.Value
		for _, r := range results {
			if merged == nil {
				merged = r.Value
				continue
			}
			v, err := promhttputil.MergeValues(m.antiAffinity, merged, r.Value, m.preferMax)
			if err != nil {
				return nil, err
			}
			merged = v
		}
		return merged, nil
	}
	// Default series merge: set union; ordinals are ignored.
	m.mergeSeriesFn = func(results []OrdinalSeries) []model.LabelSet {
		var merged []model.LabelSet
		for _, r := range results {
			if merged == nil {
				merged = r.Series
				continue
			}
			merged = MergeLabelSets(merged, r.Series)
		}
		return merged
	}
	return m, nil
}

// MultiAPI implements the API interface while merging the results from the apis it wraps
type MultiAPI struct {
	apis            []API
	apiFingerprints []model.Fingerprint
	antiAffinity    model.Time
	metricFunc      MultiAPIMetricFunc
	requiredCount   int // number "per key" that we require to respond
	preferMax       bool
	mergeFn         MergeFunc
	mergeSeriesFn   MergeSeriesFunc

	// partialResponse, when true, relaxes requiredCount: the fan-out returns
	// whatever responded (plus a warning per failed backend) instead of failing
	// the whole call when a backend errors. Only set by the cross-group
	// constructor; the within-group HA path leaves it false.
	partialResponse bool
}

func (m *MultiAPI) recordMetric(i int, api, status string, took float64) {
	if m.metricFunc != nil {
		m.metricFunc(i, api, status, took)
	}
}

// abortOnError reports whether an error from one backend should abort the whole
// fan-out immediately. In partial-response mode a single failure is tolerated,
// so we never abort early; otherwise we abort as soon as a fingerprint bucket
// can no longer reach requiredCount.
func (m *MultiAPI) abortOnError(ls model.Fingerprint, outstanding, success map[model.Fingerprint]int) bool {
	if m.partialResponse {
		return false
	}
	return (outstanding[ls] + success[ls]) < m.requiredCount
}

// missingRequired reports the final-check failure. In partial-response mode the
// fan-out succeeds as long as at least one backend responded (callers surface a
// warning for the rest); otherwise every bucket must reach requiredCount.
func (m *MultiAPI) missingRequired(outstanding, success map[model.Fingerprint]int) bool {
	if m.partialResponse {
		return len(success) == 0
	}
	for k := range outstanding {
		if success[k] < m.requiredCount {
			return true
		}
	}
	return false
}

// partialWarning formats the degradation warning attached when a backend fails
// in partial-response mode.
func partialWarning(i int, err error) v1.Warnings {
	return v1.Warnings{fmt.Sprintf("partial_response: backend[%d] unavailable: %s", i, err.Error())}
}

// gathered is one backend's successful response plus the ordinal it came from.
type gathered[T any] struct {
	value   T
	ordinal int
}

// scatterGather fans `call` out across all of m.apis concurrently and collects
// the responses in arrival order — there is no head-of-line blocking on a slow
// low-ordinal backend. It applies the requiredCount / partialResponse policy via
// m.abortOnError / m.missingRequired and accumulates warnings.
//
// It is partial-aware on context cancellation: if the caller's ctx is canceled
// (e.g. the PromQL engine's query deadline) after at least one backend has
// already answered and partialResponse is on, it returns the successes gathered
// so far (with a warning) instead of discarding everything with ctx.Err(). This
// is the fix for the case where a low-ordinal backend hangs past the deadline:
// previously the whole query failed even though healthy backends had responded.
//
// Successful results are returned sorted by ascending ordinal so callers merge
// deterministically (lowest server_group first), independent of arrival order.
func scatterGather[T any](
	ctx context.Context,
	m *MultiAPI,
	apiName string,
	call func(ctx context.Context, api API) (T, v1.Warnings, error),
) ([]gathered[T], promhttputil.WarningSet, error) {
	childContext, childContextCancel := context.WithCancel(ctx)
	defer childContextCancel()

	type rawResult struct {
		value    T
		warnings v1.Warnings
		err      error
		ls       model.Fingerprint
		ordinal  int
	}

	resultChan := make(chan rawResult, len(m.apis))
	outstanding := make(map[model.Fingerprint]int) // fingerprint -> outstanding
	for i := range m.apis {
		outstanding[m.apiFingerprints[i]]++
	}

	for i, api := range m.apis {
		go func(i int, api API) {
			start := time.Now()
			value, w, err := call(childContext, api)
			took := time.Since(start)
			if err != nil {
				m.recordMetric(i, apiName, "error", took.Seconds())
			} else {
				m.recordMetric(i, apiName, "success", took.Seconds())
			}
			resultChan <- rawResult{
				value:    value,
				warnings: w,
				err:      NormalizePromError(err),
				ls:       m.apiFingerprints[i],
				ordinal:  i,
			}
		}(i, api)
	}

	warnings := make(promhttputil.WarningSet)
	successMap := make(map[model.Fingerprint]int) // fingerprint -> success
	results := make([]gathered[T], 0, len(m.apis))
	var lastError error

	for n := 0; n < len(m.apis); n++ {
		select {
		case <-ctx.Done():
			// Caller deadline/cancel fired. In partial-response mode, keep what
			// already succeeded rather than discarding it; otherwise honor it.
			if m.partialResponse && len(results) > 0 {
				warnings.AddWarnings(v1.Warnings{fmt.Sprintf(
					"partial_response: %s returning %d/%d backends after cancellation: %s",
					apiName, len(results), len(m.apis), ctx.Err())})
				sortGathered(results)
				return results, warnings, nil
			}
			return nil, warnings, ctx.Err()

		case ret := <-resultChan:
			warnings.AddWarnings(ret.warnings)
			outstanding[ret.ls]--
			if ret.err != nil {
				if m.abortOnError(ret.ls, outstanding, successMap) {
					return nil, warnings, ret.err
				}
				if m.partialResponse {
					warnings.AddWarnings(partialWarning(ret.ordinal, ret.err))
				}
				lastError = ret.err
				continue
			}
			successMap[ret.ls]++
			results = append(results, gathered[T]{value: ret.value, ordinal: ret.ordinal})
		}
	}

	if m.missingRequired(outstanding, successMap) {
		return nil, warnings, errors.Wrap(lastError, "Unable to fetch from downstream servers")
	}

	sortGathered(results)
	return results, warnings, nil
}

// sortGathered orders gathered results by ascending source ordinal.
func sortGathered[T any](results []gathered[T]) {
	sort.Slice(results, func(i, j int) bool { return results[i].ordinal < results[j].ordinal })
}

// toOrdinalValues adapts gathered value results into the merge-hook input.
func toOrdinalValues(results []gathered[model.Value]) []OrdinalValue {
	out := make([]OrdinalValue, len(results))
	for i, r := range results {
		out[i] = OrdinalValue{Value: r.value, Ordinal: r.ordinal}
	}
	return out
}

// toOrdinalSeries adapts gathered series results into the merge-hook input.
func toOrdinalSeries(results []gathered[[]model.LabelSet]) []OrdinalSeries {
	out := make([]OrdinalSeries, len(results))
	for i, r := range results {
		out[i] = OrdinalSeries{Series: r.value, Ordinal: r.ordinal}
	}
	return out
}

// LabelValues performs a query for the values of the given label.
func (m *MultiAPI) LabelValues(ctx context.Context, label string, matchers []string, startTime time.Time, endTime time.Time) (model.LabelValues, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "label_values",
		func(ctx context.Context, api API) (model.LabelValues, v1.Warnings, error) {
			return api.LabelValues(ctx, label, matchers, startTime, endTime)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	// Label values are a set union; merge order does not matter.
	seen := make(map[model.LabelValue]struct{})
	for _, r := range results {
		for _, v := range r.value {
			seen[v] = struct{}{}
		}
	}

	result := make(model.LabelValues, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	sort.Sort(result)

	return result, warnings.Warnings(), nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (m *MultiAPI) LabelNames(ctx context.Context, matchers []string, startTime time.Time, endTime time.Time) ([]string, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "label_names",
		func(ctx context.Context, api API) ([]string, v1.Warnings, error) {
			return api.LabelNames(ctx, matchers, startTime, endTime)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	// Label names are a set union; merge order does not matter.
	result := make(map[string]struct{})
	for _, r := range results {
		for _, v := range r.value {
			result[v] = struct{}{}
		}
	}

	stringResult := make([]string, 0, len(result))
	for k := range result {
		stringResult = append(stringResult, k)
	}

	sort.Strings(stringResult)

	return stringResult, warnings.Warnings(), nil
}

// Query performs a query for the given time.
func (m *MultiAPI) Query(ctx context.Context, query string, ts time.Time) (model.Value, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "query",
		func(ctx context.Context, api API) (model.Value, v1.Warnings, error) {
			return api.Query(ctx, query, ts)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	merged, err := m.mergeFn(toOrdinalValues(results))
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	return merged, warnings.Warnings(), nil
}

// QueryRange performs a query for the given range.
func (m *MultiAPI) QueryRange(ctx context.Context, query string, r v1.Range) (model.Value, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "query_range",
		func(ctx context.Context, api API) (model.Value, v1.Warnings, error) {
			return api.QueryRange(ctx, query, r)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	merged, err := m.mergeFn(toOrdinalValues(results))
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	return merged, warnings.Warnings(), nil
}

// Series finds series by label matchers.
func (m *MultiAPI) Series(ctx context.Context, matches []string, startTime time.Time, endTime time.Time) ([]model.LabelSet, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "series",
		func(ctx context.Context, api API) ([]model.LabelSet, v1.Warnings, error) {
			return api.Series(ctx, matches, startTime, endTime)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	return m.mergeSeriesFn(toOrdinalSeries(results)), warnings.Warnings(), nil
}

// GetValue fetches a `model.Value` which represents the actual collected data
func (m *MultiAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) (model.Value, v1.Warnings, error) {
	results, warnings, err := scatterGather(ctx, m, "get_value",
		func(ctx context.Context, api API) (model.Value, v1.Warnings, error) {
			return api.GetValue(ctx, start, end, matchers)
		})
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	merged, err := m.mergeFn(toOrdinalValues(results))
	if err != nil {
		return nil, warnings.Warnings(), err
	}

	return merged, warnings.Warnings(), nil
}

// Metadata returns metadata about metrics currently scraped by the metric name.
func (m *MultiAPI) Metadata(ctx context.Context, metric, limit string) (map[string][]v1.Metadata, error) {
	// Metadata has no warnings channel; adapt it to scatterGather's shape.
	results, _, err := scatterGather(ctx, m, "query",
		func(ctx context.Context, api API) (map[string][]v1.Metadata, v1.Warnings, error) {
			v, err := api.Metadata(ctx, metric, limit)
			return v, nil, err
		})
	if err != nil {
		return nil, err
	}

	// First writer wins per metric name. Results are sorted by ascending
	// ordinal, so the lowest server_group deterministically wins.
	var result map[string][]v1.Metadata
	for _, r := range results {
		if result == nil {
			result = r.value
			continue
		}
		for k, v := range r.value {
			if _, ok := result[k]; !ok {
				result[k] = v
			}
		}
	}

	return result, nil
}
