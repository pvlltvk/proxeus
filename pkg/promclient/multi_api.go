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
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/jacksontj/promxy/pkg/promapi"
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

// ordinalSeriesSet tags one backend's SeriesSet with the source index (its
// position in the apis slice / YAML server_group order).
type ordinalSeriesSet struct {
	ordinal int
	ss      storage.SeriesSet
}

// seriesSetMergeFunc folds the fanned-out, per-backend SeriesSets (tagged with
// their source ordinal) into a single result. The default implementation
// (NewMultiAPI) strips ordinals and does within-group HA anti-affinity merging
// via MergeSeriesSets; the cross-group implementation does deterministic
// n-way dedup keyed on ordinal. ctx is the fan-out's request context so the
// merge can honor per-query markers (e.g. IsAggregatePushdown).
type seriesSetMergeFunc func(ctx context.Context, sets []ordinalSeriesSet) storage.SeriesSet

// mergeSeriesFunc folds the successful per-backend Series() results into one
// labelset slice, inputs sorted by ascending ordinal. The default set-unions
// (MergeLabelSets); cross-group callers override it for reduced-fingerprint
// dedup.
type mergeSeriesFunc func(results []gathered[[]model.LabelSet]) []model.LabelSet

// NewMustMultiAPI returns a MultiAPI
func NewMustMultiAPI(apis []API, antiAffinity model.Time, antiAffinityDynamic bool, metricFunc MultiAPIMetricFunc, requiredCount int, preferMax bool) *MultiAPI {
	a, err := NewMultiAPI(apis, antiAffinity, antiAffinityDynamic, metricFunc, requiredCount, preferMax)
	if err != nil {
		panic(err)
	}
	return a
}

// NewMultiAPI returns a MultiAPI. When antiAffinityDynamic is true,
// MergeSampleStream infers the per-series anti-affinity from the inter-
// sample spacing of the data, using antiAffinity only as a fallback when
// too few samples are present to estimate. This lets a single server-
// group host series with mixed scrape intervals without forcing one
// global buffer that's wrong for half the metrics. See #734.
func NewMultiAPI(apis []API, antiAffinity model.Time, antiAffinityDynamic bool, metricFunc MultiAPIMetricFunc, requiredCount int, preferMax bool) (*MultiAPI, error) {
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
		apis:                apis,
		apiFingerprints:     apiFingerprints,
		antiAffinity:        antiAffinity,
		antiAffinityDynamic: antiAffinityDynamic,
		metricFunc:          metricFunc,
		requiredCount:       requiredCount,
		preferMax:           preferMax,
	}
	// Default merge: within-group HA semantics; ordinals are ignored.
	// MergeSeriesSets applies anti-affinity dedup across the fanned-out sets.
	m.seriesSetMergeFn = func(_ context.Context, sets []ordinalSeriesSet) storage.SeriesSet {
		stripped := make([]storage.SeriesSet, len(sets))
		for i, s := range sets {
			stripped[i] = s.ss
		}
		return MergeSeriesSets(m.antiAffinity, m.antiAffinityDynamic, m.preferMax, stripped...)
	}
	// Default series merge: set union; ordinals are ignored.
	m.mergeSeriesFn = func(results []gathered[[]model.LabelSet]) []model.LabelSet {
		var merged []model.LabelSet
		for _, r := range results {
			if merged == nil {
				merged = r.value
				continue
			}
			merged = MergeLabelSets(merged, r.value)
		}
		return merged
	}
	return m, nil
}

// MultiAPI implements the API interface while merging the results from the apis it wraps
type MultiAPI struct {
	apis                []API
	apiFingerprints     []model.Fingerprint
	antiAffinity        model.Time
	antiAffinityDynamic bool
	metricFunc          MultiAPIMetricFunc
	requiredCount       int // number "per key" that we require to respond
	preferMax           bool
	seriesSetMergeFn    seriesSetMergeFunc
	mergeSeriesFn       mergeSeriesFunc

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

// partialResponseErr formats the degradation warning attached when a backend
// fails in partial-response mode.
func partialResponseErr(i int, err error) error {
	return fmt.Errorf("partial_response: backend[%d] unavailable: %s", i, err.Error())
}

// partialWarning formats the degradation warning attached when a backend fails
// in partial-response mode (v1.Warnings variant, used by scatterGather).
func partialWarning(i int, err error) v1.Warnings {
	return v1.Warnings{partialResponseErr(i, err).Error()}
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

	for range m.apis {
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

// scatterMerge fans `call` out across all members, applies the
// success/requiredCount HA logic (relaxed when m.partialResponse is set), and
// merges the successful SeriesSets via m.seriesSetMergeFn. Each result is
// tagged with its source ordinal so the merge hook can fold deterministically
// (e.g. lowest-ordinal-wins cross-group dedup) regardless of arrival order.
// Warnings are unioned across all responses.
func (m *MultiAPI) scatterMerge(ctx context.Context, op string, call func(context.Context, API) storage.SeriesSet) storage.SeriesSet {
	childContext, childContextCancel := context.WithCancel(ctx)
	defer childContextCancel()

	type chanResult struct {
		ss      storage.SeriesSet
		ls      model.Fingerprint
		ordinal int
	}
	resultChan := make(chan chanResult, len(m.apis))
	outstandingRequests := make(map[model.Fingerprint]int)

	for i, api := range m.apis {
		outstandingRequests[m.apiFingerprints[i]]++
		go func(i int, api API) {
			start := time.Now()
			ss := call(childContext, api)
			took := time.Since(start)
			status := "success"
			if ss.Err() != nil {
				status = "error"
			}
			m.recordMetric(i, op, status, took.Seconds())
			resultChan <- chanResult{ss: ss, ls: m.apiFingerprints[i], ordinal: i}
		}(i, api)
	}

	var sets []ordinalSeriesSet
	var warnings annotations.Annotations
	var lastError error
	successMap := make(map[model.Fingerprint]int)
	for range m.apis {
		select {
		case <-ctx.Done():
			// Caller deadline/cancel fired. In partial-response mode, keep what
			// already succeeded rather than discarding it; otherwise honor it.
			if m.partialResponse && len(sets) > 0 {
				warnings.Add(fmt.Errorf("partial_response: %s returning %d/%d backends after cancellation: %w",
					op, len(sets), len(m.apis), ctx.Err()))
				sortOrdinalSeriesSets(sets)
				return WithWarnings(m.seriesSetMergeFn(ctx, sets), warnings)
			}
			return promapi.NewSeriesSet(nil, warnings, ctx.Err())

		case ret := <-resultChan:
			outstandingRequests[ret.ls]--
			warnings = MergeAnnotations(warnings, ret.ss.Warnings())
			if err := NormalizePromError(ret.ss.Err()); err != nil {
				if m.abortOnError(ret.ls, outstandingRequests, successMap) {
					return promapi.NewSeriesSet(nil, warnings, err)
				}
				if m.partialResponse {
					warnings.Add(partialResponseErr(ret.ordinal, err))
				}
				lastError = err
			} else {
				successMap[ret.ls]++
				sets = append(sets, ordinalSeriesSet{ordinal: ret.ordinal, ss: ret.ss})
			}
		}
	}

	if m.missingRequired(outstandingRequests, successMap) {
		return promapi.NewSeriesSet(nil, warnings, errors.Wrap(lastError, "Unable to fetch from downstream servers"))
	}

	sortOrdinalSeriesSets(sets)
	return WithWarnings(m.seriesSetMergeFn(ctx, sets), warnings)
}

// sortOrdinalSeriesSets orders results by ascending source ordinal, so merge
// hooks fold deterministically (lowest server_group first) independent of
// arrival order.
func sortOrdinalSeriesSets(sets []ordinalSeriesSet) {
	sort.Slice(sets, func(i, j int) bool { return sets[i].ordinal < sets[j].ordinal })
}

// Query performs a query for the given time.
func (m *MultiAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return m.scatterMerge(ctx, "query", func(c context.Context, api API) storage.SeriesSet {
		return api.Query(c, query, ts)
	})
}

// QueryRange performs a query for the given range.
func (m *MultiAPI) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	return m.scatterMerge(ctx, "query_range", func(c context.Context, api API) storage.SeriesSet {
		return api.QueryRange(c, query, r)
	})
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

	return m.mergeSeriesFn(results), warnings.Warnings(), nil
}

// GetValue fetches the raw collected data, merged across HA members.
func (m *MultiAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	return m.scatterMerge(ctx, "get_value", func(c context.Context, api API) storage.SeriesSet {
		return api.GetValue(c, start, end, matchers)
	})
}

// Metadata returns metadata about metrics currently scraped by the metric name.
func (m *MultiAPI) Metadata(ctx context.Context, metric, limit string) (map[string][]v1.Metadata, error) {
	// Metadata has no warnings channel; adapt it to scatterGather's shape.
	// Per-backend warnings are intentionally dropped because the upstream API.Metadata signature returns only (map, error).
	results, _, err := scatterGather(ctx, m, "metadata",
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

// QueryExemplars performs a query for exemplars by the given query and time range.
// We fan the query out to every server-group and concatenate the per-series
// exemplar lists, deduplicating by series labels (sum-style merge — the union of
// exemplars from all server-groups for the same series).
func (m *MultiAPI) QueryExemplars(ctx context.Context, query string, startTime, endTime time.Time) ([]v1.ExemplarQueryResult, error) {
	childContext, childContextCancel := context.WithCancel(ctx)
	defer childContextCancel()

	type chanResult struct {
		v   []v1.ExemplarQueryResult
		err error
		ls  model.Fingerprint
	}

	resultChans := make([]chan chanResult, len(m.apis))
	outstandingRequests := make(map[model.Fingerprint]int)

	for i, api := range m.apis {
		resultChans[i] = make(chan chanResult, 1)
		outstandingRequests[m.apiFingerprints[i]]++
		go func(i int, retChan chan chanResult, api API) {
			start := time.Now()
			result, err := api.QueryExemplars(childContext, query, startTime, endTime)
			took := time.Since(start)
			if err != nil {
				m.recordMetric(i, "query_exemplars", "error", took.Seconds())
			} else {
				m.recordMetric(i, "query_exemplars", "success", took.Seconds())
			}
			retChan <- chanResult{
				v:   result,
				err: NormalizePromError(err),
				ls:  m.apiFingerprints[i],
			}
		}(i, resultChans[i], api)
	}

	// Wait for results, merging by series labels.
	merged := map[model.Fingerprint]*v1.ExemplarQueryResult{}
	var lastError error
	successMap := make(map[model.Fingerprint]int)
	for i := 0; i < len(m.apis); i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case ret := <-resultChans[i]:
			outstandingRequests[ret.ls]--
			if ret.err != nil {
				if (outstandingRequests[ret.ls] + successMap[ret.ls]) < m.requiredCount {
					return nil, ret.err
				}
				lastError = ret.err
				continue
			}
			successMap[ret.ls]++
			for j := range ret.v {
				qr := ret.v[j]
				fp := qr.SeriesLabels.Fingerprint()
				existing, ok := merged[fp]
				if !ok {
					cp := qr
					merged[fp] = &cp
					continue
				}
				existing.Exemplars = append(existing.Exemplars, qr.Exemplars...)
			}
		}
	}

	for k := range outstandingRequests {
		if successMap[k] < m.requiredCount {
			return nil, errors.Wrap(lastError, "Unable to fetch from downstream servers")
		}
	}

	out := make([]v1.ExemplarQueryResult, 0, len(merged))
	for _, qr := range merged {
		out = append(out, *qr)
	}
	return out, nil
}
