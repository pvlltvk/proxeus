package promclient

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/jacksontj/promxy/pkg/promhttputil"
)

// NewCrossGroupMultiAPI builds a MultiAPI that performs deterministic
// cross-group dedup: collisions modulo the union of per-group `labels`
// keys are resolved by lowest ordinal (YAML order).
//
// apis, groupNames, and groupLabels must all have the same length; element i
// is the API/name/labels for the server_group at index i.
//
// dedupCounter may be nil; if non-nil it is incremented on query-path
// (Query / QueryRange) collisions with label values {winner, loser} identifying
// the group names.
//
// dedupMetadata, when true, additionally wires reduced-fingerprint dedup into
// MultiAPI.Series so /api/v1/series collapses series that differ only by
// per-group external labels. dedupMetadataCounter mirrors dedupCounter for
// that path; pass nil to skip the metric.
func NewCrossGroupMultiAPI(
	apis []API,
	groupNames []string,
	groupLabels []model.LabelSet,
	dedupCounter *prometheus.CounterVec,
	dedupMetadata bool,
	dedupMetadataCounter *prometheus.CounterVec,
	partialResponse bool,
) (*MultiAPI, error) {
	if len(apis) != len(groupNames) || len(apis) != len(groupLabels) {
		return nil, fmt.Errorf("apis, groupNames, and groupLabels must have the same length")
	}

	// ignoreLabels is the union of all per-group label keys. Series that
	// differ only in these keys are considered the same underlying series.
	ignoreLabels := make(map[model.LabelName]struct{})
	for _, ls := range groupLabels {
		for k := range ls {
			ignoreLabels[k] = struct{}{}
		}
	}

	// requiredCount=1: each server_group has unique labels, so it occupies its
	// own fingerprint bucket of size 1. With partialResponse=false this means
	// EVERY backend must respond (any one error fails the whole query — see
	// MultiAPI.missingRequired); partialResponse=true relaxes that to "at least
	// one backend responded", returning partial results with a warning.
	// antiAffinity/preferMax disabled — those are within-group HA concerns.
	m, err := NewMultiAPI(apis, model.TimeFromUnix(0), nil, 1, false)
	if err != nil {
		return nil, err
	}
	m.partialResponse = partialResponse

	names := make([]string, len(groupNames))
	copy(names, groupNames)

	// N-way merge in a single pass: every series is bucketed under its true
	// origin ordinal, so collisions are attributed to the exact winner/loser
	// server_group (the chained binary form misattributed them to the running
	// minimum ordinal). MultiAPI feeds these hooks the successful results sorted
	// by ascending ordinal.
	m.mergeFn = func(results []gathered[model.Value]) (model.Value, error) {
		values := make([]model.Value, len(results))
		ordinals := make([]int, len(results))
		for i, r := range results {
			values[i] = r.value
			ordinals[i] = r.ordinal
		}
		merged, stats, err := promhttputil.MergeValuesDeterministic(values, ordinals, ignoreLabels)
		if err != nil {
			return nil, err
		}
		if dedupCounter != nil {
			for pair, count := range stats.Pairs {
				dedupCounter.WithLabelValues(names[pair[0]], names[pair[1]]).Add(float64(count))
			}
		}
		return merged, nil
	}

	if dedupMetadata {
		m.mergeSeriesFn = func(results []gathered[[]model.LabelSet]) []model.LabelSet {
			sets := make([][]model.LabelSet, len(results))
			ordinals := make([]int, len(results))
			for i, r := range results {
				sets[i] = r.value
				ordinals[i] = r.ordinal
			}
			merged, stats := mergeLabelSetsDeterministic(sets, ordinals, ignoreLabels)
			if dedupMetadataCounter != nil {
				for pair, count := range stats.Pairs {
					dedupMetadataCounter.WithLabelValues(names[pair[0]], names[pair[1]], "series").Add(float64(count))
				}
			}
			return merged
		}
	}

	return m, nil
}
