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

	// requiredCount=1: dedup mode means "give me the series from whichever
	// backend has it"; a single healthy backend is enough to serve the query.
	// antiAffinity/preferMax disabled — those are within-group HA concerns.
	m, err := NewMultiAPI(apis, model.TimeFromUnix(0), nil, 1, false)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(groupNames))
	copy(names, groupNames)

	m.mergeFn = func(a, b model.Value, idxA, idxB int) (model.Value, error) {
		opts := promhttputil.DedupOpts{
			IgnoreLabels: ignoreLabels,
			OrdinalA:     idxA,
			OrdinalB:     idxB,
			NameA:        names[idxA],
			NameB:        names[idxB],
		}
		merged, stats, err := promhttputil.MergeValuesDeterministic(a, b, opts)
		if err != nil {
			return nil, err
		}
		if dedupCounter != nil && stats.Collisions > 0 {
			winnerName, loserName := names[idxA], names[idxB]
			if idxB < idxA {
				winnerName, loserName = names[idxB], names[idxA]
			}
			dedupCounter.WithLabelValues(winnerName, loserName).Add(float64(stats.Collisions))
		}
		return merged, nil
	}

	if dedupMetadata {
		m.mergeSeriesFn = func(a, b []model.LabelSet, idxA, idxB int) []model.LabelSet {
			opts := DedupLabelSetsOpts{
				IgnoreLabels: ignoreLabels,
				OrdinalA:     idxA,
				OrdinalB:     idxB,
			}
			merged, stats := MergeLabelSetsDeterministic(a, b, opts)
			if dedupMetadataCounter != nil && stats.Collisions > 0 {
				winnerName, loserName := names[idxA], names[idxB]
				if idxB < idxA {
					winnerName, loserName = names[idxB], names[idxA]
				}
				dedupMetadataCounter.WithLabelValues(winnerName, loserName, "series").Add(float64(stats.Collisions))
			}
			return merged
		}
	}

	return m, nil
}
