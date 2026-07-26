package promclient

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"

	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// drainToMatrix materializes ss into a model.Matrix, one *model.SampleStream
// per series (labels + all samples). This is the boundary between the
// SeriesSet world and promhttputil's tested model.Value dedup core
// (MergeValuesDeterministic), which cross-group dedup reuses unchanged.
func drainToMatrix(ss storage.SeriesSet) (model.Matrix, error) {
	return SeriesSetToMatrix(ss)
}

// CrossGroupBackend is one server_group's API client plus the identity
// (name and external labels) used for cross-group dedup and collision
// attribution.
type CrossGroupBackend struct {
	API    API
	Name   string
	Labels model.LabelSet
}

// CrossGroupOpts configures the cross-group dedup behavior of
// NewCrossGroupMultiAPI. Collisions and MetadataCollisions may be nil to skip
// the corresponding metric.
type CrossGroupOpts struct {
	// DedupMetadata, when true, additionally wires reduced-fingerprint dedup
	// into MultiAPI.Series so /api/v1/series collapses series that differ only
	// by per-group external labels.
	DedupMetadata bool

	// PartialResponse, when true, relaxes the fan-out so a query succeeds as
	// long as at least one backend responded.
	PartialResponse bool

	// Collisions, if non-nil, is incremented on query-path (Query / QueryRange)
	// collisions with label values {winner, loser} identifying the group names.
	Collisions *prometheus.CounterVec

	// MetadataCollisions mirrors Collisions for the /api/v1/series dedup path
	// (only used when DedupMetadata is true).
	MetadataCollisions *prometheus.CounterVec
}

// NewCrossGroupMultiAPI builds a MultiAPI that performs deterministic
// cross-group dedup: collisions modulo the union of per-group `labels`
// keys are resolved by lowest ordinal (YAML order, i.e. backends[i]'s
// position in the slice).
func NewCrossGroupMultiAPI(backends []CrossGroupBackend, opts CrossGroupOpts) (*MultiAPI, error) {
	apis := make([]API, len(backends))
	names := make([]string, len(backends))

	// ignoreLabels is the union of all per-group label keys. Series that
	// differ only in these keys are considered the same underlying series.
	ignoreLabels := make(map[model.LabelName]struct{})
	for i, b := range backends {
		apis[i] = b.API
		names[i] = b.Name
		for k := range b.Labels {
			ignoreLabels[k] = struct{}{}
		}
	}

	// requiredCount=1: each server_group has unique labels, so it occupies its
	// own fingerprint bucket of size 1. With PartialResponse=false this means
	// EVERY backend must respond (any one error fails the whole query — see
	// MultiAPI.missingRequired); PartialResponse=true relaxes that to "at least
	// one backend responded", returning partial results with a warning.
	// antiAffinity/preferMax disabled — those are within-group HA concerns.
	m, err := NewMultiAPI(apis, model.TimeFromUnix(0), false, nil, 1, false)
	if err != nil {
		return nil, err
	}
	m.partialResponse = opts.PartialResponse

	// Keep the default (non-dedup) merge around: pushed-down aggregation
	// queries fan out per-group aggregate PARTIALS that the engine re-combines
	// upstream. Partials are never duplicates of each other, but the
	// aggregation may collapse exactly the labels the reduced fingerprint
	// needs (bare `sum(up)` leaves only the external labels, which dedup
	// ignores) — deduping them silently drops whole backends. NOTE: this means
	// aggregates over series that exist in MULTIPLE groups are combined, not
	// deduped (same as proxeus without cross_group_dedup); only raw selector
	// results get cross-group dedup.
	defaultMerge := m.seriesSetMergeFn

	// N-way merge in a single pass: every series is bucketed under its true
	// origin ordinal, so collisions are attributed to the exact winner/loser
	// server_group (the chained binary form misattributed them to the running
	// minimum ordinal). scatterMerge feeds this hook the successful SeriesSets
	// sorted by ascending ordinal.
	//
	// Cross-group dedup buckets on the reduced fingerprint (full labels minus
	// the per-group external labels), which a label-sorted streaming merge
	// can't bucket without a re-sort, and the lowest-ordinal winner for any
	// bucket may arrive in any input -- so this is a pipeline barrier
	// regardless. We materialize each group's SeriesSet into a model.Matrix
	// and reuse the tested promhttputil.MergeValuesDeterministic core
	// (always via its ValMatrix branch: instant queries become single-sample
	// streams, the same shape PromAPIV1.Query already produces via
	// ModelValueToSeriesSet).
	m.seriesSetMergeFn = func(ctx context.Context, sets []ordinalSeriesSet) storage.SeriesSet {
		if IsAggregatePushdown(ctx) {
			return defaultMerge(ctx, sets)
		}
		inputs := make([]promhttputil.OrdinalValue, len(sets))
		for i, s := range sets {
			matrix, err := drainToMatrix(s.ss)
			if err != nil {
				return ModelValueToSeriesSet(nil, nil, err)
			}
			inputs[i] = promhttputil.OrdinalValue{Ordinal: s.ordinal, Value: matrix}
		}
		merged, stats, err := promhttputil.MergeValuesDeterministic(inputs, ignoreLabels)
		if err != nil {
			return ModelValueToSeriesSet(nil, nil, err)
		}
		if opts.Collisions != nil {
			for pair, count := range stats.Pairs {
				opts.Collisions.WithLabelValues(names[pair[0]], names[pair[1]]).Add(float64(count))
			}
		}
		return ModelValueToSeriesSet(merged, nil, nil)
	}

	if opts.DedupMetadata {
		m.mergeSeriesFn = func(results []gathered[[]model.LabelSet]) []model.LabelSet {
			inputs := make([]OrdinalLabelSets, len(results))
			for i, r := range results {
				inputs[i] = OrdinalLabelSets{Ordinal: r.ordinal, Sets: r.value}
			}
			merged, stats := mergeLabelSetsDeterministic(inputs, ignoreLabels)
			if opts.MetadataCollisions != nil {
				for pair, count := range stats.Pairs {
					opts.MetadataCollisions.WithLabelValues(names[pair[0]], names[pair[1]], "series").Add(float64(count))
				}
			}
			return merged
		}
	}

	return m, nil
}
