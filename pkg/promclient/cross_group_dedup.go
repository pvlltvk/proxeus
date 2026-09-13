package promclient

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// dedupSeriesSets merges N per-backend SeriesSets in a single pass, resolving
// cross-backend collisions (series that match modulo ignore) by lowest ordinal.
// The winner is emitted as-is: its full labelset (including its own external
// labels) and its samples are passed through untouched, and a loser's sample
// iterator is never consumed.
//
// This is the storage.SeriesSet-native form of
// promhttputil.MergeValuesDeterministic's matrix branch, which the query path
// used to reach via a model.Matrix round trip. Callers should pass the sets in
// ascending-ordinal order for a deterministic result ordering, but correctness
// (which series wins, and collision attribution) does not depend on the order.
//
// ignore must be sorted ascending; it is the union of the per-group external
// label names.
func dedupSeriesSets(sets []ordinalSeriesSet, ignore []string, stats *promhttputil.DedupStats) storage.SeriesSet {
	// A single backend is passed through whole. With nothing to collide
	// against, two series of one group that share a reduced fingerprint are
	// not duplicates of each other -- same as MergeValuesDeterministic's
	// single-input case.
	if len(sets) == 1 {
		return sets[0].ss
	}

	type entry struct {
		lbls    labels.Labels
		ordinal int
		idx     int
		// losers holds the ordinals that lost this bucket. Attribution is
		// deferred to the end because the winner is only known once every set
		// has been seen: recording against the winner of the moment would
		// attribute a collision to a backend that a later, lower ordinal goes
		// on to beat. nil unless the bucket actually collided.
		losers []int
	}

	// Buckets are keyed on the exact reduced labelset (the encoded labels minus
	// the ignored names), so unrelated series can never share a bucket -- the
	// model.Value core trusted a 64-bit fingerprint for the same job.
	buckets := make(map[string]*entry)
	var result []storage.Series
	var buf []byte

	for _, set := range sets {
		for set.ss.Next() {
			series := set.ss.At()
			lbls := series.Labels()
			buf = lbls.BytesWithoutLabels(buf, ignore...)

			existing, ok := buckets[string(buf)]
			if !ok {
				result = append(result, series)
				buckets[string(buf)] = &entry{lbls: lbls, ordinal: set.ordinal, idx: len(result) - 1}
				continue
			}

			// Exact duplicate of this bucket's winner: keep first.
			if labels.Equal(existing.lbls, lbls) {
				continue
			}

			// Genuine cross-group collision: lower ordinal wins.
			if set.ordinal < existing.ordinal {
				existing.losers = append(existing.losers, existing.ordinal)
				result[existing.idx] = series
				existing.lbls = lbls
				existing.ordinal = set.ordinal
			} else {
				existing.losers = append(existing.losers, set.ordinal)
			}
		}
		if err := set.ss.Err(); err != nil {
			return promapi.NewSeriesSet(nil, nil, err)
		}
	}

	// Attribute every collision to the bucket's final winner. Map iteration
	// order does not matter: Record only increments counters.
	for _, e := range buckets {
		for _, loser := range e.losers {
			stats.Record(e.ordinal, loser)
		}
	}

	// Series are emitted in order of first appearance (lowest ordinal first),
	// not sorted by labels: same as the model.Matrix merge this replaces, and
	// ProxyQuerier.Select — the only consumer — ignores sortSeries anyway.
	// Warnings are attached by the caller (scatterMerge), which sees every
	// backend's set, including the ones that failed.
	return promapi.NewSeriesSet(result, nil, nil)
}
