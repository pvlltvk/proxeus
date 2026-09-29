package promclient

import (
	"sort"

	"github.com/prometheus/common/model"
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
// the pre-SeriesSet model.Value merge's matrix branch, which the query path
// used to reach via a model.Matrix round trip. Callers should pass the sets in
// ascending-ordinal order for a deterministic result ordering, but correctness
// (which series wins, and collision attribution) does not depend on the order.
//
// ignore must be sorted ascending; it is the union of the per-group external
// label names.
func dedupSeriesSets(sets []ordinalSeriesSet, ignore []string, stats *promhttputil.DedupStats) storage.SeriesSet {
	return dedupSeriesSetsFillGaps(sets, ignore, dedupGapOpts{}, stats, nil)
}

type dedupGapOpts struct {
	fillGaps bool
	gap      model.Time // 0 = auto, see promhttputil.GapThreshold
}

// dedupSeriesSetsFillGaps is dedupSeriesSets that, with opts.fillGaps, fills
// the winner's gaps from the losers in ascending-ordinal order. Non-colliding
// series still pass through untouched. fillStats may be nil.
func dedupSeriesSetsFillGaps(sets []ordinalSeriesSet, ignore []string, opts dedupGapOpts, stats *promhttputil.DedupStats, fillStats *promhttputil.GapFillStats) storage.SeriesSet {
	// A single backend is passed through whole. With nothing to collide
	// against, two series of one group that share a reduced fingerprint are
	// not duplicates of each other -- same as the old model.Value merge's
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
		// members is only populated with opts.fillGaps; the winner isn't known
		// until every set has been seen.
		members []dedupBucketMember
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
				e := &entry{lbls: lbls, ordinal: set.ordinal, idx: len(result) - 1}
				if opts.fillGaps {
					e.members = append(e.members, dedupBucketMember{ordinal: set.ordinal, series: series})
				}
				buckets[string(buf)] = e
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
			if opts.fillGaps {
				existing.members = append(existing.members, dedupBucketMember{ordinal: set.ordinal, series: series})
			}
		}
		if err := set.ss.Err(); err != nil {
			return promapi.NewSeriesSet(nil, nil, err)
		}
	}

	// Map iteration order does not matter: Record only increments counters, and
	// each bucket's fill only touches its own result[idx] slot.
	for _, e := range buckets {
		for _, loser := range e.losers {
			stats.Record(e.ordinal, loser)
		}
		if opts.fillGaps && len(e.members) > 1 {
			result[e.idx] = fillBucketGaps(e.members, opts.gap, fillStats)
		}
	}

	// Series are emitted in order of first appearance (lowest ordinal first),
	// not sorted by labels: same as the model.Matrix merge this replaces, and
	// ProxyQuerier.Select — the only consumer — ignores sortSeries anyway.
	// Warnings are attached by the caller (scatterMerge), which sees every
	// backend's set, including the ones that failed.
	return promapi.NewSeriesSet(result, nil, nil)
}

type dedupBucketMember struct {
	ordinal int
	series  storage.Series
}

// fillBucketGaps returns the lowest-ordinal member, with its labels, gap-filled
// from the others.
func fillBucketGaps(members []dedupBucketMember, gap model.Time, fillStats *promhttputil.GapFillStats) storage.Series {
	sort.Slice(members, func(i, j int) bool { return members[i].ordinal < members[j].ordinal })

	base := seriesToSampleStream(members[0].series)
	fillers := make([]*model.SampleStream, len(members)-1)
	for i, m := range members[1:] {
		fillers[i] = seriesToSampleStream(m.series)
	}

	threshold := promhttputil.GapThreshold(gap, base, firstOrNil(fillers))
	merged, fillStat := promhttputil.PriorityMergeSampleStream(base, fillers, threshold)

	if fillStats != nil {
		for i, n := range fillStat.Filled {
			fillStats.Record(members[0].ordinal, members[i+1].ordinal, n)
		}
	}

	return sampleStreamToSeries(merged, members[0].series.Labels())
}

func firstOrNil(s []*model.SampleStream) *model.SampleStream {
	if len(s) == 0 {
		return nil
	}
	return s[0]
}
