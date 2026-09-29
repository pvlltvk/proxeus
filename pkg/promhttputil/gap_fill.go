package promhttputil

import (
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
)

// FillStats.Filled[i] is the number of samples taken from fillers[i].
type FillStats struct {
	Filled []int
}

// PriorityMergeSampleStream keeps every sample of base and takes samples from
// fillers only inside base's gaps, trying fillers in order; a gap a filler
// also has is passed on to the next one.
//
// A gap is the span before base's first sample, after its last, between two
// samples at least gap apart, or after a StaleNaN (the source stopped
// scraping). StaleNaNs are kept so the engine's lookback doesn't carry the
// last value across the gap.
func PriorityMergeSampleStream(base *model.SampleStream, fillers []*model.SampleStream, gap model.Time) (*model.SampleStream, FillStats) {
	stats := FillStats{Filled: make([]int, len(fillers))}
	if gap <= 0 {
		gap = 1
	}

	floatSeqs := make([][]model.SamplePair, len(fillers)+1)
	floatSeqs[0] = base.Values
	for i, f := range fillers {
		floatSeqs[i+1] = f.Values
	}
	values := fillFloats(nil, nil, floatSeqs, 0, gap, stats.Filled, make([]int, len(floatSeqs)))

	histSeqs := make([][]model.SampleHistogramPair, len(fillers)+1)
	histSeqs[0] = base.Histograms
	for i, f := range fillers {
		histSeqs[i+1] = f.Histograms
	}
	histograms := fillHistograms(nil, nil, histSeqs, 0, gap, stats.Filled, make([]int, len(histSeqs)))

	return &model.SampleStream{Metric: base.Metric, Values: values, Histograms: histograms}, stats
}

// GapThreshold returns override if set, otherwise twice the median sample
// interval (dynamicBufferForStream returns half of it), or 0 when there are
// too few samples to estimate.
func GapThreshold(override model.Time, base, fillerHint *model.SampleStream) model.Time {
	if override > 0 {
		return override
	}
	if fillerHint == nil {
		fillerHint = &model.SampleStream{}
	}
	half, ok := dynamicBufferForStream(base, fillerHint)
	if !ok {
		return 0
	}
	return half * 4
}

// marginBounds keeps filled samples gap/4 away from a real neighbor, since
// backends scrape at independent phases. Not gap/2: with the auto threshold a
// single missing step sits exactly gap/2 from both neighbors and would never
// be filled.
func marginBounds(lo, hi *model.Time, gap model.Time) (*model.Time, *model.Time) {
	margin := gap / 4
	var effLo, effHi *model.Time
	if lo != nil {
		t := *lo + margin
		effLo = &t
	}
	if hi != nil {
		t := *hi - margin
		effHi = &t
	}
	return effLo, effHi
}

func needsFill(stale bool, subLo, upper *model.Time, gap model.Time) bool {
	if stale || subLo == nil || upper == nil {
		return true
	}
	return *upper-*subLo >= gap
}

func timePtr(t model.Time) *model.Time { return &t }

// fillFloats emits seqs[lvl]'s samples within (lo, hi) and recurses into
// seqs[lvl+1] for the gaps between them. Calls at one level move forward in
// time, so cursors[lvl] lets each resume where the last stopped instead of
// rescanning.
func fillFloats(lo, hi *model.Time, seqs [][]model.SamplePair, lvl int, gap model.Time, filled []int, cursors []int) []model.SamplePair {
	effLo, effHi := marginBounds(lo, hi, gap)
	seq := seqs[lvl]
	i := cursors[lvl]
	for i < len(seq) && effLo != nil && seq[i].Timestamp <= *effLo {
		i++
	}

	var out []model.SamplePair
	// Gap sizes are measured from the real neighbor lo, not effLo; using effLo
	// would count the margin twice.
	subLo := lo
	stale := false

	for ; i < len(seq); i++ {
		p := seq[i]
		if effHi != nil && p.Timestamp >= *effHi {
			break
		}

		if needsFill(stale, subLo, timePtr(p.Timestamp), gap) && lvl+1 < len(seqs) {
			out = append(out, fillFloats(subLo, timePtr(p.Timestamp), seqs, lvl+1, gap, filled, cursors)...)
		}

		if value.IsStaleNaN(float64(p.Value)) {
			if lvl == 0 {
				out = append(out, p)
			}
			stale = true
		} else {
			out = append(out, p)
			if lvl > 0 {
				filled[lvl-1]++
			}
			stale = false
		}
		subLo = timePtr(p.Timestamp)
	}
	cursors[lvl] = i

	if needsFill(stale, subLo, hi, gap) && lvl+1 < len(seqs) {
		out = append(out, fillFloats(subLo, hi, seqs, lvl+1, gap, filled, cursors)...)
	}
	return out
}

// fillHistograms mirrors fillFloats. Staleness of a histogram series is
// signaled by a float StaleNaN, so there is nothing to track here.
func fillHistograms(lo, hi *model.Time, seqs [][]model.SampleHistogramPair, lvl int, gap model.Time, filled []int, cursors []int) []model.SampleHistogramPair {
	effLo, effHi := marginBounds(lo, hi, gap)
	seq := seqs[lvl]
	i := cursors[lvl]
	for i < len(seq) && effLo != nil && seq[i].Timestamp <= *effLo {
		i++
	}

	var out []model.SampleHistogramPair
	subLo := lo

	for ; i < len(seq); i++ {
		p := seq[i]
		if effHi != nil && p.Timestamp >= *effHi {
			break
		}

		if needsFill(false, subLo, timePtr(p.Timestamp), gap) && lvl+1 < len(seqs) {
			out = append(out, fillHistograms(subLo, timePtr(p.Timestamp), seqs, lvl+1, gap, filled, cursors)...)
		}

		out = append(out, p)
		if lvl > 0 {
			filled[lvl-1]++
		}
		subLo = timePtr(p.Timestamp)
	}
	cursors[lvl] = i

	if needsFill(false, subLo, hi, gap) && lvl+1 < len(seqs) {
		out = append(out, fillHistograms(subLo, hi, seqs, lvl+1, gap, filled, cursors)...)
	}
	return out
}
