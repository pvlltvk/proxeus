package promhttputil

import (
	"sort"

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
	staleSeqs := make([][]model.Time, len(fillers)+1)
	histSeqs[0], staleSeqs[0] = base.Histograms, staleTimes(base.Values)
	for i, f := range fillers {
		histSeqs[i+1], staleSeqs[i+1] = f.Histograms, staleTimes(f.Values)
	}
	histograms := fillHistograms(nil, nil, histSeqs, staleSeqs, 0, gap, stats.Filled, make([]int, len(histSeqs)))

	return &model.SampleStream{Metric: base.Metric, Values: values, Histograms: histograms}, stats
}

// GapThreshold returns override if set, otherwise twice the median sample
// interval of base, or of fillerHint when base has fewer than two samples, or
// 0 when neither does. Base's own spacing is never mixed with the filler's: a
// denser filler would make the base's normal spacing look like gaps.
func GapThreshold(override model.Time, base, fillerHint *model.SampleStream) model.Time {
	if override > 0 {
		return override
	}
	for _, s := range []*model.SampleStream{base, fillerHint} {
		if s == nil {
			continue
		}
		if m, ok := medianInterval(s); ok {
			return 2 * m
		}
	}
	return 0
}

func medianInterval(s *model.SampleStream) (model.Time, bool) {
	ts := make([]model.Time, 0, max(len(s.Values), len(s.Histograms)))
	if len(s.Values) >= len(s.Histograms) {
		for _, p := range s.Values {
			ts = append(ts, p.Timestamp)
		}
	} else {
		for _, p := range s.Histograms {
			ts = append(ts, p.Timestamp)
		}
	}
	var intervals []model.Time
	for i := 1; i < len(ts); i++ {
		if d := ts[i] - ts[i-1]; d > 0 {
			intervals = append(intervals, d)
		}
	}
	if len(intervals) == 0 {
		return 0, false
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	return intervals[len(intervals)/2], true
}

func staleTimes(values []model.SamplePair) []model.Time {
	var ts []model.Time
	for _, p := range values {
		if value.IsStaleNaN(float64(p.Value)) {
			ts = append(ts, p.Timestamp)
		}
	}
	return ts
}

func staleBetween(stale []model.Time, lo, hi *model.Time) bool {
	i := 0
	if lo != nil {
		i = sort.Search(len(stale), func(k int) bool { return stale[k] >= *lo })
	}
	return i < len(stale) && (hi == nil || stale[i] < *hi)
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

// fillHistograms mirrors fillFloats. A histogram series goes stale through a
// float StaleNaN, so stale[lvl] holds those timestamps from the float side.
func fillHistograms(lo, hi *model.Time, seqs [][]model.SampleHistogramPair, stale [][]model.Time, lvl int, gap model.Time, filled []int, cursors []int) []model.SampleHistogramPair {
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

		if needsFill(staleBetween(stale[lvl], subLo, timePtr(p.Timestamp)), subLo, timePtr(p.Timestamp), gap) && lvl+1 < len(seqs) {
			out = append(out, fillHistograms(subLo, timePtr(p.Timestamp), seqs, stale, lvl+1, gap, filled, cursors)...)
		}

		out = append(out, p)
		if lvl > 0 {
			filled[lvl-1]++
		}
		subLo = timePtr(p.Timestamp)
	}
	cursors[lvl] = i

	if needsFill(staleBetween(stale[lvl], subLo, hi), subLo, hi, gap) && lvl+1 < len(seqs) {
		out = append(out, fillHistograms(subLo, hi, seqs, stale, lvl+1, gap, filled, cursors)...)
	}
	return out
}
