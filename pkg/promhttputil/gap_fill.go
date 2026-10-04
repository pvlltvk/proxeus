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

	// Floats and histograms share one timeline per stream, so a base sample of
	// either type keeps a filler's sample of the other type out of its slot.
	seqs := make([][]timelineSample, len(fillers)+1)
	seqs[0] = timeline(base)
	for i, f := range fillers {
		seqs[i+1] = timeline(f)
	}
	merged := fill(nil, nil, seqs, 0, gap, stats.Filled, make([]int, len(seqs)))

	out := &model.SampleStream{Metric: base.Metric}
	for _, p := range merged {
		if p.h != nil {
			out.Histograms = append(out.Histograms, model.SampleHistogramPair{Timestamp: p.t, Histogram: p.h})
		} else {
			out.Values = append(out.Values, model.SamplePair{Timestamp: p.t, Value: p.f})
		}
	}
	return out, stats
}

// timelineSample is a float sample, or a histogram one when h is set.
type timelineSample struct {
	t model.Time
	f model.SampleValue
	h *model.SampleHistogram
}

func (p timelineSample) stale() bool { return p.h == nil && value.IsStaleNaN(float64(p.f)) }

func timeline(s *model.SampleStream) []timelineSample {
	out := make([]timelineSample, 0, len(s.Values)+len(s.Histograms))
	i, j := 0, 0
	for i < len(s.Values) || j < len(s.Histograms) {
		if j == len(s.Histograms) || (i < len(s.Values) && s.Values[i].Timestamp <= s.Histograms[j].Timestamp) {
			out = append(out, timelineSample{t: s.Values[i].Timestamp, f: s.Values[i].Value})
			i++
		} else {
			out = append(out, timelineSample{t: s.Histograms[j].Timestamp, h: s.Histograms[j].Histogram})
			j++
		}
	}
	return out
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
	ts := timeline(s)
	var intervals []model.Time
	for i := 1; i < len(ts); i++ {
		if d := ts[i].t - ts[i-1].t; d > 0 {
			intervals = append(intervals, d)
		}
	}
	if len(intervals) == 0 {
		return 0, false
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	return intervals[len(intervals)/2], true
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

// fill emits seqs[lvl]'s samples within (lo, hi) and recurses into
// seqs[lvl+1] for the gaps between them. Calls at one level move forward in
// time, so cursors[lvl] lets each resume where the last stopped instead of
// rescanning.
func fill(lo, hi *model.Time, seqs [][]timelineSample, lvl int, gap model.Time, filled []int, cursors []int) []timelineSample {
	effLo, effHi := marginBounds(lo, hi, gap)
	seq := seqs[lvl]
	i := cursors[lvl]
	for i < len(seq) && effLo != nil && seq[i].t <= *effLo {
		i++
	}

	var out []timelineSample
	// Gap sizes are measured from the real neighbor lo, not effLo; using effLo
	// would count the margin twice.
	subLo := lo
	stale := false

	for ; i < len(seq); i++ {
		p := seq[i]
		if effHi != nil && p.t >= *effHi {
			break
		}

		if needsFill(stale, subLo, timePtr(p.t), gap) && lvl+1 < len(seqs) {
			out = append(out, fill(subLo, timePtr(p.t), seqs, lvl+1, gap, filled, cursors)...)
		}

		if p.stale() {
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
		subLo = timePtr(p.t)
	}
	cursors[lvl] = i

	if needsFill(stale, subLo, hi, gap) && lvl+1 < len(seqs) {
		out = append(out, fill(subLo, hi, seqs, lvl+1, gap, filled, cursors)...)
	}
	return out
}
