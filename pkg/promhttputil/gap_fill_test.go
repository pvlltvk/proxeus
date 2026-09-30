package promhttputil

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/value"
)

func sp(t int64, v float64) model.SamplePair {
	return model.SamplePair{Timestamp: model.Time(t), Value: model.SampleValue(v)}
}

func staleAt(t int64) model.SamplePair {
	return sp(t, math.Float64frombits(value.StaleNaN))
}

func stream(pairs ...model.SamplePair) *model.SampleStream {
	return &model.SampleStream{Values: pairs}
}

func hp(t int64, sum float64) model.SampleHistogramPair {
	return model.SampleHistogramPair{Timestamp: model.Time(t), Histogram: &model.SampleHistogram{Sum: model.FloatString(sum), Count: model.FloatString(1)}}
}

func TestPriorityMergeSampleStream(t *testing.T) {
	tests := []struct {
		name       string
		base       *model.SampleStream
		fillers    []*model.SampleStream
		gap        model.Time
		wantValues []model.SamplePair
		wantFilled []int
	}{
		{
			name:       "gapless base is untouched and nothing is read from fillers",
			base:       stream(sp(0, 1), sp(10, 2), sp(20, 3)),
			fillers:    []*model.SampleStream{stream(sp(0, 100), sp(10, 101), sp(20, 102))},
			gap:        15,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 2), sp(20, 3)},
			wantFilled: []int{0},
		},
		{
			// The filler also has points at 0 and 40 (the base's own
			// timestamps), which the margin excludes so they never
			// interleave right next to a real base sample.
			name:       "gap in the middle is filled from the filler",
			base:       stream(sp(0, 1), sp(10, 2), sp(40, 5)),
			fillers:    []*model.SampleStream{stream(sp(0, 100), sp(10, 101), sp(20, 102), sp(30, 103), sp(40, 104))},
			gap:        15,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 2), sp(20, 102), sp(30, 103), sp(40, 5)},
			wantFilled: []int{2},
		},
		{
			name: "leading and trailing gaps are filled",
			base: stream(sp(20, 1), sp(30, 2)),
			fillers: []*model.SampleStream{
				stream(sp(0, 100), sp(10, 101), sp(40, 104), sp(50, 105)),
			},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 100), sp(10, 101), sp(20, 1), sp(30, 2), sp(40, 104), sp(50, 105)},
			wantFilled: []int{4},
		},
		{
			name:       "no fillers leaves base untouched",
			base:       stream(sp(0, 1), sp(40, 5)),
			fillers:    nil,
			gap:        15,
			wantValues: []model.SamplePair{sp(0, 1), sp(40, 5)},
			wantFilled: []int{},
		},
		{
			name: "second-priority filler also has a gap that the third fills",
			base: stream(sp(0, 1), sp(50, 5)),
			fillers: []*model.SampleStream{
				stream(sp(10, 201)),
				stream(sp(20, 302), sp(30, 303)),
			},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 201), sp(20, 302), sp(30, 303), sp(50, 5)},
			wantFilled: []int{1, 2},
		},
		{
			name: "phase-offset raw samples: filler points outside the gap margin are not interleaved",
			base: stream(sp(0, 1), sp(4, 1), sp(8, 1), sp(50, 1), sp(54, 1), sp(58, 1)),
			fillers: []*model.SampleStream{
				stream(sp(2, 200), sp(6, 200), sp(24, 200), sp(36, 200), sp(52, 200), sp(56, 200)),
			},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(4, 1), sp(8, 1), sp(24, 200), sp(36, 200), sp(50, 1), sp(54, 1), sp(58, 1)},
			wantFilled: []int{2},
		},
		{
			name:       "a StaleNaN in the base starts a gap that the filler fills",
			base:       stream(sp(0, 1), staleAt(10), sp(60, 6)),
			fillers:    []*model.SampleStream{stream(sp(20, 102), sp(30, 103), sp(40, 104))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(20, 102), sp(30, 103), sp(40, 104), sp(60, 6)},
			wantFilled: []int{3},
		},
		{
			name:       "trailing StaleNaN with nothing to fill from keeps the marker",
			base:       stream(sp(0, 1), staleAt(10)),
			fillers:    nil,
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10)},
			wantFilled: []int{},
		},
		{
			name:       "a StaleNaN forces a gap even when the next base sample arrives well inside the gap threshold",
			base:       stream(sp(0, 1), staleAt(50), sp(65, 2)),
			fillers:    []*model.SampleStream{stream(sp(57, 999))},
			gap:        20,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(50), sp(57, 999), sp(65, 2)},
			wantFilled: []int{1},
		},
		{
			name:       "filler samples exactly on the margin boundary are excluded, one unit inside is kept",
			base:       stream(sp(0, 1), sp(100, 2)),
			fillers:    []*model.SampleStream{stream(sp(4, 901), sp(5, 902), sp(95, 903), sp(96, 904))},
			gap:        16,
			wantValues: []model.SamplePair{sp(0, 1), sp(5, 902), sp(95, 903), sp(100, 2)},
			wantFilled: []int{2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, stats := PriorityMergeSampleStream(tc.base, tc.fillers, tc.gap)
			assertSamplePairsEqual(t, got.Values, tc.wantValues)
			if fmt.Sprint(stats.Filled) != fmt.Sprint(tc.wantFilled) {
				t.Errorf("Filled = %v, want %v", stats.Filled, tc.wantFilled)
			}
		})
	}
}

// assertSamplePairsEqual compares values one at a time so a StaleNaN
// (NaN != NaN under ==) doesn't make an otherwise-correct result look wrong.
func assertSamplePairsEqual(t *testing.T, got, want []model.SamplePair) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	for i := range got {
		if got[i].Timestamp != want[i].Timestamp {
			t.Fatalf("got %v\nwant %v", got, want)
		}
		gv, wv := float64(got[i].Value), float64(want[i].Value)
		if math.IsNaN(gv) && math.IsNaN(wv) {
			continue
		}
		if gv != wv {
			t.Fatalf("got %v\nwant %v", got, want)
		}
	}
}

func TestPriorityMergeSampleStream_Histograms(t *testing.T) {
	base := &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 1), hp(40, 5)}}
	filler := &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(10, 101), hp(20, 102), hp(30, 103)}}

	got, stats := PriorityMergeSampleStream(base, []*model.SampleStream{filler}, 15)

	want := []model.SampleHistogramPair{hp(0, 1), hp(10, 101), hp(20, 102), hp(30, 103), hp(40, 5)}
	if len(got.Histograms) != len(want) {
		t.Fatalf("Histograms = %v, want %v", got.Histograms, want)
	}
	for i := range want {
		if got.Histograms[i].Timestamp != want[i].Timestamp || got.Histograms[i].Histogram.Sum != want[i].Histogram.Sum {
			t.Fatalf("Histograms[%d] = %v, want %v", i, got.Histograms[i], want[i])
		}
	}
	if fmt.Sprint(stats.Filled) != "[3]" {
		t.Fatalf("Filled = %v, want [3]", stats.Filled)
	}
}

// Histogram series go stale through a float StaleNaN.
func TestPriorityMergeSampleStream_HistogramGapOpenedByFloatStaleNaN(t *testing.T) {
	base := &model.SampleStream{
		Values:     []model.SamplePair{staleAt(6)},
		Histograms: []model.SampleHistogramPair{hp(0, 1), hp(15, 2)},
	}
	filler := &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(7, 99)}}

	got, stats := PriorityMergeSampleStream(base, []*model.SampleStream{filler}, 20)

	want := []model.SampleHistogramPair{hp(0, 1), hp(7, 99), hp(15, 2)}
	if len(got.Histograms) != len(want) {
		t.Fatalf("Histograms = %v, want %v (filler's sample during the base's stale period should be pulled in)", got.Histograms, want)
	}
	if fmt.Sprint(stats.Filled) != "[1]" {
		t.Fatalf("Filled = %v, want [1]", stats.Filled)
	}
}

// A denser filler must not shrink the threshold below the base's own spacing.
func TestGapThreshold_AutoIgnoresDenserFiller(t *testing.T) {
	base := stream(sp(0, 1), sp(60000, 1), sp(120000, 1))
	var fillerPairs []model.SamplePair
	for ts := int64(0); ts <= 45000; ts += 5000 {
		fillerPairs = append(fillerPairs, sp(ts, 999))
	}
	filler := &model.SampleStream{Values: fillerPairs}

	threshold := GapThreshold(0, base, filler)
	got, _ := PriorityMergeSampleStream(base, []*model.SampleStream{filler}, threshold)

	assertSamplePairsEqual(t, got.Values, base.Values)
}

func TestPriorityMergeSampleStream_MixedFloatAndHistogram(t *testing.T) {
	base := &model.SampleStream{
		Values:     []model.SamplePair{sp(0, 1), sp(40, 5)},
		Histograms: []model.SampleHistogramPair{hp(100, 1), hp(140, 5)},
	}
	filler := &model.SampleStream{
		Values:     []model.SamplePair{sp(10, 101), sp(20, 102), sp(30, 103)},
		Histograms: []model.SampleHistogramPair{hp(110, 201), hp(120, 202), hp(130, 203)},
	}

	got, stats := PriorityMergeSampleStream(base, []*model.SampleStream{filler}, 15)

	assertSamplePairsEqual(t, got.Values, []model.SamplePair{sp(0, 1), sp(10, 101), sp(20, 102), sp(30, 103), sp(40, 5)})
	if len(got.Histograms) != 5 {
		t.Fatalf("Histograms = %v, want 5 points", got.Histograms)
	}
	if fmt.Sprint(stats.Filled) != "[6]" {
		t.Fatalf("Filled = %v, want [6] (3 float + 3 histogram)", stats.Filled)
	}
}

// referenceFillGaps is a naive oracle: for each gap it scans every filler from
// the start.
func referenceFillGaps(base []model.SamplePair, fillers [][]model.SamplePair, gap model.Time) []model.SamplePair {
	return referenceFillRange(nil, nil, append([][]model.SamplePair{base}, fillers...), 0, gap)
}

func referenceFillRange(lo, hi *model.Time, seqs [][]model.SamplePair, lvl int, gap model.Time) []model.SamplePair {
	if lvl >= len(seqs) {
		return nil
	}
	margin := gap / 4
	inWindow := func(ts model.Time) bool {
		if lo != nil && ts <= *lo+margin {
			return false
		}
		if hi != nil && ts >= *hi-margin {
			return false
		}
		return true
	}

	var candidates []model.SamplePair
	for _, p := range seqs[lvl] {
		if inWindow(p.Timestamp) {
			candidates = append(candidates, p)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Timestamp < candidates[j].Timestamp })

	var out []model.SamplePair
	subLo := lo
	stale := false
	for _, p := range candidates {
		ts := p.Timestamp
		if stale || subLo == nil || ts-*subLo >= gap {
			out = append(out, referenceFillRange(subLo, &ts, seqs, lvl+1, gap)...)
		}
		if value.IsStaleNaN(float64(p.Value)) {
			if lvl == 0 {
				out = append(out, p)
			}
			stale = true
		} else {
			out = append(out, p)
			stale = false
		}
		subLo = &ts
	}
	if stale || subLo == nil || hi == nil {
		out = append(out, referenceFillRange(subLo, hi, seqs, lvl+1, gap)...)
	} else if *hi-*subLo >= gap {
		out = append(out, referenceFillRange(subLo, hi, seqs, lvl+1, gap)...)
	}
	return out
}

func TestPriorityMergeSampleStream_MatchesReference(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))

	randomSeries := func(n int, maxT int64, staleChance float64) []model.SamplePair {
		if n == 0 {
			return nil
		}
		ts := make([]int64, n)
		for i := range ts {
			ts[i] = rnd.Int63n(maxT)
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		out := make([]model.SamplePair, 0, n)
		last := int64(-1)
		for _, x := range ts {
			if x == last {
				continue // keep timestamps unique, same as real series
			}
			last = x
			if rnd.Float64() < staleChance {
				out = append(out, staleAt(x))
			} else {
				out = append(out, sp(x, rnd.Float64()*100))
			}
		}
		return out
	}

	for trial := 0; trial < 200; trial++ {
		base := randomSeries(rnd.Intn(10), 500, 0.1)
		var fillers [][]model.SamplePair
		fillerStreams := make([]*model.SampleStream, rnd.Intn(3))
		for i := range fillerStreams {
			f := randomSeries(rnd.Intn(10), 500, 0.1)
			fillers = append(fillers, f)
			fillerStreams[i] = &model.SampleStream{Values: f}
		}
		gap := model.Time(10 + rnd.Intn(50))

		want := referenceFillGaps(base, fillers, gap)
		got, _ := PriorityMergeSampleStream(&model.SampleStream{Values: base}, fillerStreams, gap)

		if fmt.Sprint(got.Values) != fmt.Sprint(want) {
			t.Fatalf("trial %d: base=%v fillers=%v gap=%d\n got: %v\nwant: %v", trial, base, fillers, gap, got.Values, want)
		}
	}
}
