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
			name:       "a filler's terminal StaleNaN is kept",
			base:       stream(sp(0, 1), staleAt(10)),
			fillers:    []*model.SampleStream{stream(sp(20, 102), staleAt(30))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(20, 102), staleAt(30)},
			wantFilled: []int{1},
		},
		{
			name:       "a filler's StaleNaN is kept when the base resumes after it",
			base:       stream(sp(0, 1), staleAt(10), sp(60, 6)),
			fillers:    []*model.SampleStream{stream(sp(20, 102), staleAt(30))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(20, 102), staleAt(30), sp(60, 6)},
			wantFilled: []int{1},
		},
		{
			name:       "a filler's StaleNaN that ends nothing it emitted is dropped",
			base:       stream(sp(0, 1), staleAt(10), sp(60, 6)),
			fillers:    []*model.SampleStream{stream(staleAt(20), sp(30, 103))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(30, 103), sp(60, 6)},
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

// Histogram series go stale through a float StaleNaN. Without it the 30 between
// the base's histograms would be below the threshold.
func TestPriorityMergeSampleStream_HistogramGapOpenedByFloatStaleNaN(t *testing.T) {
	base := &model.SampleStream{
		Values:     []model.SamplePair{staleAt(6)},
		Histograms: []model.SampleHistogramPair{hp(0, 1), hp(30, 2)},
	}
	filler := &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(18, 99)}}

	got, stats := PriorityMergeSampleStream(base, []*model.SampleStream{filler}, 40)

	want := []model.SampleHistogramPair{hp(0, 1), hp(18, 99), hp(30, 2)}
	if len(got.Histograms) != len(want) {
		t.Fatalf("Histograms = %v, want %v (filler's sample during the base's stale period should be pulled in)", got.Histograms, want)
	}
	if fmt.Sprint(stats.Filled) != "[1]" {
		t.Fatalf("Filled = %v, want [1]", stats.Filled)
	}
}

func staleHp(t int64) model.SampleHistogramPair {
	return hp(t, math.Float64frombits(value.StaleNaN))
}

func histogramTimes(hs []model.SampleHistogramPair) []int64 {
	var out []int64
	for _, h := range hs {
		out = append(out, int64(h.Timestamp))
	}
	return out
}

func TestPriorityMergeSampleStream_HistogramStaleNaN(t *testing.T) {
	tests := []struct {
		name       string
		base       *model.SampleStream
		filler     *model.SampleStream
		gap        model.Time
		wantValues []int64
		wantHists  []int64
		wantFilled string
	}{
		{
			name:       "histogram marker opens a gap inside the threshold",
			base:       &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 1), staleHp(50), hp(65, 2)}},
			filler:     &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(57, 9)}},
			gap:        20,
			wantHists:  []int64{0, 50, 57, 65},
			wantFilled: "[1]",
		},
		{
			name:       "filler histogram marker is kept",
			base:       &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 1), staleHp(10)}},
			filler:     &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(20, 9), staleHp(30)}},
			gap:        8,
			wantHists:  []int64{0, 10, 20, 30},
			wantFilled: "[1]",
		},
		{
			name: "float filler samples then a histogram marker on the shared timeline",
			base: &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 1), staleHp(10)}},
			filler: &model.SampleStream{
				Values:     []model.SamplePair{sp(20, 9)},
				Histograms: []model.SampleHistogramPair{staleHp(30)},
			},
			gap:        8,
			wantValues: []int64{20},
			wantHists:  []int64{0, 10, 30},
			wantFilled: "[1]",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, stats := PriorityMergeSampleStream(tc.base, []*model.SampleStream{tc.filler}, tc.gap)
			var values []int64
			for _, p := range got.Values {
				values = append(values, int64(p.Timestamp))
			}
			if fmt.Sprint(values) != fmt.Sprint(tc.wantValues) || fmt.Sprint(histogramTimes(got.Histograms)) != fmt.Sprint(tc.wantHists) {
				t.Fatalf("floats at %v, histograms at %v; want %v, %v", values, histogramTimes(got.Histograms), tc.wantValues, tc.wantHists)
			}
			if fmt.Sprint(stats.Filled) != tc.wantFilled {
				t.Fatalf("Filled = %v, want %s", stats.Filled, tc.wantFilled)
			}
		})
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

// A sample of either type occupies its slot: the filler's other type must not
// land next to it.
func TestPriorityMergeSampleStream_SharedTimeline(t *testing.T) {
	tests := []struct {
		name           string
		base, filler   *model.SampleStream
		wantValues     []int64
		wantHistograms []int64
	}{
		{
			name:   "gapless floats, histogram filler",
			base:   &model.SampleStream{Values: []model.SamplePair{sp(0, 1), sp(10, 2), sp(20, 3)}},
			filler: &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 9), hp(10, 9), hp(20, 9)}},

			wantValues: []int64{0, 10, 20},
		},
		{
			name:   "gapless histograms, float filler",
			base:   &model.SampleStream{Histograms: []model.SampleHistogramPair{hp(0, 1), hp(10, 2), hp(20, 3)}},
			filler: &model.SampleStream{Values: []model.SamplePair{sp(0, 9), sp(10, 9), sp(20, 9)}},

			wantHistograms: []int64{0, 10, 20},
		},
		{
			name: "base turns from floats to histograms, filler the other way",
			base: &model.SampleStream{
				Values:     []model.SamplePair{sp(0, 1), sp(10, 2)},
				Histograms: []model.SampleHistogramPair{hp(20, 3), hp(30, 4)},
			},
			filler: &model.SampleStream{
				Values:     []model.SamplePair{sp(20, 9), sp(30, 9), sp(40, 9)},
				Histograms: []model.SampleHistogramPair{hp(0, 9), hp(10, 9)},
			},

			wantValues:     []int64{0, 10, 40},
			wantHistograms: []int64{20, 30},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, _ := PriorityMergeSampleStream(test.base, []*model.SampleStream{test.filler}, 15)
			var values, histograms []int64
			for _, p := range got.Values {
				values = append(values, int64(p.Timestamp))
			}
			for _, p := range got.Histograms {
				histograms = append(histograms, int64(p.Timestamp))
			}
			if fmt.Sprint(values) != fmt.Sprint(test.wantValues) || fmt.Sprint(histograms) != fmt.Sprint(test.wantHistograms) {
				t.Fatalf("floats at %v, histograms at %v; want floats at %v, histograms at %v", values, histograms, test.wantValues, test.wantHistograms)
			}
		})
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
	live := false
	for _, p := range candidates {
		ts := p.Timestamp
		if stale || subLo == nil || ts-*subLo >= gap {
			out = append(out, referenceFillRange(subLo, &ts, seqs, lvl+1, gap)...)
		}
		if value.IsStaleNaN(float64(p.Value)) {
			if lvl == 0 || live {
				out = append(out, p)
			}
			stale = true
			live = false
		} else {
			out = append(out, p)
			stale = false
			live = true
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

func TestPriorityMergeSampleStream_FillerMarkersAcrossLevels(t *testing.T) {
	tests := []struct {
		name       string
		base       *model.SampleStream
		fillers    []*model.SampleStream
		gap        model.Time
		wantValues []model.SamplePair
	}{
		{
			name:       "level 2 fills after the level 1 marker",
			base:       stream(sp(0, 1), staleAt(10)),
			fillers:    []*model.SampleStream{stream(sp(20, 101), staleAt(30)), stream(sp(40, 201))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(20, 101), staleAt(30), sp(40, 201)},
		},
		{
			name:       "level 2 marker is kept after level 1 marker",
			base:       stream(sp(0, 1), staleAt(10)),
			fillers:    []*model.SampleStream{stream(sp(20, 101), staleAt(30)), stream(sp(40, 201), staleAt(50))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), staleAt(10), sp(20, 101), staleAt(30), sp(40, 201), staleAt(50)},
		},
		{
			name:       "level 1 marker at effHi is excluded, one before is kept",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(sp(10, 101), staleAt(28)), stream(sp(20, 201))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(20, 201), sp(30, 2)},
		},
		{
			name:       "level 1 marker one before effHi",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(sp(10, 101), staleAt(27)), stream(sp(20, 201))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(20, 201), staleAt(27), sp(30, 2)},
		},
		{
			name:       "level 1 marker at effLo is excluded",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(staleAt(2), sp(10, 101))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(30, 2)},
		},
		{
			name:       "level 1 marker just inside effLo ends nothing and is dropped",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(staleAt(3), sp(10, 101))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(30, 2)},
		},
		{
			name:       "level 1 marker on a base timestamp never lands",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(sp(0, 100), sp(10, 101), staleAt(30))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(30, 2)},
		},
		{
			name:       "consecutive filler markers keep only the first",
			base:       stream(sp(0, 1), sp(30, 2)),
			fillers:    []*model.SampleStream{stream(sp(10, 101), staleAt(12), staleAt(14))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), staleAt(12), sp(30, 2)},
		},
		{
			name:       "live filler with an internal gap filled deeper still ends with its marker",
			base:       stream(sp(0, 1), sp(60, 2)),
			fillers:    []*model.SampleStream{stream(sp(10, 101), staleAt(30)), stream(sp(20, 201))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(20, 201), staleAt(30), sp(60, 2)},
		},
		{
			name:       "dropped level 1 marker still lets level 2 fill after it",
			base:       stream(sp(0, 1), sp(60, 2)),
			fillers:    []*model.SampleStream{stream(staleAt(20)), stream(sp(25, 201))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(25, 201), sp(60, 2)},
		},
		{
			name:       "liveness restarts per base gap",
			base:       stream(sp(0, 1), sp(30, 2), sp(60, 3)),
			fillers:    []*model.SampleStream{stream(sp(10, 101), sp(40, 102), staleAt(50))},
			gap:        8,
			wantValues: []model.SamplePair{sp(0, 1), sp(10, 101), sp(30, 2), sp(40, 102), staleAt(50), sp(60, 3)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := PriorityMergeSampleStream(tc.base, tc.fillers, tc.gap)
			assertSamplePairsEqual(t, got.Values, tc.wantValues)
		})
	}
}

type tagged struct {
	t     int64
	stale bool
	id    float64
	hist  bool
}

func taggedOf(s *model.SampleStream) []tagged {
	var out []tagged
	for _, p := range s.Values {
		out = append(out, tagged{int64(p.Timestamp), value.IsStaleNaN(float64(p.Value)), float64(p.Value), false})
	}
	for _, p := range s.Histograms {
		out = append(out, tagged{int64(p.Timestamp), value.IsStaleNaN(float64(p.Histogram.Sum)), float64(p.Histogram.Sum), true})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].t < out[j].t })
	return out
}

// Histograms must behave exactly like floats on the shared timeline: run the
// float oracle on an all-float copy, then compare per timestamp with the
// sample type preserved. Also checks structural invariants that don't depend
// on the algorithm: sorted unique output, every base sample kept, and
// everything else lands only inside a base gap and outside the margin.
func TestPriorityMergeSampleStream_MixedTypesMatchReferenceAndInvariants(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	next := 0.0
	series := func(n int, maxT int64) (floats []model.SamplePair, mixed *model.SampleStream) {
		mixed = &model.SampleStream{}
		seen := map[int64]bool{}
		var ts []int64
		for i := 0; i < n; i++ {
			x := rnd.Int63n(maxT)
			if !seen[x] {
				seen[x] = true
				ts = append(ts, x)
			}
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		for _, x := range ts {
			v := model.SampleValue(math.Float64frombits(value.StaleNaN))
			if rnd.Float64() >= 0.2 {
				next++
				v = model.SampleValue(next)
			}
			floats = append(floats, model.SamplePair{Timestamp: model.Time(x), Value: v})
			if rnd.Intn(2) == 0 {
				mixed.Values = append(mixed.Values, floats[len(floats)-1])
			} else {
				mixed.Histograms = append(mixed.Histograms, model.SampleHistogramPair{Timestamp: model.Time(x), Histogram: &model.SampleHistogram{Sum: model.FloatString(v), Count: 1}})
			}
		}
		return floats, mixed
	}

	for trial := 0; trial < 2000; trial++ {
		baseF, baseM := series(rnd.Intn(10), 400)
		var fillersF [][]model.SamplePair
		var fillersM []*model.SampleStream
		for i, n := 0, rnd.Intn(4); i < n; i++ {
			f, m := series(rnd.Intn(12), 400)
			fillersF = append(fillersF, f)
			fillersM = append(fillersM, m)
		}
		gap := model.Time(1 + rnd.Intn(60))

		want := referenceFillGaps(baseF, fillersF, gap)
		got, _ := PriorityMergeSampleStream(baseM, fillersM, gap)
		gotT := taggedOf(got)

		fail := func(format string, args ...any) {
			t.Fatalf("trial %d gap=%d base=%v fillers=%v\n got: %v\n"+format, append([]any{trial, gap, baseF, fillersF, gotT}, args...)...)
		}

		if len(gotT) != len(want) {
			fail("want: %v", want)
		}
		for i, w := range want {
			g := gotT[i]
			if g.t != int64(w.Timestamp) || g.stale != value.IsStaleNaN(float64(w.Value)) || (!g.stale && g.id != float64(w.Value)) {
				fail("mismatch at %d, want: %v", i, want)
			}
		}

		for _, g := range gotT {
			if g.stale {
				continue
			}
			var src *model.SampleStream
			for _, s := range append([]*model.SampleStream{baseM}, fillersM...) {
				for _, p := range taggedOf(s) {
					if p.id == g.id {
						src = s
						if p.hist != g.hist {
							fail("sample id %v changed type", g.id)
						}
					}
				}
			}
			if src == nil {
				fail("sample id %v from nowhere", g.id)
			}
		}

		baseT := taggedOf(baseM)
		baseAt := map[int64]bool{}
		for _, b := range baseT {
			baseAt[b.t] = true
		}
		for i, g := range gotT {
			if i > 0 && g.t <= gotT[i-1].t {
				fail("not strictly increasing at %d", i)
			}
		}
		for _, b := range baseT {
			found := false
			for _, g := range gotT {
				if g.t == b.t && g.stale == b.stale && g.hist == b.hist && (g.stale || g.id == b.id) {
					found = true
				}
			}
			if !found {
				fail("base sample %v lost", b)
			}
		}
		margin := int64(gap / 4)
		for _, g := range gotT {
			if baseAt[g.t] {
				continue
			}
			var prev, nxt *tagged
			for i := range baseT {
				if baseT[i].t < g.t {
					prev = &baseT[i]
				} else if baseT[i].t > g.t && nxt == nil {
					nxt = &baseT[i]
				}
			}
			if prev != nil && g.t-prev.t <= margin || nxt != nil && nxt.t-g.t <= margin {
				fail("filler sample %v inside margin of base", g)
			}
			if prev != nil && nxt != nil && !prev.stale && nxt.t-prev.t < int64(gap) {
				fail("filler sample %v inside gapless base coverage", g)
			}
		}
	}
}
