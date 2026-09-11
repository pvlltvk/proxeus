package promclient

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// matrixSet builds the ordinal-tagged SeriesSet a backend contributes to the
// merge, in the shape the query path produces (one materialized series per
// stream).
func matrixSet(ordinal int, mat model.Matrix) ordinalSeriesSet {
	return ordinalSeriesSet{ordinal: ordinal, ss: ModelValueToSeriesSet(mat, nil, nil)}
}

// dedupStream is a one-sample matrix stream, the shape an instant query yields.
func dedupStream(m model.Metric, v model.SampleValue) *model.SampleStream {
	return &model.SampleStream{Metric: m, Values: []model.SamplePair{{Timestamp: 100, Value: v}}}
}

func TestDedupSeriesSets(t *testing.T) {
	ignore := []string{"backend"}

	for _, tc := range []struct {
		name      string
		ignore    []string
		inputs    []model.Matrix // index == source ordinal
		want      model.Matrix
		wantPairs map[[2]int]int
	}{
		{
			name:   "disjoint series all survive in ordinal order",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
				{dedupStream(model.Metric{"__name__": "mem", "backend": "sg1"}, 2)},
			},
			want: model.Matrix{
				dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1),
				dedupStream(model.Metric{"__name__": "mem", "backend": "sg1"}, 2),
			},
		},
		{
			name:   "lowest ordinal wins and keeps its own labels",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 99)},
			},
			want:      model.Matrix{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
			wantPairs: map[[2]int]int{{0, 1}: 1},
		},
		{
			name:   "multi-loser records one pair per loser",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 2)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg2"}, 3)},
			},
			want:      model.Matrix{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
			wantPairs: map[[2]int]int{{0, 1}: 1, {0, 2}: 1},
		},
		{
			name:   "collision between middle ordinals is attributed exactly",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "mem", "backend": "sg0"}, 7)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg2"}, 2)},
			},
			want: model.Matrix{
				dedupStream(model.Metric{"__name__": "mem", "backend": "sg0"}, 7),
				dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 1),
			},
			wantPairs: map[[2]int]int{{1, 2}: 1},
		},
		{
			name:   "exact full-label duplicate keeps first, is not a collision",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "shared"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "shared"}, 2)},
			},
			want: model.Matrix{dedupStream(model.Metric{"__name__": "cpu", "backend": "shared"}, 1)},
		},
		{
			name:   "series missing the ignored label still collides",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 2)},
			},
			want:      model.Matrix{dedupStream(model.Metric{"__name__": "cpu"}, 1)},
			wantPairs: map[[2]int]int{{0, 1}: 1},
		},
		{
			name:   "empty ignore set makes external labels significant",
			ignore: nil,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 2)},
			},
			want: model.Matrix{
				dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1),
				dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 2),
			},
		},
		{
			name:   "an ignored label is not enough to make series equal",
			ignore: ignore,
			inputs: []model.Matrix{
				{dedupStream(model.Metric{"__name__": "cpu", "instance": "a", "backend": "sg0"}, 1)},
				{dedupStream(model.Metric{"__name__": "cpu", "instance": "b", "backend": "sg1"}, 2)},
			},
			want: model.Matrix{
				dedupStream(model.Metric{"__name__": "cpu", "instance": "a", "backend": "sg0"}, 1),
				dedupStream(model.Metric{"__name__": "cpu", "instance": "b", "backend": "sg1"}, 2),
			},
		},
		{
			name:   "an empty backend contributes nothing",
			ignore: ignore,
			inputs: []model.Matrix{
				{},
				{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 5)},
			},
			want: model.Matrix{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg1"}, 5)},
		},
		{
			name:   "a single backend is passed through without intra-group dedup",
			ignore: ignore,
			inputs: []model.Matrix{
				{
					dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1),
					dedupStream(model.Metric{"__name__": "cpu", "backend": "other"}, 2),
				},
			},
			want: model.Matrix{
				dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1),
				dedupStream(model.Metric{"__name__": "cpu", "backend": "other"}, 2),
			},
		},
		{
			name:   "no backends at all",
			ignore: ignore,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sets := make([]ordinalSeriesSet, len(tc.inputs))
			for i, mat := range tc.inputs {
				sets[i] = matrixSet(i, mat)
			}

			stats := &promhttputil.DedupStats{}
			got, err := SeriesSetToMatrix(dedupSeriesSets(sets, tc.ignore, stats))
			if err != nil {
				t.Fatalf("SeriesSetToMatrix: %v", err)
			}
			if got.String() != tc.want.String() {
				t.Errorf("merged matrix:\n got: %v\nwant: %v", got, tc.want)
			}

			wantCollisions := 0
			for _, c := range tc.wantPairs {
				wantCollisions += c
			}
			if stats.Collisions != wantCollisions {
				t.Errorf("Collisions = %d, want %d", stats.Collisions, wantCollisions)
			}
			if len(stats.Pairs) != len(tc.wantPairs) {
				t.Fatalf("Pairs = %v, want %v", stats.Pairs, tc.wantPairs)
			}
			for pair, want := range tc.wantPairs {
				if stats.Pairs[pair] != want {
					t.Errorf("Pairs[%v] = %d, want %d", pair, stats.Pairs[pair], want)
				}
			}
		})
	}
}

// A backend that fails while being drained fails the whole merge, as the
// model.Matrix round trip did (it surfaced ss.Err() from the drain).
func TestDedupSeriesSets_SetError(t *testing.T) {
	boom := errors.New("backend exploded")
	sets := []ordinalSeriesSet{
		matrixSet(0, model.Matrix{dedupStream(model.Metric{"__name__": "cpu", "backend": "sg0"}, 1)}),
		{ordinal: 1, ss: storage.ErrSeriesSet(boom)},
	}

	ss := dedupSeriesSets(sets, []string{"backend"}, &promhttputil.DedupStats{})
	for ss.Next() {
	}
	if !errors.Is(ss.Err(), boom) {
		t.Fatalf("Err() = %v, want %v", ss.Err(), boom)
	}
}

// Native histograms are never re-encoded: the winning series' samples -- float,
// histogram, or both on one series -- come out exactly as they went in, in the
// same order.
func TestDedupSeriesSets_NativeHistogramsPassThrough(t *testing.T) {
	hist := func(sum float64) *histogram.FloatHistogram {
		return &histogram.FloatHistogram{Schema: histogram.CustomBucketsSchema, Count: 4, Sum: sum}
	}
	winner := promapi.NewSeries(
		labels.FromStrings("__name__", "latency", "backend", "sg0"),
		[]chunks.Sample{
			promapi.FloatSample(1, 1.5),
			promapi.HistogramSample(2, hist(10)),
			promapi.HistogramSample(3, hist(11)),
		},
	)
	loser := promapi.NewSeries(
		labels.FromStrings("__name__", "latency", "backend", "sg1"),
		[]chunks.Sample{promapi.HistogramSample(2, hist(20))},
	)

	stats := &promhttputil.DedupStats{}
	ss := dedupSeriesSets([]ordinalSeriesSet{
		{ordinal: 0, ss: promapi.NewSeriesSet([]storage.Series{winner}, nil, nil)},
		{ordinal: 1, ss: promapi.NewSeriesSet([]storage.Series{loser}, nil, nil)},
	}, []string{"backend"}, stats)

	if !ss.Next() {
		t.Fatal("expected one merged series")
	}
	got := ss.At()
	if ss.Next() {
		t.Fatalf("expected the collided pair to collapse, got a second series %v", ss.At().Labels())
	}
	if stats.Collisions != 1 {
		t.Fatalf("Collisions = %d, want 1", stats.Collisions)
	}
	if got.Labels().Get("backend") != "sg0" {
		t.Fatalf("winner backend = %q, want sg0", got.Labels().Get("backend"))
	}

	type point struct {
		t   int64
		f   float64
		sum float64
		typ chunkenc.ValueType
	}
	var points []point
	it := got.Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		switch vt {
		case chunkenc.ValFloat:
			ts, v := it.At()
			points = append(points, point{t: ts, f: v, typ: vt})
		default:
			ts, fh := it.AtFloatHistogram(nil)
			points = append(points, point{t: ts, sum: fh.Sum, typ: chunkenc.ValFloatHistogram})
		}
	}
	want := []point{
		{t: 1, f: 1.5, typ: chunkenc.ValFloat},
		{t: 2, sum: 10, typ: chunkenc.ValFloatHistogram},
		{t: 3, sum: 11, typ: chunkenc.ValFloatHistogram},
	}
	if fmt.Sprint(points) != fmt.Sprint(want) {
		t.Fatalf("winner samples = %v, want %v", points, want)
	}
}

// mergeViaModelValue is the pre-perf/native-seriesset-dedup query path: drain
// every backend into a model.Matrix, merge with the model.Value core, convert
// back. Kept here as the reference implementation the native merge is checked
// and benchmarked against.
func mergeViaModelValue(sets []ordinalSeriesSet, ignore map[model.LabelName]struct{}) (storage.SeriesSet, *promhttputil.DedupStats) {
	inputs := make([]promhttputil.OrdinalValue, len(sets))
	for i, s := range sets {
		matrix, err := SeriesSetToMatrix(s.ss)
		if err != nil {
			return ModelValueToSeriesSet(nil, nil, err), nil
		}
		inputs[i] = promhttputil.OrdinalValue{Ordinal: s.ordinal, Value: matrix}
	}
	merged, stats, err := promhttputil.MergeValuesDeterministic(inputs, ignore)
	if err != nil {
		return ModelValueToSeriesSet(nil, nil, err), nil
	}
	return ModelValueToSeriesSet(merged, nil, nil), stats
}

// randomBackends builds nGroups backends of nSeries series each, drawn from a
// shared pool of `shared` logical series (so groups overlap modulo the
// "backend" label) plus group-private ones, with randomized label sets, sample
// counts and values.
func randomBackends(rnd *rand.Rand, nGroups, nSeries, shared int) [][]storage.Series {
	out := make([][]storage.Series, nGroups)
	for g := 0; g < nGroups; g++ {
		series := make([]storage.Series, 0, nSeries)
		for i := 0; i < nSeries; i++ {
			name := fmt.Sprintf("private_%d_%d", g, i)
			if i < shared {
				name = fmt.Sprintf("shared_%d", rnd.Intn(shared))
			}
			lbls := labels.FromStrings(
				"__name__", name,
				"backend", fmt.Sprintf("sg%d", g),
				"instance", fmt.Sprintf("host%d", rnd.Intn(3)),
				"job", "node",
			)
			samples := make([]chunks.Sample, rnd.Intn(4)+1)
			for j := range samples {
				samples[j] = promapi.FloatSample(int64(j)*1000, rnd.Float64())
			}
			series = append(series, promapi.NewSeries(lbls, samples))
		}
		out[g] = series
	}
	return out
}

func setsOf(backends [][]storage.Series) []ordinalSeriesSet {
	sets := make([]ordinalSeriesSet, len(backends))
	for i, series := range backends {
		sets[i] = ordinalSeriesSet{ordinal: i, ss: promapi.NewSeriesSet(series, nil, nil)}
	}
	return sets
}

// TestDedupSeriesSets_MatchesModelValueMerge is the equivalence check: on
// randomized inputs (fixed seed), the native merge must return the same series,
// in the same order, with the same samples and the same collision stats as the
// model.Value round trip it replaces.
func TestDedupSeriesSets_MatchesModelValueMerge(t *testing.T) {
	ignoreNames := []string{"backend"}
	ignoreSet := map[model.LabelName]struct{}{"backend": {}}

	rnd := rand.New(rand.NewSource(1))
	for _, nGroups := range []int{1, 2, 3, 5} {
		for _, shared := range []int{0, 5, 20} {
			name := fmt.Sprintf("groups=%d/shared=%d", nGroups, shared)
			backends := randomBackends(rnd, nGroups, 20, shared)

			t.Run(name, func(t *testing.T) {
				wantSS, wantStats := mergeViaModelValue(setsOf(backends), ignoreSet)
				want, err := SeriesSetToMatrix(wantSS)
				if err != nil {
					t.Fatalf("reference merge: %v", err)
				}
				// Guard against a vacuous case: overlapping groups must
				// actually collide, or the comparison proves nothing.
				if shared > 0 && nGroups > 1 && wantStats.Collisions == 0 {
					t.Fatalf("generated input produced no collisions to compare")
				}

				stats := &promhttputil.DedupStats{}
				got, err := SeriesSetToMatrix(dedupSeriesSets(setsOf(backends), ignoreNames, stats))
				if err != nil {
					t.Fatalf("native merge: %v", err)
				}

				if got.String() != want.String() {
					t.Errorf("merged matrix differs\n got: %v\nwant: %v", got, want)
				}
				if stats.Collisions != wantStats.Collisions {
					t.Errorf("Collisions = %d, want %d", stats.Collisions, wantStats.Collisions)
				}
				if fmt.Sprint(stats.Pairs) != fmt.Sprint(wantStats.Pairs) {
					t.Errorf("Pairs = %v, want %v", stats.Pairs, wantStats.Pairs)
				}
			})
		}
	}
}

// BenchmarkCrossGroupDedup compares the native merge with the model.Value round
// trip it replaced, on the same inputs:
//
//	go test -bench=BenchmarkCrossGroupDedup -benchmem -run=^$ ./pkg/promclient/
func BenchmarkCrossGroupDedup(b *testing.B) {
	const (
		nGroups  = 3
		nSeries  = 2000
		nSamples = 60
	)
	ignoreNames := []string{"backend"}
	ignoreSet := map[model.LabelName]struct{}{"backend": {}}

	for _, overlapPct := range []int{5, 50} {
		backends := make([][]storage.Series, nGroups)
		shared := nSeries * overlapPct / 100
		for g := range backends {
			series := make([]storage.Series, nSeries)
			for i := range series {
				name := fmt.Sprintf("private_%d_%d", g, i)
				if i < shared {
					name = fmt.Sprintf("shared_%d", i)
				}
				samples := make([]chunks.Sample, nSamples)
				for j := range samples {
					samples[j] = promapi.FloatSample(int64(j)*1000, float64(j))
				}
				series[i] = promapi.NewSeries(labels.FromStrings(
					"__name__", name,
					"backend", fmt.Sprintf("sg%d", g),
					"instance", "host1",
					"job", "node",
				), samples)
			}
			backends[g] = series
		}

		b.Run(fmt.Sprintf("overlap=%dpct/path=model_value", overlapPct), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ss, _ := mergeViaModelValue(setsOf(backends), ignoreSet)
				drainSeriesSet(b, ss)
			}
		})

		b.Run(fmt.Sprintf("overlap=%dpct/path=native", overlapPct), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ss := dedupSeriesSets(setsOf(backends), ignoreNames, &promhttputil.DedupStats{})
				drainSeriesSet(b, ss)
			}
		})
	}
}

// drainSeriesSet reads every sample of ss, so a benchmark accounts for the work
// a real consumer (the promql engine) does on the merged result.
func drainSeriesSet(b *testing.B, ss storage.SeriesSet) {
	b.Helper()
	for ss.Next() {
		it := ss.At().Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			it.At()
		}
	}
	if err := ss.Err(); err != nil {
		b.Fatalf("merged SeriesSet: %v", err)
	}
}
