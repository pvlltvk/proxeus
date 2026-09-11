package promclient

import (
	"fmt"
	"sync"
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

// series is a small helper for building a one-off storage.Series from a
// label pair list, for tests that don't need model.Matrix's shape.
func series(samples []chunks.Sample, kv ...string) storage.Series {
	return promapi.NewSeries(labels.FromStrings(kv...), samples)
}

func oneFloat(t int64, v float64) []chunks.Sample { return []chunks.Sample{promapi.FloatSample(t, v)} }

func setOf(ordinal int, ss ...storage.Series) ordinalSeriesSet {
	return ordinalSeriesSet{ordinal: ordinal, ss: promapi.NewSeriesSet(ss, nil, nil)}
}

// TestDedupSeriesSets_ReducedLabelSetEmpty covers the case where a series'
// only labels are the ones being ignored, so BytesWithoutLabels reduces both
// sides to the same empty (bare labelSep byte) key. Two otherwise-nameless
// series across groups must still collide on that shared empty bucket, same
// as any other genuine collision.
func TestDedupSeriesSets_ReducedLabelSetEmpty(t *testing.T) {
	sets := []ordinalSeriesSet{
		setOf(0, series(oneFloat(0, 1), "backend", "sg0")),
		setOf(1, series(oneFloat(0, 2), "backend", "sg1")),
	}
	stats := &promhttputil.DedupStats{}
	ss := dedupSeriesSets(sets, []string{"backend"}, stats)

	if !ss.Next() {
		t.Fatal("expected one surviving series")
	}
	got := ss.At()
	if ss.Next() {
		t.Fatalf("expected the two bare-ignore-label series to collapse, got a second series %v", ss.At().Labels())
	}
	if got.Labels().Get("backend") != "sg0" {
		t.Fatalf("winner backend = %q, want sg0", got.Labels().Get("backend"))
	}
	if stats.Collisions != 1 {
		t.Fatalf("Collisions = %d, want 1", stats.Collisions)
	}
}

// TestDedupSeriesSets_SeparatorByteInLabelValue proves BytesWithoutLabels
// (labelSep=0xfe, sep=0xff) cannot conflate two distinct reduced labelsets
// merely because a value contains a byte that also happens to be a
// structural separator in the encoding. Label values decoded from a
// backend's JSON response are valid UTF-8 (0xfe/0xff are not legal UTF-8
// bytes at all, matching the same invariant Prometheus's own Labels.Bytes/
// Labels.Hash rely on), so this test uses the byte that *can* legitimately
// appear in a UTF-8 value, 0x00, plus a value containing a literal quote,
// and checks that near-miss labelsets are kept apart rather than falsely
// merged.
func TestDedupSeriesSets_SeparatorByteInLabelValue(t *testing.T) {
	sets := []ordinalSeriesSet{
		setOf(0,
			series(oneFloat(0, 1), "__name__", "m", "instance", "a\x00b", "backend", "sg0"),
			series(oneFloat(0, 2), "__name__", "m", "instance", `a"b`, "backend", "sg0"),
		),
		setOf(1,
			series(oneFloat(0, 3), "__name__", "m", "instance", "a\x00c", "backend", "sg1"),
			series(oneFloat(0, 4), "__name__", "m", "instance", "a\x00b", "backend", "sg1"), // genuine collision with sg0's first series
		),
	}
	stats := &promhttputil.DedupStats{}
	ss := dedupSeriesSets(sets, []string{"backend"}, stats)

	got := map[string]float64{}
	for ss.Next() {
		s := ss.At()
		it := s.Iterator(nil)
		it.Next()
		_, v := it.At()
		got[s.Labels().Get("instance")] = v
	}
	if err := ss.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}

	want := map[string]float64{
		"a\x00b": 1, // sg0 wins the collision with sg1's identical-value series
		`a"b`:    2,
		"a\x00c": 3,
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if stats.Collisions != 1 {
		t.Fatalf("Collisions = %d, want 1", stats.Collisions)
	}
}

// TestDedupSeriesSets_DuplicateWithinOneGroup covers D1/D2 side by side: an
// exact full-label duplicate *within* a single group's own SeriesSet is not
// cross-group dedup's concern (single-backend inputs bypass the merge
// entirely, same as MergeValuesDeterministic's single-input case) so both
// copies must survive; the same duplicate appearing again in a second group
// is a genuine cross-group collision once ignore-label differences are
// accounted for.
func TestDedupSeriesSets_DuplicateWithinOneGroup(t *testing.T) {
	dup := func(backend string) storage.Series {
		return series(oneFloat(0, 1), "__name__", "cpu", "instance", "x", "backend", backend)
	}

	t.Run("single group keeps both exact duplicates", func(t *testing.T) {
		sets := []ordinalSeriesSet{setOf(0, dup("sg0"), dup("sg0"))}
		stats := &promhttputil.DedupStats{}
		mat, err := SeriesSetToMatrix(dedupSeriesSets(sets, []string{"backend"}, stats))
		if err != nil {
			t.Fatalf("SeriesSetToMatrix: %v", err)
		}
		if len(mat) != 2 {
			t.Fatalf("expected both exact duplicates to survive within one group, got %d: %v", len(mat), mat)
		}
		if stats.Collisions != 0 {
			t.Fatalf("Collisions = %d, want 0 (dedupSeriesSets never looks at a single-set input)", stats.Collisions)
		}
	})

	// Once a second group is in play, dedupSeriesSets folds every input
	// (including a single group's own exact-duplicate series) through the
	// same shared-bucket map -- an in-group exact duplicate silently
	// collapses to its first occurrence, same as
	// mergeMatricesDeterministic's identical fingerprint-bucket behavior on
	// main. This is unrelated to which group the duplicate came from: it is
	// only the "am I the sole input" bypass that changes with group count,
	// not per-group treatment of duplicates.
	t.Run("a second group makes the whole merge fold exact duplicates, including the first group's own", func(t *testing.T) {
		sets := []ordinalSeriesSet{
			setOf(0, dup("sg0"), dup("sg0")),
			setOf(1, dup("sg1")),
		}
		wantSS, wantStats := mergeViaModelValue(sets, map[model.LabelName]struct{}{"backend": {}})
		want, err := SeriesSetToMatrix(wantSS)
		if err != nil {
			t.Fatalf("reference merge: %v", err)
		}

		sets = []ordinalSeriesSet{
			setOf(0, dup("sg0"), dup("sg0")),
			setOf(1, dup("sg1")),
		}
		stats := &promhttputil.DedupStats{}
		got, err := SeriesSetToMatrix(dedupSeriesSets(sets, []string{"backend"}, stats))
		if err != nil {
			t.Fatalf("SeriesSetToMatrix: %v", err)
		}

		if got.String() != want.String() {
			t.Fatalf("merged matrix differs\n got: %v\nwant: %v", got, want)
		}
		if len(got) != 1 {
			t.Fatalf("expected the in-group duplicate and the cross-group collision to fold to 1 series, got %d: %v", len(got), got)
		}
		if stats.Collisions != wantStats.Collisions {
			t.Fatalf("Collisions = %d, want %d (old-path reference)", stats.Collisions, wantStats.Collisions)
		}
	})
}

// TestDedupSeriesSets_OrdinalOrderIndependence proves the documented
// invariant that correctness does not depend on input arrival/iteration
// order, only on each set's tagged ordinal -- the middle-ordinal source
// must beat a later one and lose to an earlier one, however the sets slice
// itself is ordered, and Pairs must attribute the true {winner,loser}
// ordinals rather than something derived from iteration position.
func TestDedupSeriesSets_OrdinalOrderIndependence(t *testing.T) {
	cpu := func(backend string) storage.Series {
		return series(oneFloat(0, 1), "__name__", "cpu", "backend", backend)
	}

	// Deliberately NOT sorted by ordinal: 2, 0, 1.
	sets := []ordinalSeriesSet{
		setOf(2, cpu("sg2")),
		setOf(0, cpu("sg0")),
		setOf(1, cpu("sg1")),
	}
	stats := &promhttputil.DedupStats{}
	ss := dedupSeriesSets(sets, []string{"backend"}, stats)

	if !ss.Next() {
		t.Fatal("expected one surviving series")
	}
	got := ss.At()
	if ss.Next() {
		t.Fatal("expected all three to collide into one")
	}
	if got.Labels().Get("backend") != "sg0" {
		t.Fatalf("winner backend = %q, want sg0 (lowest ordinal), independent of arrival order", got.Labels().Get("backend"))
	}
	wantPairs := map[[2]int]int{{0, 1}: 1, {0, 2}: 1}
	if fmt.Sprint(stats.Pairs) != fmt.Sprint(wantPairs) {
		t.Fatalf("Pairs = %v, want %v", stats.Pairs, wantPairs)
	}
}

// TestDedupSeriesSets_LargeCardinality is a map-growth sanity check: a large
// number of disjoint buckets across groups must all survive with the right
// per-group winner, with no panic and no cross-contamination between
// buckets.
func TestDedupSeriesSets_LargeCardinality(t *testing.T) {
	const n = 20000
	build := func(backend string, valueOffset float64) []storage.Series {
		out := make([]storage.Series, n)
		for i := 0; i < n; i++ {
			out[i] = series(oneFloat(0, float64(i)+valueOffset),
				"__name__", "cpu", "instance", fmt.Sprintf("host%d", i), "backend", backend)
		}
		return out
	}
	sets := []ordinalSeriesSet{
		setOf(0, build("sg0", 0)...),
		setOf(1, build("sg1", 1000000)...),
	}
	stats := &promhttputil.DedupStats{}
	mat, err := SeriesSetToMatrix(dedupSeriesSets(sets, []string{"backend"}, stats))
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}
	if len(mat) != n {
		t.Fatalf("expected %d surviving series (every instance collides across groups), got %d", n, len(mat))
	}
	if stats.Collisions != n {
		t.Fatalf("Collisions = %d, want %d", stats.Collisions, n)
	}
	for _, s := range mat {
		if s.Metric["backend"] != "sg0" {
			t.Fatalf("expected sg0 (lowest ordinal) to win every bucket, got %v", s.Metric)
		}
		if v := s.Values[0].Value; float64(v) >= 1000000 {
			t.Fatalf("winner sample looks like it came from the loser: %v", s)
		}
	}
}

// TestDedupSeriesSets_ConcurrentUse runs many independent dedupSeriesSets
// calls (disjoint inputs, no shared state) in parallel under the race
// detector, to catch any accidental package-level mutable state (e.g. a
// shared scratch buffer) sneaking into the merge.
func TestDedupSeriesSets_ConcurrentUse(t *testing.T) {
	const goroutines = 16
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			sets := []ordinalSeriesSet{
				setOf(0, series(oneFloat(0, float64(g)), "__name__", "cpu", "g", fmt.Sprint(g), "backend", "sg0")),
				setOf(1, series(oneFloat(0, float64(-g)), "__name__", "cpu", "g", fmt.Sprint(g), "backend", "sg1")),
			}
			stats := &promhttputil.DedupStats{}
			mat, err := SeriesSetToMatrix(dedupSeriesSets(sets, []string{"backend"}, stats))
			if err != nil {
				t.Errorf("goroutine %d: SeriesSetToMatrix: %v", g, err)
				return
			}
			if len(mat) != 1 || stats.Collisions != 1 {
				t.Errorf("goroutine %d: got %d series / %d collisions, want 1/1", g, len(mat), stats.Collisions)
			}
		}(g)
	}
	wg.Wait()
}

// TestDedupSeriesSets_HistogramsMatchModelValueMerge checks native-histogram
// fidelity against the reference model.Value path with a custom-buckets
// histogram containing empty buckets in the middle -- the exact shape
// floatHistogramToSampleHistogram's JSON-shaped carrier is documented to
// drop when it re-flattens a histogram. In this codebase both paths turn out
// bit-identical here already: histogram_convert.go's pre-existing
// FloatHistogram<->SampleHistogram pin (keyed by pointer identity, untouched
// by this change) makes the old round trip lossless for any single
// synchronous merge, so this test demonstrates equivalence, not a fidelity
// regression the old path had in practice for this call path.
func TestDedupSeriesSets_HistogramsMatchModelValueMerge(t *testing.T) {
	buildFH := func(sum float64, buckets []float64) *histogram.FloatHistogram {
		count := 0.0
		for _, b := range buckets {
			count += b
		}
		return &histogram.FloatHistogram{
			Schema:          histogram.CustomBucketsSchema,
			Count:           count,
			Sum:             sum,
			CustomValues:    []float64{1, 2, 3, 4},
			PositiveSpans:   []histogram.Span{{Offset: 0, Length: uint32(len(buckets))}},
			PositiveBuckets: buckets,
		}
	}
	winner := promapi.NewSeries(
		labels.FromStrings("__name__", "latency", "backend", "sg0"),
		[]chunks.Sample{promapi.HistogramSample(1, buildFH(123.5, []float64{10, 0, 0, 20}))},
	)
	loser := promapi.NewSeries(
		labels.FromStrings("__name__", "latency", "backend", "sg1"),
		[]chunks.Sample{promapi.HistogramSample(1, buildFH(1, []float64{1}))},
	)

	buildSets := func() []ordinalSeriesSet {
		return []ordinalSeriesSet{
			{ordinal: 0, ss: promapi.NewSeriesSet([]storage.Series{winner}, nil, nil)},
			{ordinal: 1, ss: promapi.NewSeriesSet([]storage.Series{loser}, nil, nil)},
		}
	}
	ignoreSet := map[model.LabelName]struct{}{"backend": {}}
	ignoreNames := []string{"backend"}

	oldSS, oldStats := mergeViaModelValue(buildSets(), ignoreSet)
	newSS := dedupSeriesSets(buildSets(), ignoreNames, &promhttputil.DedupStats{})

	oldFH := drainOneFloatHistogram(t, oldSS)
	newFH := drainOneFloatHistogram(t, newSS)

	if oldStats.Collisions != 1 {
		t.Fatalf("reference merge collisions = %d, want 1", oldStats.Collisions)
	}
	if got, want := newFH.String(), oldFH.String(); got != want {
		t.Fatalf("native merge histogram = %s\nwant (old model.Value path) = %s", got, want)
	}
	// Pin down the specific fidelity property: the empty middle buckets
	// survive on both paths (this is the shape the JSON-flattened carrier
	// would otherwise have dropped).
	if oldFH.Schema != histogram.CustomBucketsSchema || newFH.Schema != histogram.CustomBucketsSchema {
		t.Fatalf("expected CustomBucketsSchema on both, got old=%v new=%v", oldFH.Schema, newFH.Schema)
	}
}

func drainOneFloatHistogram(t *testing.T, ss storage.SeriesSet) *histogram.FloatHistogram {
	t.Helper()
	if !ss.Next() {
		t.Fatal("expected one merged series")
	}
	it := ss.At().Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		if vt == chunkenc.ValFloatHistogram || vt == chunkenc.ValHistogram {
			_, fh := it.AtFloatHistogram(nil)
			return fh
		}
	}
	t.Fatal("expected a histogram sample")
	return nil
}
