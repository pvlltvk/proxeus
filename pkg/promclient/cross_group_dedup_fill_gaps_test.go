package promclient

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

func floats(pairs ...[2]int64) []chunks.Sample {
	out := make([]chunks.Sample, len(pairs))
	for i, p := range pairs {
		out[i] = promapi.FloatSample(p[0], float64(p[1]))
	}
	return out
}

func drainFloats(t *testing.T, s storage.Series) [][2]int64 {
	t.Helper()
	var out [][2]int64
	it := s.Iterator(nil)
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		if vt != chunkenc.ValFloat {
			t.Fatalf("unexpected value type %v", vt)
		}
		ts, v := it.At()
		out = append(out, [2]int64{ts, int64(v)})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

func TestDedupSeriesSetsFillGaps_FlagOffIsUnchanged(t *testing.T) {
	winner := series(floats([2]int64{0, 1}, [2]int64{40, 4}), "__name__", "cpu", "backend", "sg0")
	loser := series(floats([2]int64{0, 100}, [2]int64{10, 101}, [2]int64{20, 102}, [2]int64{30, 103}, [2]int64{40, 104}), "__name__", "cpu", "backend", "sg1")

	build := func() []ordinalSeriesSet {
		return []ordinalSeriesSet{setOf(0, winner), setOf(1, loser)}
	}

	statsOld := &promhttputil.DedupStats{}
	ssOld := dedupSeriesSets(build(), []string{"backend"}, statsOld)

	statsNew := &promhttputil.DedupStats{}
	ssNew := dedupSeriesSetsFillGaps(build(), []string{"backend"}, dedupGapOpts{}, statsNew)

	if !ssOld.Next() || !ssNew.Next() {
		t.Fatal("expected one surviving series from both paths")
	}
	oldGot := drainFloats(t, ssOld.At())
	newGot := drainFloats(t, ssNew.At())
	if fmt.Sprint(oldGot) != fmt.Sprint(newGot) {
		t.Fatalf("dedupSeriesSetsFillGaps (flag off) = %v, want dedupSeriesSets's %v", newGot, oldGot)
	}
	if fmt.Sprint(oldGot) != "[[0 1] [40 4]]" {
		t.Fatalf("got %v, want the winner's own samples untouched (no loser read)", oldGot)
	}
}

func TestDedupSeriesSetsFillGaps_FillsFromLoser(t *testing.T) {
	base := series(floats([2]int64{0, 1}, [2]int64{40, 4}), "__name__", "cpu", "backend", "sg0")
	filler := series(floats([2]int64{10, 101}, [2]int64{20, 102}, [2]int64{30, 103}), "__name__", "cpu", "backend", "sg1")

	sets := []ordinalSeriesSet{setOf(0, base), setOf(1, filler)}
	stats := &promhttputil.DedupStats{}
	ss := dedupSeriesSetsFillGaps(sets, []string{"backend"}, dedupGapOpts{fillGaps: true, gap: 15}, stats)

	if !ss.Next() {
		t.Fatal("expected one merged series")
	}
	got := ss.At()
	if ss.Next() {
		t.Fatal("expected the bucket to collapse to one series")
	}
	if got.Labels().Get("backend") != "sg0" {
		t.Fatalf("winner labels backend = %q, want sg0", got.Labels().Get("backend"))
	}
	want := "[[0 1] [10 101] [20 102] [30 103] [40 4]]"
	if fmt.Sprint(drainFloats(t, got)) != want {
		t.Fatalf("got %v, want %s", drainFloats(t, got), want)
	}
	if stats.Collisions != 1 {
		t.Fatalf("Collisions = %d, want 1", stats.Collisions)
	}
}

func TestDedupSeriesSetsFillGaps_OrdinalOrderIndependence(t *testing.T) {
	cpu := func(backend string, samples []chunks.Sample) storage.Series {
		return series(samples, "__name__", "cpu", "backend", backend)
	}
	bySG := map[string][]chunks.Sample{
		"sg0": floats([2]int64{0, 1}, [2]int64{40, 4}),
		"sg1": floats([2]int64{10, 101}),
		"sg2": floats([2]int64{20, 102}, [2]int64{30, 103}),
	}

	permutations := [][3]int{
		{0, 1, 2}, {0, 2, 1}, {1, 0, 2},
		{1, 2, 0}, {2, 0, 1}, {2, 1, 0},
	}

	for _, perm := range permutations {
		t.Run(fmt.Sprintf("%d%d%d", perm[0], perm[1], perm[2]), func(t *testing.T) {
			sets := make([]ordinalSeriesSet, 0, len(perm))
			for _, ordinal := range perm {
				sg := fmt.Sprintf("sg%d", ordinal)
				sets = append(sets, setOf(ordinal, cpu(sg, bySG[sg])))
			}
			stats := &promhttputil.DedupStats{}
			ss := dedupSeriesSetsFillGaps(sets, []string{"backend"}, dedupGapOpts{fillGaps: true, gap: 8}, stats)

			if !ss.Next() {
				t.Fatal("expected one surviving series")
			}
			got := ss.At()
			if ss.Next() {
				t.Fatal("expected all three to collide into one")
			}
			if got.Labels().Get("backend") != "sg0" {
				t.Fatalf("winner backend = %q, want sg0", got.Labels().Get("backend"))
			}
			want := "[[0 1] [10 101] [20 102] [30 103] [40 4]]"
			if fmt.Sprint(drainFloats(t, got)) != want {
				t.Fatalf("perm %v: got %v, want %s", perm, drainFloats(t, got), want)
			}
		})
	}
}

// Auto gap threshold (Gap: 0) is exercised end-to-end here rather than at the
// promhttputil level: it needs enough samples in the base to estimate an
// interval from.
func TestDedupSeriesSetsFillGaps_AutoThreshold(t *testing.T) {
	// Base scrapes every 10 units; one scrape at 20 is missing.
	base := series(floats([2]int64{0, 1}, [2]int64{10, 1}, [2]int64{30, 1}, [2]int64{40, 1}, [2]int64{50, 1}), "__name__", "up", "backend", "sg0")
	filler := series(floats([2]int64{20, 9}), "__name__", "up", "backend", "sg1")

	sets := []ordinalSeriesSet{setOf(0, base), setOf(1, filler)}
	ss := dedupSeriesSetsFillGaps(sets, []string{"backend"}, dedupGapOpts{fillGaps: true}, &promhttputil.DedupStats{})

	if !ss.Next() {
		t.Fatal("expected one merged series")
	}
	got := drainFloats(t, ss.At())
	want := "[[0 1] [10 1] [20 9] [30 1] [40 1] [50 1]]"
	if fmt.Sprint(got) != want {
		t.Fatalf("got %v, want %s", got, want)
	}
}
