package promhttputil

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
)

// TestMergeValuesDeterministic_VectorByteStability runs the merge 1000 times on
// the same two Vector inputs and asserts the result is byte-identical every
// iteration. This proves no map-iteration nondeterminism leaks.
func TestMergeValuesDeterministic_VectorByteStability(t *testing.T) {
	ignore := ignoreSet("backend")

	a := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "thanos"}, Value: 42, Timestamp: 100},
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "thanos"}, Value: 10, Timestamp: 100},
		{Metric: model.Metric{"__name__": "disk", "instance": "x", "backend": "thanos"}, Value: 7, Timestamp: 100},
	}
	b := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "vm"}, Value: 99, Timestamp: 100},
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "vm"}, Value: 20, Timestamp: 100},
		{Metric: model.Metric{"__name__": "net", "instance": "x", "backend": "vm"}, Value: 3, Timestamp: 100},
	}

	v0, _, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("iteration 0: %v", err)
	}
	ref, err := json.Marshal(v0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < 1000; i++ {
		vi, _, err := merge2(a, b, ignore, 0, 1)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		got, err := json.Marshal(vi)
		if err != nil {
			t.Fatalf("iteration %d: json.Marshal: %v", i, err)
		}
		if string(got) != string(ref) {
			t.Fatalf("iteration %d: result differs from iteration 0\ngot:  %s\nwant: %s", i, got, ref)
		}
	}
}

// TestMergeValuesDeterministic_MatrixByteStability runs the merge 1000 times on
// the same two Matrix inputs and asserts the result is byte-identical every
// iteration.
func TestMergeValuesDeterministic_MatrixByteStability(t *testing.T) {
	ignore := ignoreSet("backend")

	a := model.Matrix{
		{
			Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "thanos"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 10}, {Timestamp: 2, Value: 11}},
		},
		{
			Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "thanos"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 5}},
		},
		{
			Metric: model.Metric{"__name__": "disk", "instance": "y", "backend": "thanos"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 9}},
		},
	}
	b := model.Matrix{
		{
			Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "vm"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 99}, {Timestamp: 2, Value: 100}},
		},
		{
			Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "vm"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 50}},
		},
		{
			Metric: model.Metric{"__name__": "net", "instance": "z", "backend": "vm"},
			Values: []model.SamplePair{{Timestamp: 1, Value: 1}},
		},
	}

	v0, _, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("iteration 0: %v", err)
	}
	ref, err := json.Marshal(v0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < 1000; i++ {
		vi, _, err := merge2(a, b, ignore, 0, 1)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		got, err := json.Marshal(vi)
		if err != nil {
			t.Fatalf("iteration %d: json.Marshal: %v", i, err)
		}
		if string(got) != string(ref) {
			t.Fatalf("iteration %d: result differs from iteration 0\ngot:  %s\nwant: %s", i, got, ref)
		}
	}
}

// TestMergeValuesDeterministic_ThreeBackendLowestOrdinalWins verifies that with
// three backends — two of which (0 and 2) carry the same series modulo the
// ignored label — the lowest ordinal wins and the collision is attributed to the
// exact {winner, loser} pair {0, 2}.
func TestMergeValuesDeterministic_ThreeBackendLowestOrdinalWins(t *testing.T) {
	ignore := ignoreSet("backend")

	src0 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 1, Timestamp: 100},
	}
	src1 := model.Vector{
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "sg1"}, Value: 2, Timestamp: 100},
	}
	src2 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg2"}, Value: 3, Timestamp: 100},
	}

	final, stats, err := MergeValuesDeterministic(
		ordinalValues([]model.Value{src0, src1, src2}, []int{0, 1, 2}),
		ignore,
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}
	if stats.Pairs[[2]int{0, 2}] != 1 {
		t.Fatalf("expected collision attributed to {0,2}=1, got %v", stats.Pairs)
	}

	vec, ok := final.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", final)
	}
	// Expect: cpu (sg0 wins), mem (sg1, unique) = 2 series.
	if len(vec) != 2 {
		t.Fatalf("expected 2 series, got %d: %v", len(vec), vec)
	}

	var cpuSample *model.Sample
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" {
			cpuSample = s
		}
	}
	if cpuSample == nil {
		t.Fatal("cpu series not found in final result")
	}
	if cpuSample.Metric["backend"] != "sg0" {
		t.Fatalf("expected lowest-ordinal winner 'sg0', got %q", cpuSample.Metric["backend"])
	}
	if cpuSample.Value != 1 {
		t.Fatalf("expected value 1 from sg0, got %v", cpuSample.Value)
	}
}

// TestMergeValuesDeterministic_OrderIndependent verifies that feeding the inputs
// in a permuted order (with their true ordinals) produces the same winner and
// the same collision attribution as ascending order — correctness does not
// depend on arrival order.
func TestMergeValuesDeterministic_OrderIndependent(t *testing.T) {
	ignore := ignoreSet("backend")

	src0 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 1, Timestamp: 100},
	}
	src1 := model.Vector{
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "sg1"}, Value: 2, Timestamp: 100},
	}
	src2 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg2"}, Value: 3, Timestamp: 100},
	}

	// Permuted input order [2, 0, 1] with matching ordinals.
	final, stats, err := MergeValuesDeterministic(
		ordinalValues([]model.Value{src2, src0, src1}, []int{2, 0, 1}),
		ignore,
	)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if stats.Collisions != 1 || stats.Pairs[[2]int{0, 2}] != 1 {
		t.Fatalf("permuted order: expected collision {0,2}=1, got collisions=%d pairs=%v", stats.Collisions, stats.Pairs)
	}

	vec := final.(model.Vector)
	if len(vec) != 2 {
		t.Fatalf("expected 2 series, got %d: %v", len(vec), vec)
	}

	var cpuSample *model.Sample
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" {
			cpuSample = s
		}
	}
	if cpuSample == nil {
		t.Fatal("cpu series not found in final result (permuted order)")
	}
	if cpuSample.Metric["backend"] != "sg0" {
		t.Fatalf("permuted order: expected winner 'sg0', got %q", cpuSample.Metric["backend"])
	}
	if cpuSample.Value != 1 {
		t.Fatalf("permuted order: expected value 1 from sg0, got %v", cpuSample.Value)
	}
}

// TestMergeValuesDeterministic_CollisionCounting verifies Collisions counting
// semantics: one overlapping series → 1, five → 5, zero → 0, and three backends
// all sharing one series → 2 (each loser collides with the single sg0 winner).
func TestMergeValuesDeterministic_CollisionCounting(t *testing.T) {
	ignore := ignoreSet("backend")

	t.Run("one_overlap", func(t *testing.T) {
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 2},
		}
		_, stats, err := merge2(a, b, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 1 {
			t.Fatalf("expected 1 collision, got %d", stats.Collisions)
		}
	})

	t.Run("five_overlaps", func(t *testing.T) {
		a := make(model.Vector, 5)
		b := make(model.Vector, 5)
		for i := range a {
			name := model.LabelValue('a' + rune(i))
			a[i] = &model.Sample{Metric: model.Metric{"__name__": name, "backend": "sg0"}, Value: model.SampleValue(i)}
			b[i] = &model.Sample{Metric: model.Metric{"__name__": name, "backend": "sg1"}, Value: model.SampleValue(i + 100)}
		}
		_, stats, err := merge2(a, b, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 5 {
			t.Fatalf("expected 5 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("zero_overlap", func(t *testing.T) {
		a := model.Vector{
			{Metric: model.Metric{"__name__": "alpha", "backend": "sg0"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "beta", "backend": "sg1"}, Value: 2},
		}
		_, stats, err := merge2(a, b, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	// Three backends all carry the same series. sg0 wins; sg1 and sg2 each lose
	// to it, so the counter records two collisions: {0,1} and {0,2}.
	t.Run("three_backends_share_one_series", func(t *testing.T) {
		series := func(backend string, val model.SampleValue) model.Vector {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": model.LabelValue(backend)}, Value: val},
			}
		}
		final, stats, err := MergeValuesDeterministic(
			ordinalValues([]model.Value{series("sg0", 1), series("sg1", 2), series("sg2", 3)}, []int{0, 1, 2}),
			ignore,
		)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 2 {
			t.Fatalf("expected 2 collisions, got %d", stats.Collisions)
		}
		if stats.Pairs[[2]int{0, 1}] != 1 || stats.Pairs[[2]int{0, 2}] != 1 {
			t.Fatalf("expected collisions {0,1}=1 and {0,2}=1, got %v", stats.Pairs)
		}
		if vec := final.(model.Vector); len(vec) != 1 || vec[0].Metric["backend"] != "sg0" {
			t.Fatalf("expected single sg0 winner, got %v", final)
		}
	})
}

// TestMergeValuesDeterministic_NilInputNoCollisions verifies that passing nil as
// either argument always returns 0 collisions, for both Vector and Matrix types.
func TestMergeValuesDeterministic_NilInputNoCollisions(t *testing.T) {
	ignore := ignoreSet("backend")

	t.Run("nil_a_vector", func(t *testing.T) {
		b := model.Vector{{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 1}}
		_, stats, err := merge2(nil, b, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_b_vector", func(t *testing.T) {
		a := model.Vector{{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1}}
		_, stats, err := merge2(a, nil, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_a_matrix", func(t *testing.T) {
		b := model.Matrix{{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Values: []model.SamplePair{{Timestamp: 1, Value: 1}}}}
		_, stats, err := merge2(nil, b, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_b_matrix", func(t *testing.T) {
		a := model.Matrix{{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Values: []model.SamplePair{{Timestamp: 1, Value: 1}}}}
		_, stats, err := merge2(a, nil, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("both_nil", func(t *testing.T) {
		_, stats, err := merge2(nil, nil, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})
}

// TestMergeValuesDeterministic_EmptyIgnoreLabels verifies that with
// IgnoreLabels={} the reduced fingerprint equals the full fingerprint: two
// series with the same full FP collapse (exact-FP duplicate, no collision); two
// with different full FPs pass through independently.
func TestMergeValuesDeterministic_EmptyIgnoreLabels(t *testing.T) {
	empty := map[model.LabelName]struct{}{}

	t.Run("same_full_fp_collapses", func(t *testing.T) {
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 99},
		}
		v, stats, err := merge2(a, b, empty, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		vec := v.(model.Vector)
		// Identical series are within-group duplicates, not cross-group
		// collisions: deduped to one, zero collisions.
		if len(vec) != 1 {
			t.Fatalf("expected 1 series (deduped), got %d", len(vec))
		}
		if stats.Collisions != 0 {
			t.Fatalf("exact-FP duplicate: expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("different_full_fp_no_collision", func(t *testing.T) {
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "mem"}, Value: 2},
		}
		v, stats, err := merge2(a, b, empty, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(v.(model.Vector)) != 2 {
			t.Fatalf("expected 2 series, got %d", len(v.(model.Vector)))
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})
}

// TestMergeValuesDeterministic_IgnoreLabelMissingOnOneSource verifies that when
// IgnoreLabels contains a key present on source 0 but absent from source 1, the
// reduced fingerprints still match and lower-ordinal source 0 wins. This is an
// unusual but possible case (mismatched label injection across backends).
func TestMergeValuesDeterministic_IgnoreLabelMissingOnOneSource(t *testing.T) {
	ignore := ignoreSet("backend")

	// Source 0: has backend label "thanos".
	a := model.Vector{
		{Metric: model.Metric{"__name__": "up", "instance": "x", "backend": "thanos"}, Value: 1, Timestamp: 100},
	}
	// Source 1: no backend label at all.
	b := model.Vector{
		{Metric: model.Metric{"__name__": "up", "instance": "x"}, Value: 0, Timestamp: 100},
	}

	v, stats, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vec := v.(model.Vector)

	// After stripping "backend", both reduce to {__name__="up", instance="x"}.
	// They share the same reduced FP → collision. Ordinal 0 wins.
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision (mismatched label injection), got %d", stats.Collisions)
	}
	if len(vec) != 1 {
		t.Fatalf("expected 1 series (deduped), got %d: %v", len(vec), vec)
	}
	if vec[0].Value != 1 {
		t.Fatalf("expected source 0 value 1, got %v", vec[0].Value)
	}
	if vec[0].Metric["backend"] != "thanos" {
		t.Fatalf("expected winner metric to retain 'backend=thanos', got %q", vec[0].Metric["backend"])
	}
}

// TestMergeValuesDeterministic_DuplicateReducedFPWithinSource pins the behavior
// when a single input Vector contains two samples with the same reduced
// fingerprint (which can't happen from a real Prometheus, but the implementation
// must not panic or produce incorrect output). The first sample seen for a
// reduced FP keeps the slot.
func TestMergeValuesDeterministic_DuplicateReducedFPWithinSource(t *testing.T) {
	ignore := ignoreSet("backend")

	t.Run("same_full_fp_exact_dup", func(t *testing.T) {
		// Two identical samples in 'a': the second is an exact-FP duplicate.
		// Collision counter stays 0; result has 1 series.
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1},
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 2},
		}
		v, stats, err := merge2(a, model.Vector{}, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("exact full-FP dup in a: expected 0 collisions, got %d", stats.Collisions)
		}
		if len(v.(model.Vector)) != 1 {
			t.Fatalf("exact full-FP dup in a: expected 1 series, got %d", len(v.(model.Vector)))
		}
	})

	t.Run("different_full_fp_same_reduced_fp_in_a", func(t *testing.T) {
		// Two samples in 'a' that differ only in the ignored "backend" label map
		// to the same reduced FP. The second collides with the first; both share
		// the same ordinal, so the first keeps the slot.
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 10},
			{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0b"}, Value: 20},
		}
		v, stats, err := merge2(a, model.Vector{}, ignore, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 1 {
			t.Fatalf("reduced-FP collision within a: expected 1 collision, got %d", stats.Collisions)
		}
		vec := v.(model.Vector)
		if len(vec) != 1 {
			t.Fatalf("expected 1 series after intra-source dedup, got %d", len(vec))
		}
		if vec[0].Value != 10 {
			t.Fatalf("expected first sample value 10 to be retained, got %v", vec[0].Value)
		}
	})
}

// TestMergeValuesDeterministic_ConcurrentSafety runs the byte-stability scenario
// from 8 goroutines × 100 iterations concurrently. Each goroutine verifies
// byte-stability against a shared reference. Any race or shared-state corruption
// is caught by the race detector or diverging results.
func TestMergeValuesDeterministic_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	ignore := ignoreSet("backend")

	a := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "thanos"}, Value: 42, Timestamp: 100},
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "thanos"}, Value: 10, Timestamp: 100},
		{Metric: model.Metric{"__name__": "disk", "instance": "x", "backend": "thanos"}, Value: 7, Timestamp: 100},
	}
	b := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "vm"}, Value: 99, Timestamp: 100},
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "vm"}, Value: 20, Timestamp: 100},
		{Metric: model.Metric{"__name__": "net", "instance": "x", "backend": "vm"}, Value: 3, Timestamp: 100},
	}

	// Build canonical reference once outside the goroutines.
	ref0, _, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("reference merge: %v", err)
	}
	refJSON, err := json.Marshal(ref0)
	if err != nil {
		t.Fatalf("reference json.Marshal: %v", err)
	}

	const goroutines = 8
	const itersEach = 100

	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < itersEach; i++ {
				v, _, mergeErr := merge2(a, b, ignore, 0, 1)
				if mergeErr != nil {
					errs[g] = mergeErr
					return
				}
				got, marshalErr := json.Marshal(v)
				if marshalErr != nil {
					errs[g] = marshalErr
					return
				}
				if string(got) != string(refJSON) {
					errs[g] = fmt.Errorf("goroutine %d iter %d: result differs from reference\ngot:  %s\nwant: %s", g, i, got, refJSON)
					return
				}
			}
		}()
	}
	wg.Wait()

	for g, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: %v", g, e)
		}
	}
}
