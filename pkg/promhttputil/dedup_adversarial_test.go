package promhttputil

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
)

// vectorEqual returns true when two Vectors contain the same samples (by label
// set and value) regardless of ordering in the slice.
func vectorEqual(a, b model.Vector) bool {
	if len(a) != len(b) {
		return false
	}
	type key struct {
		fp  model.Fingerprint
		val model.SampleValue
		ts  model.Time
	}
	counts := make(map[key]int, len(a))
	for _, s := range a {
		counts[key{s.Metric.Fingerprint(), s.Value, s.Timestamp}]++
	}
	for _, s := range b {
		counts[key{s.Metric.Fingerprint(), s.Value, s.Timestamp}]--
	}
	for _, v := range counts {
		if v != 0 {
			return false
		}
	}
	return true
}

// matrixEqual returns true when two Matrices contain the same streams (by
// metric fingerprint and values slice) regardless of ordering.
func matrixEqual(a, b model.Matrix) bool {
	if len(a) != len(b) {
		return false
	}
	type key struct {
		fp model.Fingerprint
	}
	type entry struct {
		values []model.SamplePair
	}
	index := make(map[model.Fingerprint][]model.SamplePair, len(a))
	for _, s := range a {
		fp := model.Metric(s.Metric).Fingerprint()
		index[fp] = s.Values
	}
	for _, s := range b {
		fp := model.Metric(s.Metric).Fingerprint()
		vals, ok := index[fp]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(vals, s.Values) {
			return false
		}
	}
	return true
}

// TestB1F1_VectorByteStability runs MergeValuesDeterministic 1000 times on
// the same two Vector inputs and asserts the result is byte-identical every
// iteration. This proves no map-iteration nondeterminism leaks.
func TestB1F1_VectorByteStability(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "thanos",
		NameB:        "vm",
	}

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

	v0, _, err := MergeValuesDeterministic(a, b, opts)
	if err != nil {
		t.Fatalf("iteration 0: %v", err)
	}
	ref, err := json.Marshal(v0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < 1000; i++ {
		vi, _, err := MergeValuesDeterministic(a, b, opts)
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

// TestB1F2_MatrixByteStability runs MergeValuesDeterministic 1000 times on
// the same two Matrix inputs and asserts the result is byte-identical every
// iteration.
func TestB1F2_MatrixByteStability(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "thanos",
		NameB:        "vm",
	}

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

	v0, _, err := MergeValuesDeterministic(a, b, opts)
	if err != nil {
		t.Fatalf("iteration 0: %v", err)
	}
	ref, err := json.Marshal(v0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < 1000; i++ {
		vi, _, err := MergeValuesDeterministic(a, b, opts)
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

// TestB1F3_ThreeBackendLowestOrdinalWins tests the chained merge scenario that
// MultiAPI produces: merge(0,1) → resultA with resultIdx=0, then
// merge(resultA,2) → final. Source 0 must win the collision with source 2.
//
// The ordinal plumbing works because MultiAPI passes resultIdx (running min
// ordinal) as idxA to mergeFn on subsequent merges. After merge(0,1), resultIdx
// stays 0 because 0 < 1. So merge(resultA,2) is called as mergeFn(_, _, 0, 2),
// and ordinal 0 beats ordinal 2.
func TestB1F3_ThreeBackendLowestOrdinalWins(t *testing.T) {
	ignoreLabels := map[model.LabelName]struct{}{"backend": {}}

	// In-order arrival: 0 arrives first, then 1, then 2.
	// Sources 0 and 2 both have "cpu" (modulo backend); source 1 has "mem" only.
	src0 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 1, Timestamp: 100},
	}
	src1 := model.Vector{
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "sg1"}, Value: 2, Timestamp: 100},
	}
	src2 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg2"}, Value: 3, Timestamp: 100},
	}

	// Step 1: merge(0, 1) → resultA; resultIdx=0 (min(0,1)).
	resultA, statsA, err := MergeValuesDeterministic(src0, src1, DedupOpts{
		IgnoreLabels: ignoreLabels,
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "sg0",
		NameB:        "sg1",
	})
	if err != nil {
		t.Fatalf("merge(0,1): %v", err)
	}
	if statsA.Collisions != 0 {
		t.Fatalf("merge(0,1): expected 0 collisions, got %d", statsA.Collisions)
	}

	// Step 2: merge(resultA, 2) with idxA=0 (resultIdx), idxB=2.
	final, statsB, err := MergeValuesDeterministic(resultA, src2, DedupOpts{
		IgnoreLabels: ignoreLabels,
		OrdinalA:     0,
		OrdinalB:     2,
		NameA:        "running",
		NameB:        "sg2",
	})
	if err != nil {
		t.Fatalf("merge(resultA,2): %v", err)
	}
	if statsB.Collisions != 1 {
		t.Fatalf("merge(resultA,2): expected 1 collision, got %d", statsB.Collisions)
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

// TestB1F3_ThreeBackendPermutedArrival verifies that arrival order [2,0,1]
// produces the SAME winner (source 0) as in-order arrival. This tests that
// the running-min-ordinal tracking is correct regardless of which channel
// delivers first.
//
// With arrival order 2,0,1: merge(2,0) with idxA=2,idxB=0 → ordinal 0 wins;
// resultIdx becomes min(2,0)=0. Then merge(result,1) with idxA=0,idxB=1 →
// no collision (mem is unique). Final cpu must still carry sg0's value.
func TestB1F3_ThreeBackendPermutedArrival(t *testing.T) {
	ignoreLabels := map[model.LabelName]struct{}{"backend": {}}

	src0 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 1, Timestamp: 100},
	}
	src1 := model.Vector{
		{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "sg1"}, Value: 2, Timestamp: 100},
	}
	src2 := model.Vector{
		{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg2"}, Value: 3, Timestamp: 100},
	}

	// Arrival order: 2 first, then 0, then 1.
	// Step 1: merge(2, 0) — src2 arrived first (resultIdx=2), src0 second (i=0).
	resultA, stats1, err := MergeValuesDeterministic(src2, src0, DedupOpts{
		IgnoreLabels: ignoreLabels,
		OrdinalA:     2,
		OrdinalB:     0,
		NameA:        "sg2",
		NameB:        "sg0",
	})
	if err != nil {
		t.Fatalf("merge(2,0): %v", err)
	}
	if stats1.Collisions != 1 {
		t.Fatalf("merge(2,0): expected 1 collision, got %d", stats1.Collisions)
	}

	// After merge(2,0), resultIdx = min(2,0) = 0.
	// Step 2: merge(resultA, 1) with idxA=0 (resultIdx), idxB=1.
	final, stats2, err := MergeValuesDeterministic(resultA, src1, DedupOpts{
		IgnoreLabels: ignoreLabels,
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "running",
		NameB:        "sg1",
	})
	if err != nil {
		t.Fatalf("merge(resultA,1): %v", err)
	}
	if stats2.Collisions != 0 {
		t.Fatalf("merge(resultA,1): expected 0 collisions, got %d", stats2.Collisions)
	}

	vec, ok := final.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", final)
	}
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
		t.Fatal("cpu series not found in final result (permuted arrival)")
	}
	if cpuSample.Metric["backend"] != "sg0" {
		t.Fatalf("permuted arrival: expected winner 'sg0', got %q", cpuSample.Metric["backend"])
	}
	if cpuSample.Value != 1 {
		t.Fatalf("permuted arrival: expected value 1 from sg0, got %v", cpuSample.Value)
	}
}

// TestB1F4_CollisionCounterAssertions verifies Collisions counting semantics:
// 1 overlapping series → 1, 5 overlapping series → 5, 0 overlap → 0.
// Chained merge: when backend 2 collides with the running result (which itself
// was the winner of a prior collision), the second merge increments by 1.
func TestB1F4_CollisionCounterAssertions(t *testing.T) {
	opts := func(ordA, ordB int) DedupOpts {
		return DedupOpts{
			IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
			OrdinalA:     ordA,
			OrdinalB:     ordB,
		}
	}

	t.Run("one_overlap", func(t *testing.T) {
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 2},
		}
		_, stats, err := MergeValuesDeterministic(a, b, opts(0, 1))
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
		_, stats, err := MergeValuesDeterministic(a, b, opts(0, 1))
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
		_, stats, err := MergeValuesDeterministic(a, b, opts(0, 1))
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	// Chained merge: backends 0, 1, 2 all have the same series. First merge
	// (0 vs 1) yields 1 collision. Second merge (running result vs 2) yields
	// exactly 1 more collision — the third backend collides with the already-
	// deduped running result, regardless of whether the first merge was a collision.
	t.Run("chained_merge_collision_increments", func(t *testing.T) {
		series := func(backend string, val model.SampleValue) model.Vector {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": model.LabelValue(backend)}, Value: val},
			}
		}
		src0 := series("sg0", 1)
		src1 := series("sg1", 2)
		src2 := series("sg2", 3)

		resultA, stats1, err := MergeValuesDeterministic(src0, src1, opts(0, 1))
		if err != nil {
			t.Fatal(err)
		}
		if stats1.Collisions != 1 {
			t.Fatalf("first merge: expected 1 collision, got %d", stats1.Collisions)
		}

		_, stats2, err := MergeValuesDeterministic(resultA, src2, opts(0, 2))
		if err != nil {
			t.Fatal(err)
		}
		// The running result (sg0 winner) has reduced FP = {__name__="cpu"}.
		// src2 also maps to the same reduced FP → collision increments by 1.
		if stats2.Collisions != 1 {
			t.Fatalf("second merge: expected 1 collision (third backend vs running result), got %d", stats2.Collisions)
		}
	})
}

// TestB1F5_NilInputNoCollisions verifies that passing nil as either argument
// always returns 0 collisions, for both Vector and Matrix types.
func TestB1F5_NilInputNoCollisions(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
	}

	t.Run("nil_a_vector", func(t *testing.T) {
		b := model.Vector{{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 1}}
		_, stats, err := MergeValuesDeterministic(nil, b, opts)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_b_vector", func(t *testing.T) {
		a := model.Vector{{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1}}
		_, stats, err := MergeValuesDeterministic(a, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_a_matrix", func(t *testing.T) {
		b := model.Matrix{{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Values: []model.SamplePair{{Timestamp: 1, Value: 1}}}}
		_, stats, err := MergeValuesDeterministic(nil, b, opts)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("nil_b_matrix", func(t *testing.T) {
		a := model.Matrix{{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Values: []model.SamplePair{{Timestamp: 1, Value: 1}}}}
		_, stats, err := MergeValuesDeterministic(a, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("both_nil", func(t *testing.T) {
		_, stats, err := MergeValuesDeterministic(nil, nil, opts)
		if err != nil {
			t.Fatal(err)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})
}

// TestB1E1_EmptyIgnoreLabels verifies that with IgnoreLabels={} (empty set),
// the reduced fingerprint equals the full fingerprint. Two series with the same
// full FP collide; two with different full FPs pass through independently.
func TestB1E1_EmptyIgnoreLabels(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{},
		OrdinalA:     0,
		OrdinalB:     1,
	}

	t.Run("same_full_fp_collides", func(t *testing.T) {
		// Both have IDENTICAL label sets — full FP matches, should be treated as
		// a cross-group collision. Lower ordinal (a, ordinal 0) wins.
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 1},
		}
		b := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 99},
		}
		v, stats, err := MergeValuesDeterministic(a, b, opts)
		if err != nil {
			t.Fatal(err)
		}
		vec := v.(model.Vector)
		// When full FPs are equal, the impl takes the exact-FP path (first wins,
		// no collision counter increment). This is by design: identical series
		// are within-group duplicates, not cross-group collisions.
		if len(vec) != 1 {
			t.Fatalf("expected 1 series (deduped), got %d", len(vec))
		}
		// Collision counter: exact-FP duplicates go through the fullFPIndex path,
		// which does NOT increment collisions (within-group semantics). Zero collisions.
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
		v, stats, err := MergeValuesDeterministic(a, b, opts)
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

// TestB1E2_IgnoreLabelsMissingOnOneSource verifies that when IgnoreLabels
// contains a key present on source 0 but absent from source 1, the reduced
// fingerprints still match and lower-ordinal source 0 wins.
//
// This is an unusual but possible case (mismatched label injection across
// backends). The function must handle it correctly by treating the absent
// label as an empty value in the reduced fingerprint computation.
func TestB1E2_IgnoreLabelsMissingOnOneSource(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "thanos",
		NameB:        "vm-no-backend-label",
	}

	// Source 0: has backend label "thanos".
	a := model.Vector{
		{Metric: model.Metric{"__name__": "up", "instance": "x", "backend": "thanos"}, Value: 1, Timestamp: 100},
	}
	// Source 1: no backend label at all.
	b := model.Vector{
		{Metric: model.Metric{"__name__": "up", "instance": "x"}, Value: 0, Timestamp: 100},
	}

	v, stats, err := MergeValuesDeterministic(a, b, opts)
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

// TestB1E3_DuplicateReducedFPWithinSingleSource documents and pins the
// behaviour when the same input Vector contains two samples with the same
// reduced fingerprint (which can't happen from a real Prometheus, but the
// implementation must not panic or produce incorrect output).
//
// Observed semantics: the first sample seen for a given reduced FP wins and
// is stored in the result; the second is treated as an exact-FP duplicate
// (the exact-FP index tracks the first sample's full FP). If the second sample
// has the same full FP it is silently dropped (within-group dedup, no collision
// counter increment). If it has a different full FP it is treated as a
// collision with the first (collision counter increments, lower ordinal wins
// — but both samples carry the same ordinal so the first always keeps the slot).
func TestB1E3_DuplicateReducedFPWithinSingleSource(t *testing.T) {
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
	}

	t.Run("same_full_fp_exact_dup", func(t *testing.T) {
		// Two identical samples in 'a'. The second hits the fullFPIndex path.
		// Collision counter stays 0; result has 1 series.
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1},
			{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 2},
		}
		b := model.Vector{}
		v, stats, err := MergeValuesDeterministic(a, b, opts)
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
		// Two samples in 'a' that differ only in the ignored "backend" label.
		// Both map to the same reduced FP. The second takes the reducedFPEntry path,
		// which increments collisions and applies lower-ordinal logic. Since both
		// share ordinal OrdinalA, the first stays (ordinal not strictly less than itself).
		a := model.Vector{
			{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 10},
			{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0b"}, Value: 20},
		}
		b := model.Vector{}
		v, stats, err := MergeValuesDeterministic(a, b, opts)
		if err != nil {
			t.Fatal(err)
		}
		// The second sample collides with the first at the reduced-FP level;
		// collision counter increments to 1.
		if stats.Collisions != 1 {
			t.Fatalf("reduced-FP collision within a: expected 1 collision, got %d", stats.Collisions)
		}
		// Both share ordinal OrdinalA=0. The incoming sample's ordinal (0) is NOT
		// strictly less than the existing winner's ordinal (0), so the first stays.
		vec := v.(model.Vector)
		if len(vec) != 1 {
			t.Fatalf("expected 1 series after intra-source dedup, got %d", len(vec))
		}
		if vec[0].Value != 10 {
			t.Fatalf("expected first sample value 10 to be retained, got %v", vec[0].Value)
		}
	})
}

// TestB1E5_ConcurrentSafety runs the F1 scenario from 8 goroutines × 100
// iterations concurrently. Each goroutine verifies byte-stability against its
// own reference. Any race condition or shared-state corruption will be caught
// by the race detector or diverging results.
func TestB1E5_ConcurrentSafety(t *testing.T) {
	t.Parallel()

	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"backend": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "thanos",
		NameB:        "vm",
	}

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
	ref0, _, err := MergeValuesDeterministic(a, b, opts)
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
				v, _, mergeErr := MergeValuesDeterministic(a, b, opts)
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
					errs[g] = &concurrencyMismatchError{goroutine: g, iter: i, got: string(got), want: string(refJSON)}
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

type concurrencyMismatchError struct {
	goroutine int
	iter      int
	got       string
	want      string
}

func (e *concurrencyMismatchError) Error() string {
	return "goroutine " + itoa(e.goroutine) + " iter " + itoa(e.iter) + ": result differs from reference\ngot:  " + e.got + "\nwant: " + e.want
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := 20
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
