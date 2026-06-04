package promhttputil

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
)

// ignoreSet builds an IgnoreLabels set from the given keys.
func ignoreSet(keys ...model.LabelName) map[model.LabelName]struct{} {
	m := make(map[model.LabelName]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

// merge2 merges exactly two values at the given ordinals — a convenience wrapper
// over the n-way MergeValuesDeterministic for the many two-backend test cases.
func merge2(a, b model.Value, ignore map[model.LabelName]struct{}, ordA, ordB int) (model.Value, *DedupStats, error) {
	return MergeValuesDeterministic([]model.Value{a, b}, []int{ordA, ordB}, ignore)
}

func TestMergeValuesDeterministic_NilInputs(t *testing.T) {
	ignore := ignoreSet("server_group")

	t.Run("both nil", func(t *testing.T) {
		v, stats, err := merge2(nil, nil, ignore, 0, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != nil {
			t.Fatalf("expected nil, got %v", v)
		}
		if stats == nil {
			t.Fatal("stats must not be nil")
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("a nil", func(t *testing.T) {
		vec := model.Vector{{Metric: model.Metric{"__name__": "foo"}, Value: 1, Timestamp: 1}}
		v, stats, err := merge2(nil, vec, ignore, 0, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rv, ok := v.(model.Vector)
		if !ok || len(rv) != 1 {
			t.Fatalf("expected b returned unchanged, got %v", v)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})

	t.Run("b nil", func(t *testing.T) {
		vec := model.Vector{{Metric: model.Metric{"__name__": "foo"}, Value: 1, Timestamp: 1}}
		v, stats, err := merge2(vec, nil, ignore, 0, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		rv, ok := v.(model.Vector)
		if !ok || len(rv) != 1 {
			t.Fatalf("expected a returned unchanged, got %v", v)
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
		}
	})
}

func TestMergeValuesDeterministic_TypeMismatch(t *testing.T) {
	a := model.Vector{{Metric: model.Metric{"__name__": "foo"}, Value: 1, Timestamp: 1}}
	b := model.Matrix{&model.SampleStream{Metric: model.Metric{"__name__": "foo"}}}
	_, _, err := merge2(a, b, ignoreSet("server_group"), 0, 1)
	if err == nil {
		t.Fatal("expected error on type mismatch")
	}
}

func TestMergeValuesDeterministic_Vector(t *testing.T) {
	// series that differ only in "server_group" label are considered collisions.
	// lower ordinal wins.
	ignore := ignoreSet("server_group")

	sA := &model.Sample{
		Metric:    model.Metric{"__name__": "cpu", "server_group": "thanos"},
		Value:     42,
		Timestamp: 100,
	}
	sB := &model.Sample{
		Metric:    model.Metric{"__name__": "cpu", "server_group": "victoria"},
		Value:     99,
		Timestamp: 100,
	}
	// A unique series only in b.
	sOnly := &model.Sample{
		Metric:    model.Metric{"__name__": "mem", "server_group": "victoria"},
		Value:     7,
		Timestamp: 100,
	}

	a := model.Vector{sA}
	b := model.Vector{sB, sOnly}

	v, stats, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	vec, ok := v.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", v)
	}

	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}
	if len(vec) != 2 {
		t.Fatalf("expected 2 series, got %d", len(vec))
	}

	// Find the "cpu" series; it should be sA (ordinal 0 wins).
	var cpuSample *model.Sample
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" {
			cpuSample = s
		}
	}
	if cpuSample == nil {
		t.Fatal("cpu series not found in result")
	}
	if cpuSample.Metric["server_group"] != "thanos" {
		t.Fatalf("expected winner to be 'thanos', got %q", cpuSample.Metric["server_group"])
	}
}

func TestMergeValuesDeterministic_VectorHigherOrdinalWins(t *testing.T) {
	// When ordinalB < ordinalA the b side should win.
	ignore := ignoreSet("server_group")

	sA := &model.Sample{
		Metric: model.Metric{"__name__": "cpu", "server_group": "late"},
		Value:  10,
	}
	sB := &model.Sample{
		Metric: model.Metric{"__name__": "cpu", "server_group": "early"},
		Value:  20,
	}

	v, stats, err := merge2(model.Vector{sA}, model.Vector{sB}, ignore, 1, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	vec := v.(model.Vector)
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}
	if len(vec) != 1 {
		t.Fatalf("expected 1 series, got %d", len(vec))
	}
	if vec[0].Metric["server_group"] != "early" {
		t.Fatalf("expected winner 'early', got %q", vec[0].Metric["server_group"])
	}
}

func TestMergeValuesDeterministic_VectorNoCollision(t *testing.T) {
	ignore := ignoreSet("server_group")

	// Two completely different series (different __name__).
	sA := &model.Sample{Metric: model.Metric{"__name__": "alpha", "server_group": "sg0"}, Value: 1}
	sB := &model.Sample{Metric: model.Metric{"__name__": "beta", "server_group": "sg1"}, Value: 2}

	v, stats, err := merge2(model.Vector{sA}, model.Vector{sB}, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Collisions != 0 {
		t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
	}
	if len(v.(model.Vector)) != 2 {
		t.Fatalf("expected 2 series, got %d", len(v.(model.Vector)))
	}
}

func TestMergeValuesDeterministic_Matrix(t *testing.T) {
	ignore := ignoreSet("server_group")

	streamA := &model.SampleStream{
		Metric: model.Metric{"__name__": "cpu", "server_group": "sg0"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 10}},
	}
	streamB := &model.SampleStream{
		Metric: model.Metric{"__name__": "cpu", "server_group": "sg1"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 20}},
	}
	streamOnly := &model.SampleStream{
		Metric: model.Metric{"__name__": "mem", "server_group": "sg1"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 5}},
	}

	a := model.Matrix{streamA}
	b := model.Matrix{streamB, streamOnly}

	v, stats, err := merge2(a, b, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mat, ok := v.(model.Matrix)
	if !ok {
		t.Fatalf("expected model.Matrix, got %T", v)
	}
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}
	if len(mat) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(mat))
	}

	var cpuStream *model.SampleStream
	for _, s := range mat {
		if s.Metric["__name__"] == "cpu" {
			cpuStream = s
		}
	}
	if cpuStream == nil {
		t.Fatal("cpu stream not found")
	}
	if cpuStream.Metric["server_group"] != "sg0" {
		t.Fatalf("expected winner 'sg0', got %q", cpuStream.Metric["server_group"])
	}
}

func TestMergeValuesDeterministic_MatrixNoCollision(t *testing.T) {
	ignore := ignoreSet("server_group")

	streamA := &model.SampleStream{
		Metric: model.Metric{"__name__": "cpu", "server_group": "sg0"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 10}},
	}
	streamB := &model.SampleStream{
		Metric: model.Metric{"__name__": "mem", "server_group": "sg1"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 5}},
	}

	v, stats, err := merge2(model.Matrix{streamA}, model.Matrix{streamB}, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.Collisions != 0 {
		t.Fatalf("expected 0 collisions, got %d", stats.Collisions)
	}
	if len(v.(model.Matrix)) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(v.(model.Matrix)))
	}
}

// TestMergeValuesDeterministic_EmptyIgnore verifies that an empty (nil or
// zero-length) IgnoreLabels set reduces collision detection to plain exact-FP
// dedup: distinct series pass through, identical series collapse, and no
// cross-group collisions are reported.
func TestMergeValuesDeterministic_EmptyIgnore(t *testing.T) {
	t.Run("nil_ignore_distinct_series_matches_MergeValues", func(t *testing.T) {
		sA := &model.Sample{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 1, Timestamp: 100}
		sB := &model.Sample{Metric: model.Metric{"__name__": "mem", "instance": "x"}, Value: 2, Timestamp: 100}
		a := model.Vector{sA}
		b := model.Vector{sB}

		got, stats, err := merge2(a, b, nil, 0, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want, wantErr := MergeValues(0, a, b, false)
		if wantErr != nil {
			t.Fatalf("MergeValues error: %v", wantErr)
		}

		gotVec := got.(model.Vector)
		wantVec := want.(model.Vector)
		if len(gotVec) != len(wantVec) {
			t.Fatalf("len mismatch: deterministic=%d MergeValues=%d", len(gotVec), len(wantVec))
		}
		if len(gotVec) != 2 {
			t.Fatalf("expected 2 series, got %d", len(gotVec))
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions with nil IgnoreLabels, got %d", stats.Collisions)
		}
	})

	t.Run("empty_map_ignore_exact_fp_duplicate_no_collision", func(t *testing.T) {
		// Identical series (exact-FP match) deduplicate to 1 with 0 collisions —
		// exact-FP duplicates are within-group, not cross-group collisions.
		sA := &model.Sample{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 5, Timestamp: 100}
		sB := &model.Sample{Metric: model.Metric{"__name__": "cpu", "instance": "x"}, Value: 9, Timestamp: 100}
		a := model.Vector{sA}
		b := model.Vector{sB}

		v, stats, err := merge2(a, b, map[model.LabelName]struct{}{}, 0, 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		vec := v.(model.Vector)
		if len(vec) != 1 {
			t.Fatalf("expected 1 series (deduped), got %d", len(vec))
		}
		if stats.Collisions != 0 {
			t.Fatalf("expected 0 collisions for exact-FP duplicate, got %d", stats.Collisions)
		}
	})
}

// BenchmarkMergeValuesDeterministic compares an empty IgnoreLabels set against a
// non-empty one on a 1000-sample Vector.  Run with:
//
//	go test -bench=BenchmarkMergeValuesDeterministic -benchmem -run=^$ ./pkg/promhttputil/
func BenchmarkMergeValuesDeterministic(b *testing.B) {
	const n = 1000

	makeVec := func(backend string) model.Vector {
		v := make(model.Vector, n)
		for i := 0; i < n; i++ {
			v[i] = &model.Sample{
				Metric: model.Metric{
					"__name__": model.LabelValue(fmt.Sprintf("metric_%d", i)),
					"instance": "host1",
					"backend":  model.LabelValue(backend),
				},
				Value:     model.SampleValue(i),
				Timestamp: model.Time(i * 1000),
			}
		}
		return v
	}

	a := makeVec("sg0")
	bVec := makeVec("sg1")

	b.Run("empty_ignore", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = merge2(a, bVec, nil, 0, 1)
		}
	})

	b.Run("nonempty_ignore", func(b *testing.B) {
		ignore := ignoreSet("backend")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, _ = merge2(a, bVec, ignore, 0, 1)
		}
	})
}
