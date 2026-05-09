package promhttputil

import (
	"testing"

	"github.com/prometheus/common/model"
)

func dedupOpts(ignoreKey model.LabelName, ordA, ordB int) DedupOpts {
	return DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{ignoreKey: {}},
		OrdinalA:     ordA,
		OrdinalB:     ordB,
		NameA:        "group-a",
		NameB:        "group-b",
	}
}

func TestMergeValuesDeterministic_NilInputs(t *testing.T) {
	opts := dedupOpts("server_group", 0, 1)

	t.Run("both nil", func(t *testing.T) {
		v, stats, err := MergeValuesDeterministic(nil, nil, opts)
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
		v, stats, err := MergeValuesDeterministic(nil, vec, opts)
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
		v, stats, err := MergeValuesDeterministic(vec, nil, opts)
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
	opts := dedupOpts("server_group", 0, 1)
	a := model.Vector{{Metric: model.Metric{"__name__": "foo"}, Value: 1, Timestamp: 1}}
	b := model.Matrix{&model.SampleStream{Metric: model.Metric{"__name__": "foo"}}}
	_, _, err := MergeValuesDeterministic(a, b, opts)
	if err == nil {
		t.Fatal("expected error on type mismatch")
	}
}

func TestMergeValuesDeterministic_Vector(t *testing.T) {
	// series that differ only in "server_group" label are considered collisions.
	// lower ordinal wins.
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"server_group": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "thanos",
		NameB:        "victoria",
	}

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

	v, stats, err := MergeValuesDeterministic(a, b, opts)
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
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"server_group": {}},
		OrdinalA:     1,
		OrdinalB:     0,
		NameA:        "late",
		NameB:        "early",
	}

	sA := &model.Sample{
		Metric: model.Metric{"__name__": "cpu", "server_group": "late"},
		Value:  10,
	}
	sB := &model.Sample{
		Metric: model.Metric{"__name__": "cpu", "server_group": "early"},
		Value:  20,
	}

	v, stats, err := MergeValuesDeterministic(model.Vector{sA}, model.Vector{sB}, opts)
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
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"server_group": {}},
		OrdinalA:     0,
		OrdinalB:     1,
	}

	// Two completely different series (different __name__).
	sA := &model.Sample{Metric: model.Metric{"__name__": "alpha", "server_group": "sg0"}, Value: 1}
	sB := &model.Sample{Metric: model.Metric{"__name__": "beta", "server_group": "sg1"}, Value: 2}

	v, stats, err := MergeValuesDeterministic(model.Vector{sA}, model.Vector{sB}, opts)
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
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"server_group": {}},
		OrdinalA:     0,
		OrdinalB:     1,
		NameA:        "sg0",
		NameB:        "sg1",
	}

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

	v, stats, err := MergeValuesDeterministic(a, b, opts)
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
	opts := DedupOpts{
		IgnoreLabels: map[model.LabelName]struct{}{"server_group": {}},
		OrdinalA:     0,
		OrdinalB:     1,
	}

	streamA := &model.SampleStream{
		Metric: model.Metric{"__name__": "cpu", "server_group": "sg0"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 10}},
	}
	streamB := &model.SampleStream{
		Metric: model.Metric{"__name__": "mem", "server_group": "sg1"},
		Values: []model.SamplePair{{Timestamp: 1, Value: 5}},
	}

	v, stats, err := MergeValuesDeterministic(model.Matrix{streamA}, model.Matrix{streamB}, opts)
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
