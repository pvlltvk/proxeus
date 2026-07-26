package promhttputil

import (
	"testing"

	"github.com/prometheus/common/model"
)

// Cross-group dedup buckets and picks whole series, so it should never inspect
// or rewrite a sample's payload. These tests pin that for native histograms,
// where the data lives in Sample.Histogram / SampleStream.Histograms rather
// than in the float Value that the exact-duplicate paths do touch.

// sampleHistogram builds a small native histogram with a distinguishable sum,
// so a test can tell which backend's copy survived a merge.
func sampleHistogram(sum float64) *model.SampleHistogram {
	return &model.SampleHistogram{
		Count: 4,
		Sum:   model.FloatString(sum),
		Buckets: model.HistogramBuckets{
			{Boundaries: 0, Lower: 0, Upper: 1, Count: 1},
			{Boundaries: 0, Lower: 1, Upper: 2, Count: 3},
		},
	}
}

func TestMergeValuesDeterministic_NativeHistogramVector(t *testing.T) {
	ignore := ignoreSet("backend")

	// Same logical series from two backends, each carrying a native histogram
	// with a different sum, plus one series unique to the higher ordinal.
	thanos := model.Vector{
		{Metric: model.Metric{"__name__": "latency", "backend": "thanos"}, Histogram: sampleHistogram(10), Timestamp: 1},
	}
	victoria := model.Vector{
		{Metric: model.Metric{"__name__": "latency", "backend": "victoria"}, Histogram: sampleHistogram(20), Timestamp: 1},
		{Metric: model.Metric{"__name__": "vm_only", "backend": "victoria"}, Histogram: sampleHistogram(30), Timestamp: 1},
	}

	v, stats, err := merge2(thanos, victoria, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res, ok := v.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", v)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 samples (collided pair collapsed + vm_only), got %d: %v", len(res), res)
	}
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}

	byName := map[model.LabelValue]*model.Sample{}
	for _, s := range res {
		byName[s.Metric["__name__"]] = s
	}

	// The lowest-ordinal backend wins, and its histogram survives untouched —
	// the exact-duplicate path must not blank it while patching a zero Value.
	won := byName["latency"]
	if won == nil {
		t.Fatal("latency series missing from merge result")
	}
	if won.Metric["backend"] != "thanos" {
		t.Errorf("latency: backend = %q, want thanos (lowest ordinal)", won.Metric["backend"])
	}
	if won.Histogram == nil {
		t.Fatal("latency: Histogram was dropped by the merge")
	}
	if !won.Histogram.Equal(sampleHistogram(10)) {
		t.Errorf("latency: Histogram = %v, want thanos' copy (sum 10)", won.Histogram)
	}
	if won.Value != 0 {
		t.Errorf("latency: Value = %v, want 0 — a histogram sample must not gain a float value", won.Value)
	}

	// A non-colliding histogram series passes through unchanged.
	passthrough := byName["vm_only"]
	if passthrough == nil {
		t.Fatal("vm_only series missing from merge result")
	}
	if passthrough.Histogram == nil || !passthrough.Histogram.Equal(sampleHistogram(30)) {
		t.Errorf("vm_only: Histogram = %v, want sum 30", passthrough.Histogram)
	}
}

func TestMergeValuesDeterministic_NativeHistogramMatrix(t *testing.T) {
	ignore := ignoreSet("backend")

	thanos := model.Matrix{
		{
			Metric: model.Metric{"__name__": "latency", "backend": "thanos"},
			Histograms: []model.SampleHistogramPair{
				{Timestamp: 1, Histogram: sampleHistogram(10)},
				{Timestamp: 2, Histogram: sampleHistogram(11)},
			},
		},
	}
	victoria := model.Matrix{
		{
			Metric: model.Metric{"__name__": "latency", "backend": "victoria"},
			Histograms: []model.SampleHistogramPair{
				{Timestamp: 1, Histogram: sampleHistogram(20)},
				{Timestamp: 2, Histogram: sampleHistogram(21)},
			},
		},
	}

	v, stats, err := merge2(thanos, victoria, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res, ok := v.(model.Matrix)
	if !ok {
		t.Fatalf("expected model.Matrix, got %T", v)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 stream after dedup, got %d: %v", len(res), res)
	}
	if stats.Collisions != 1 {
		t.Fatalf("expected 1 collision, got %d", stats.Collisions)
	}

	won := res[0]
	if won.Metric["backend"] != "thanos" {
		t.Errorf("backend = %q, want thanos (lowest ordinal)", won.Metric["backend"])
	}
	// Streams are picked whole, never interleaved: exactly the winner's
	// histogram points, in order, with no values borrowed from the loser.
	if len(won.Values) != 0 {
		t.Errorf("Values = %v, want empty — a histogram stream must not gain float samples", won.Values)
	}
	if len(won.Histograms) != 2 {
		t.Fatalf("Histograms has %d points, want 2 (winner's stream kept whole)", len(won.Histograms))
	}
	for i, want := range []float64{10, 11} {
		got := won.Histograms[i]
		if got.Timestamp != model.Time(i+1) {
			t.Errorf("Histograms[%d].Timestamp = %v, want %d", i, got.Timestamp, i+1)
		}
		if got.Histogram == nil || !got.Histogram.Equal(sampleHistogram(want)) {
			t.Errorf("Histograms[%d].Histogram = %v, want thanos' copy (sum %v)", i, got.Histogram, want)
		}
	}
}

// A mixed response — some series float, some native histogram — must dedup on
// series identity alone, with each surviving series keeping its own payload
// shape.
func TestMergeValuesDeterministic_MixedFloatAndHistogramVector(t *testing.T) {
	ignore := ignoreSet("backend")

	thanos := model.Vector{
		{Metric: model.Metric{"__name__": "hist", "backend": "thanos"}, Histogram: sampleHistogram(10), Timestamp: 1},
		{Metric: model.Metric{"__name__": "gauge", "backend": "thanos"}, Value: 7, Timestamp: 1},
	}
	victoria := model.Vector{
		{Metric: model.Metric{"__name__": "hist", "backend": "victoria"}, Histogram: sampleHistogram(20), Timestamp: 1},
		{Metric: model.Metric{"__name__": "gauge", "backend": "victoria"}, Value: 9, Timestamp: 1},
	}

	v, stats, err := merge2(thanos, victoria, ignore, 0, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	res := v.(model.Vector)
	if len(res) != 2 {
		t.Fatalf("expected 2 samples, got %d: %v", len(res), res)
	}
	if stats.Collisions != 2 {
		t.Fatalf("expected 2 collisions, got %d", stats.Collisions)
	}

	for _, s := range res {
		if s.Metric["backend"] != "thanos" {
			t.Errorf("%s: backend = %q, want thanos", s.Metric["__name__"], s.Metric["backend"])
		}
		switch s.Metric["__name__"] {
		case "hist":
			if s.Histogram == nil || !s.Histogram.Equal(sampleHistogram(10)) {
				t.Errorf("hist: Histogram = %v, want sum 10", s.Histogram)
			}
		case "gauge":
			if s.Histogram != nil {
				t.Errorf("gauge: Histogram = %v, want nil", s.Histogram)
			}
			if s.Value != 7 {
				t.Errorf("gauge: Value = %v, want 7", s.Value)
			}
		default:
			t.Errorf("unexpected series %v", s.Metric)
		}
	}
}
