package promclient

// e2e-equivalent tests for B1 cross-group dedup via NewCrossGroupMultiAPI.
//
// The test/promql_test.go harness uses fixed ports (8083/8085) and a shared
// storage backend — both backends serve the same data, which makes it
// impossible to assert "lower-ordinal value wins over a different value from
// the higher-ordinal backend" without invasive changes to the harness. The
// harness also cannot easily be extended without binding real ports, which
// causes flaky failures under parallel test runs.
//
// The tests here use stubAPI (defined in multi_api_test.go, same package) and
// NewCrossGroupMultiAPI to exercise the full integration path: config building,
// ignoreLabels union, MergeFunc wiring, and resultIdx propagation in MultiAPI.
// This is the clean extension point for B1 e2e coverage.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

// TestB1E2E_TwoBackendsLowerOrdinalWins is the primary end-to-end integration
// test. Two stub backends return the same series modulo a synthetic "backend"
// label. After cross-group dedup the result must contain exactly one series for
// each metric name, with the lower-ordinal backend's value, and be byte-stable
// across 50 iterations.
func TestB1E2E_TwoBackendsLowerOrdinalWins(t *testing.T) {
	const iterations = 50

	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "thanos"}, Value: 1, Timestamp: 100},
				{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "thanos"}, Value: 2, Timestamp: 100},
				{Metric: model.Metric{"__name__": "disk", "instance": "x", "backend": "thanos"}, Value: 3, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "vm"}, Value: 99, Timestamp: 100},
				{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "vm"}, Value: 88, Timestamp: 100},
				{Metric: model.Metric{"__name__": "net", "instance": "x", "backend": "vm"}, Value: 77, Timestamp: 100},
			}
		},
	}

	groupNames := []string{"thanos", "vm"}
	groupLabels := []model.LabelSet{
		{"backend": "thanos"},
		{"backend": "vm"},
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_b1_e2e_dedup_collisions_total",
	}, []string{"winner", "loser"})

	m, err := NewCrossGroupMultiAPI([]API{api0, api1}, groupNames, groupLabels, counter, false, nil)
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}

	// First call to establish the reference.
	v0, _, err := m.Query(context.Background(), "cpu", time.Now())
	if err != nil {
		t.Fatalf("Query iteration 0: %v", err)
	}

	vec0, ok := v0.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", v0)
	}

	// Expect: cpu (thanos wins), mem (thanos wins), disk (thanos only), net (vm only) = 4 series.
	if len(vec0) != 4 {
		t.Fatalf("expected 4 series, got %d: %v", len(vec0), vec0)
	}

	// Verify overlapping series carry the lower-ordinal backend's values.
	for _, s := range vec0 {
		name := s.Metric["__name__"]
		switch name {
		case "cpu":
			if s.Value != 1 {
				t.Fatalf("cpu: expected thanos value 1, got %v", s.Value)
			}
			if s.Metric["backend"] != "thanos" {
				t.Fatalf("cpu: expected backend=thanos, got %q", s.Metric["backend"])
			}
		case "mem":
			if s.Value != 2 {
				t.Fatalf("mem: expected thanos value 2, got %v", s.Value)
			}
			if s.Metric["backend"] != "thanos" {
				t.Fatalf("mem: expected backend=thanos, got %q", s.Metric["backend"])
			}
		case "disk":
			if s.Value != 3 {
				t.Fatalf("disk: expected thanos value 3, got %v", s.Value)
			}
		case "net":
			if s.Value != 77 {
				t.Fatalf("net: expected vm value 77, got %v", s.Value)
			}
		default:
			t.Fatalf("unexpected series %q in result", name)
		}
	}

	ref, err := json.Marshal(v0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < iterations; i++ {
		vi, _, err := m.Query(context.Background(), "cpu", time.Now())
		if err != nil {
			t.Fatalf("Query iteration %d: %v", i, err)
		}
		got, err := json.Marshal(vi)
		if err != nil {
			t.Fatalf("iteration %d json.Marshal: %v", i, err)
		}
		if string(got) != string(ref) {
			t.Fatalf("iteration %d: result not byte-stable\ngot:  %s\nwant: %s", i, got, ref)
		}
	}
}

// TestB1E2E_ThreeBackendsChainedMerge exercises the three-backend scenario
// end-to-end through NewCrossGroupMultiAPI. MultiAPI reads from resultChans in
// ordinal order (i=0,1,2), so resultIdx tracks the running minimum ordinal.
// Backends 0 and 2 share the same "cpu" series modulo backend labels; backend 1
// contributes a unique "mem" series. Source 0 must win.
func TestB1E2E_ThreeBackendsChainedMerge(t *testing.T) {
	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg0"}, Value: 1, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "mem", "instance": "x", "backend": "sg1"}, Value: 2, Timestamp: 100},
			}
		},
	}
	api2 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "sg2"}, Value: 3, Timestamp: 100},
			}
		},
	}

	groupNames := []string{"sg0", "sg1", "sg2"}
	groupLabels := []model.LabelSet{
		{"backend": "sg0"},
		{"backend": "sg1"},
		{"backend": "sg2"},
	}

	m, err := NewCrossGroupMultiAPI([]API{api0, api1, api2}, groupNames, groupLabels, nil, false, nil)
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}

	v, _, err := m.Query(context.Background(), "cpu", time.Now())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	vec, ok := v.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", v)
	}

	if len(vec) != 2 {
		t.Fatalf("expected 2 series (cpu + mem), got %d: %v", len(vec), vec)
	}

	var cpuSample *model.Sample
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" {
			cpuSample = s
		}
	}
	if cpuSample == nil {
		t.Fatal("cpu series not found in result")
	}
	if cpuSample.Metric["backend"] != "sg0" {
		t.Fatalf("expected lowest-ordinal winner 'sg0', got %q", cpuSample.Metric["backend"])
	}
	if cpuSample.Value != 1 {
		t.Fatalf("expected sg0 value 1, got %v", cpuSample.Value)
	}
}

// TestB1E2E_QueryRangeDedup verifies that cross-group dedup works for
// QueryRange (matrix results), which exercises mergeMatrixDeterministic through
// the full MultiAPI path.
func TestB1E2E_QueryRangeDedup(t *testing.T) {
	api0 := &stubAPI{
		queryRange: func(_ string, _ v1.Range) model.Value {
			return model.Matrix{
				{
					Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "thanos"},
					Values: []model.SamplePair{{Timestamp: 1, Value: 10}, {Timestamp: 2, Value: 11}},
				},
				{
					Metric: model.Metric{"__name__": "disk", "instance": "x", "backend": "thanos"},
					Values: []model.SamplePair{{Timestamp: 1, Value: 9}},
				},
			}
		},
	}
	api1 := &stubAPI{
		queryRange: func(_ string, _ v1.Range) model.Value {
			return model.Matrix{
				{
					Metric: model.Metric{"__name__": "cpu", "instance": "x", "backend": "vm"},
					Values: []model.SamplePair{{Timestamp: 1, Value: 99}, {Timestamp: 2, Value: 100}},
				},
				{
					Metric: model.Metric{"__name__": "net", "instance": "x", "backend": "vm"},
					Values: []model.SamplePair{{Timestamp: 1, Value: 5}},
				},
			}
		},
	}

	groupNames := []string{"thanos", "vm"}
	groupLabels := []model.LabelSet{
		{"backend": "thanos"},
		{"backend": "vm"},
	}

	m, err := NewCrossGroupMultiAPI([]API{api0, api1}, groupNames, groupLabels, nil, false, nil)
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}

	r := v1.Range{Start: time.Now().Add(-time.Hour), End: time.Now(), Step: time.Minute}
	v, _, err := m.QueryRange(context.Background(), "cpu", r)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}

	mat, ok := v.(model.Matrix)
	if !ok {
		t.Fatalf("expected model.Matrix, got %T", v)
	}

	// Expect: cpu (thanos wins), disk (thanos only), net (vm only) = 3 streams.
	if len(mat) != 3 {
		t.Fatalf("expected 3 streams, got %d: %v", len(mat), mat)
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
	if cpuStream.Metric["backend"] != "thanos" {
		t.Fatalf("expected thanos to win cpu stream, got %q", cpuStream.Metric["backend"])
	}
	if cpuStream.Values[0].Value != 10 {
		t.Fatalf("expected thanos first value 10, got %v", cpuStream.Values[0].Value)
	}
}
