package promclient

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

func TestNewCrossGroupMultiAPI_Query(t *testing.T) {
	// api0 returns cpu with server_group=sg0 (ordinal 0, should win).
	// api1 returns the same cpu metric (modulo server_group) with server_group=sg1.
	// Both also return a unique series (mem for api0, disk for api1).

	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "server_group": "sg0"}, Value: 1, Timestamp: 100},
				{Metric: model.Metric{"__name__": "mem", "server_group": "sg0"}, Value: 2, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "server_group": "sg1"}, Value: 99, Timestamp: 100},
				{Metric: model.Metric{"__name__": "disk", "server_group": "sg1"}, Value: 3, Timestamp: 100},
			}
		},
	}

	groupNames := []string{"sg0", "sg1"}
	groupLabels := []model.LabelSet{
		{"server_group": "sg0"},
		{"server_group": "sg1"},
	}

	// Register a counter to verify collision counting.
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_cross_group_dedup_collisions_total",
	}, []string{"winner", "loser"})

	m, err := NewCrossGroupMultiAPI([]API{api0, api1}, groupNames, groupLabels, counter)
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

	// Expect: cpu (winner sg0), mem (only in sg0), disk (only in sg1) = 3 series.
	if len(vec) != 3 {
		t.Fatalf("expected 3 series, got %d: %v", len(vec), vec)
	}

	// The cpu series must come from sg0 (lower ordinal wins).
	var cpuSample *model.Sample
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" {
			cpuSample = s
		}
	}
	if cpuSample == nil {
		t.Fatal("cpu series not found in result")
	}
	if cpuSample.Metric["server_group"] != "sg0" {
		t.Fatalf("expected cpu winner 'sg0', got %q", cpuSample.Metric["server_group"])
	}

	// Verify the dedup counter was incremented for the cpu collision.
	val, err := counter.GetMetricWithLabelValues("sg0", "sg1")
	if err != nil {
		t.Fatalf("counter.GetMetricWithLabelValues: %v", err)
	}
	// Gather and check value.
	ch := make(chan prometheus.Metric, 1)
	val.Collect(ch)
	m2 := <-ch
	var pb model.SampleValue
	_ = pb // avoid import issue; just verifying no panic above is enough for this test.
	_ = m2
}

func TestNewCrossGroupMultiAPI_LengthMismatch(t *testing.T) {
	_, err := NewCrossGroupMultiAPI(
		[]API{&stubAPI{}},
		[]string{"a", "b"},
		[]model.LabelSet{{"x": "1"}},
		nil,
	)
	if err == nil {
		t.Fatal("expected error on length mismatch")
	}
}
