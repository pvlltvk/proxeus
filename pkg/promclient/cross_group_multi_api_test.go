package promclient

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
)

// newCrossGroupForTest builds a *MultiAPI via NewCrossGroupMultiAPI and fails
// the test immediately if construction errors, removing the repeated
// constructor-call-plus-error-check boilerplate from each cross-group test.
func newCrossGroupForTest(t *testing.T, backends []CrossGroupBackend, opts CrossGroupOpts) *MultiAPI {
	t.Helper()
	m, err := NewCrossGroupMultiAPI(backends, opts)
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}
	return m
}

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

	// Register a counter to verify collision counting.
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_cross_group_dedup_collisions_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"server_group": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"server_group": "sg1"}},
	}, CrossGroupOpts{Collisions: counter})

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
	got2 := testutil.ToFloat64(counter.WithLabelValues("sg0", "sg1"))
	if got2 != 1 {
		t.Fatalf("expected collision counter winner=sg0 loser=sg1 to be 1, got %v", got2)
	}
}

// TestNewCrossGroupMultiAPI_CollisionCounterIncremented verifies that the
// dedupCounter passed to NewCrossGroupMultiAPI is incremented exactly once for
// each colliding series, with the correct winner/loser label values.
//
// Setup: two backends return the same "cpu" metric (modulo a "backend" label).
// After dedup the lower-ordinal backend (sg0) wins, so the counter must record
// one collision with winner="sg0", loser="sg1".
func TestNewCrossGroupMultiAPI_CollisionCounterIncremented(t *testing.T) {
	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 99, Timestamp: 100},
			}
		},
	}

	// Use an unregistered counter so this test does not touch the global registry.
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_collision_counter_incremented_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{Collisions: counter})

	_, _, err := m.Query(context.Background(), "cpu", time.Now())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// One cpu series collided; sg0 (ordinal 0) wins over sg1 (ordinal 1).
	got := testutil.ToFloat64(counter.WithLabelValues("sg0", "sg1"))
	if got != 1 {
		t.Fatalf("expected collision counter winner=sg0 loser=sg1 to be 1, got %v", got)
	}

	// No counter for the reverse direction.
	gotReverse := testutil.ToFloat64(counter.WithLabelValues("sg1", "sg0"))
	if gotReverse != 0 {
		t.Fatalf("expected no reverse collision counter, got %v", gotReverse)
	}
}

// TestNewCrossGroupMultiAPI_SeriesDedup verifies F2: when dedupMetadata is true,
// /api/v1/series collapses series that differ only by external labels, keeping
// the lower-ordinal backend's full labelset.
func TestNewCrossGroupMultiAPI_SeriesDedup(t *testing.T) {
	api0 := &stubAPI{
		series: func() []model.LabelSet {
			return []model.LabelSet{
				{"__name__": "up", "instance": "node:9100", "backend": "sg0"},
				{"__name__": "up", "instance": "only-on-sg0", "backend": "sg0"},
			}
		},
	}
	api1 := &stubAPI{
		series: func() []model.LabelSet {
			return []model.LabelSet{
				{"__name__": "up", "instance": "node:9100", "backend": "sg1"},
				{"__name__": "up", "instance": "only-on-sg1", "backend": "sg1"},
			}
		},
	}

	metaCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_series_dedup_metadata_collisions_total",
	}, []string{"winner", "loser", "endpoint"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{DedupMetadata: true, MetadataCollisions: metaCounter})

	got, _, err := m.Series(context.Background(), []string{"up"}, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("Series: %v", err)
	}

	// Expect 3 labelsets: shared node:9100 (sg0 wins) + only-on-sg0 + only-on-sg1.
	if len(got) != 3 {
		t.Fatalf("expected 3 series, got %d: %v", len(got), got)
	}

	// The shared node:9100 must come from sg0 (lower ordinal wins).
	var shared model.LabelSet
	for _, ls := range got {
		if ls["instance"] == "node:9100" {
			shared = ls
		}
	}
	if shared == nil {
		t.Fatal("shared node:9100 series not found")
	}
	if shared["backend"] != "sg0" {
		t.Fatalf("expected shared series to come from sg0, got backend=%q", shared["backend"])
	}

	// Collision counter must record exactly one collision for sg0 winning over sg1.
	wantCollisions := testutil.ToFloat64(metaCounter.WithLabelValues("sg0", "sg1", "series"))
	if wantCollisions != 1 {
		t.Fatalf("expected metadata collision counter sg0/sg1/series=1, got %v", wantCollisions)
	}
}

// TestNewCrossGroupMultiAPI_SeriesNoDedupWhenDisabled verifies that with
// dedupMetadata=false (default), /series returns one row per backend even for
// logically-identical series — preserving the pre-F2 behavior and proving the
// flag is the only switch.
func TestNewCrossGroupMultiAPI_SeriesNoDedupWhenDisabled(t *testing.T) {
	api0 := &stubAPI{
		series: func() []model.LabelSet {
			return []model.LabelSet{{"__name__": "up", "instance": "node:9100", "backend": "sg0"}}
		},
	}
	api1 := &stubAPI{
		series: func() []model.LabelSet {
			return []model.LabelSet{{"__name__": "up", "instance": "node:9100", "backend": "sg1"}}
		},
	}

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{})

	got, _, err := m.Series(context.Background(), []string{"up"}, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 series (no dedup), got %d: %v", len(got), got)
	}
}

// TestNewCrossGroupMultiAPI_CollisionAttributionMiddleOrdinal verifies that
// collision attribution is exact when the colliding series exists only in
// backends 1 and 2 (not 0). The lowest ordinal that actually contributes the
// winner is sg1, so the counter must record {winner=sg1, loser=sg2} and zero
// for {sg0, sg2}.
//
// The chained binary form misattributed this: after merging backends 0+1 into
// an accumulator, it presented the accumulator under the running-minimum ordinal
// (0), so merging backend 2 blamed sg0 for a series it never served. The n-way
// merge buckets each series under its true origin ordinal, giving exact attribution.
func TestNewCrossGroupMultiAPI_CollisionAttributionMiddleOrdinal(t *testing.T) {
	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "mem", "backend": "sg0"}, Value: 7, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 1, Timestamp: 100},
			}
		},
	}
	api2 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg2"}, Value: 2, Timestamp: 100},
			}
		},
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_collision_attribution_middle_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
		{API: api2, Name: "sg2", Labels: model.LabelSet{"backend": "sg2"}},
	}, CrossGroupOpts{Collisions: counter})

	v, _, err := m.Query(context.Background(), "cpu", time.Now())
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// cpu must come from sg1 (lower of the two contributing ordinals).
	vec, ok := v.(model.Vector)
	if !ok {
		t.Fatalf("expected model.Vector, got %T", v)
	}
	for _, s := range vec {
		if s.Metric["__name__"] == "cpu" && s.Metric["backend"] != "sg1" {
			t.Fatalf("cpu: expected winner backend sg1, got %q", s.Metric["backend"])
		}
	}

	if got := testutil.ToFloat64(counter.WithLabelValues("sg1", "sg2")); got != 1 {
		t.Fatalf("expected collision attributed to winner=sg1 loser=sg2 (=1), got %v", got)
	}
	if got := testutil.ToFloat64(counter.WithLabelValues("sg0", "sg2")); got != 0 {
		t.Fatalf("collision misattributed to sg0 (which never served cpu): sg0/sg2=%v, want 0", got)
	}
}
