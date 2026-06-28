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
// ignoreLabels union, MergeFunc wiring, and arrival-order fan-out/gather in
// MultiAPI. This is the clean extension point for B1 e2e coverage.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
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

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_b1_e2e_dedup_collisions_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "thanos", Labels: model.LabelSet{"backend": "thanos"}},
		{API: api1, Name: "vm", Labels: model.LabelSet{"backend": "vm"}},
	}, CrossGroupOpts{Collisions: counter})

	// First call to establish the reference. The cross-group merge always
	// routes through the Matrix path, so an instant query yields one
	// single-sample stream per series.
	ss0 := m.Query(context.Background(), "cpu", time.Now())
	if err := ss0.Err(); err != nil {
		t.Fatalf("Query iteration 0: %v", err)
	}

	mat0, err := SeriesSetToMatrix(ss0)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix iteration 0: %v", err)
	}

	// Expect: cpu (thanos wins), mem (thanos wins), disk (thanos only), net (vm only) = 4 series.
	if len(mat0) != 4 {
		t.Fatalf("expected 4 series, got %d: %v", len(mat0), mat0)
	}

	// Verify overlapping series carry the lower-ordinal backend's values.
	// wantBackend == "" means the backend label is not checked (series
	// unique to one backend, where the label is implied by the value).
	wantSeries := map[model.LabelValue]struct {
		wantValue   model.SampleValue
		wantBackend model.LabelValue
	}{
		"cpu":  {wantValue: 1, wantBackend: "thanos"},
		"mem":  {wantValue: 2, wantBackend: "thanos"},
		"disk": {wantValue: 3},
		"net":  {wantValue: 77},
	}
	for _, s := range mat0 {
		name := s.Metric["__name__"]
		want, ok := wantSeries[name]
		if !ok {
			t.Fatalf("unexpected series %q in result", name)
		}
		if len(s.Values) != 1 {
			t.Fatalf("%s: expected a single-sample stream for an instant query, got %d samples", name, len(s.Values))
		}
		if s.Values[0].Value != want.wantValue {
			t.Fatalf("%s: expected value %v, got %v", name, want.wantValue, s.Values[0].Value)
		}
		if want.wantBackend != "" && s.Metric["backend"] != want.wantBackend {
			t.Fatalf("%s: expected backend=%s, got %q", name, want.wantBackend, s.Metric["backend"])
		}
	}

	ref, err := json.Marshal(mat0)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	for i := 1; i < iterations; i++ {
		ssi := m.Query(context.Background(), "cpu", time.Now())
		if err := ssi.Err(); err != nil {
			t.Fatalf("Query iteration %d: %v", i, err)
		}
		mati, err := SeriesSetToMatrix(ssi)
		if err != nil {
			t.Fatalf("iteration %d SeriesSetToMatrix: %v", i, err)
		}
		got, err := json.Marshal(mati)
		if err != nil {
			t.Fatalf("iteration %d json.Marshal: %v", i, err)
		}
		if string(got) != string(ref) {
			t.Fatalf("iteration %d: result not byte-stable\ngot:  %s\nwant: %s", i, got, ref)
		}
	}
}

// TestB1E2E_ThreeBackendsNWayMerge exercises the three-backend scenario
// end-to-end through NewCrossGroupMultiAPI. The single-pass n-way merge buckets
// each series under its true origin ordinal (0, 1, 2) and resolves collisions
// in one step. Backends 0 and 2 share the same "cpu" series modulo backend
// labels; backend 1 contributes a unique "mem" series. Source 0 must win.
func TestB1E2E_ThreeBackendsNWayMerge(t *testing.T) {
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

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
		{API: api2, Name: "sg2", Labels: model.LabelSet{"backend": "sg2"}},
	}, CrossGroupOpts{})

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}

	if len(mat) != 2 {
		t.Fatalf("expected 2 series (cpu + mem), got %d: %v", len(mat), mat)
	}

	var cpuStream *model.SampleStream
	for _, s := range mat {
		if s.Metric["__name__"] == "cpu" {
			cpuStream = s
		}
	}
	if cpuStream == nil {
		t.Fatal("cpu series not found in result")
	}
	if cpuStream.Metric["backend"] != "sg0" {
		t.Fatalf("expected lowest-ordinal winner 'sg0', got %q", cpuStream.Metric["backend"])
	}
	if len(cpuStream.Values) != 1 {
		t.Fatalf("expected a single-sample stream for an instant query, got %d samples", len(cpuStream.Values))
	}
	if cpuStream.Values[0].Value != 1 {
		t.Fatalf("expected sg0 value 1, got %v", cpuStream.Values[0].Value)
	}
}

// TestB1E2E_QueryRangeDedup verifies that cross-group dedup works for
// QueryRange (matrix results), which exercises the matrix merge path through
// the full MultiAPI.
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

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "thanos", Labels: model.LabelSet{"backend": "thanos"}},
		{API: api1, Name: "vm", Labels: model.LabelSet{"backend": "vm"}},
	}, CrossGroupOpts{})

	r := v1.Range{Start: time.Now().Add(-time.Hour), End: time.Now(), Step: time.Minute}
	ss := m.QueryRange(context.Background(), "cpu", r)
	if err := ss.Err(); err != nil {
		t.Fatalf("QueryRange: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
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

// TestB1E2E_WinnerArrivesAfterLoserOnTheWire verifies that scatterMerge's
// shared result channel does not corrupt ordinal attribution when the
// lower-ordinal ("winner") backend's response arrives on the wire AFTER the
// higher-ordinal ("loser") backend's. ordinal travels with each chanResult
// (not inferred from arrival order/loop index), and sortOrdinalSeriesSets
// re-sorts by ordinal before merging -- so the lower-ordinal backend (sg0)
// must still win the "cpu" collision and be the recorded winner, even though
// sg1 answers first.
func TestB1E2E_WinnerArrivesAfterLoserOnTheWire(t *testing.T) {
	api0 := &stubAPI{ // ordinal 0 (winner), but slow to respond
		query: func() model.Value {
			time.Sleep(50 * time.Millisecond)
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 1, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{ // ordinal 1 (loser), responds immediately
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 99, Timestamp: 100},
			}
		},
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_winner_after_loser_collisions_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{Collisions: counter})

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}

	if len(mat) != 1 {
		t.Fatalf("expected 1 series (cpu), got %d: %v", len(mat), mat)
	}
	if got := mat[0].Metric["backend"]; got != "sg0" {
		t.Fatalf("expected lower-ordinal sg0 to win despite arriving second, got backend=%q", got)
	}

	// Collision must be attributed sg0-wins-over-sg1, never the reverse.
	if got := testutil.ToFloat64(counter.WithLabelValues("sg0", "sg1")); got != 1 {
		t.Fatalf("expected collision counter winner=sg0 loser=sg1 to be 1, got %v", got)
	}
	if got := testutil.ToFloat64(counter.WithLabelValues("sg1", "sg0")); got != 0 {
		t.Fatalf("collision misattributed in reverse (winner=sg1 loser=sg0): got %v, want 0", got)
	}
}

// TestB1E2E_SingleBackend verifies the degenerate single-backend case: with
// only one server_group configured, NewCrossGroupMultiAPI must construct
// successfully (requiredCount=1 is satisfiable by one API) and Query must
// pass the backend's data through unchanged.
func TestB1E2E_SingleBackend(t *testing.T) {
	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg0"}, Value: 42, Timestamp: 100},
			}
		},
	}

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
	}, CrossGroupOpts{})

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}
	if len(mat) != 1 {
		t.Fatalf("expected 1 series, got %d: %v", len(mat), mat)
	}
	if mat[0].Values[0].Value != 42 {
		t.Fatalf("expected value 42, got %v", mat[0].Values[0].Value)
	}
}

// TestB1E2E_EmptySeriesSetAmongHealthyBackends verifies that a backend
// returning a successful-but-empty result (zero series, no error) does not
// disrupt the merge: the other backend's series must still appear in the
// merged result, and the empty backend contributes nothing.
func TestB1E2E_EmptySeriesSetAmongHealthyBackends(t *testing.T) {
	api0 := &stubAPI{ // returns no series at all
		query: func() model.Value {
			return model.Vector{}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: model.Metric{"__name__": "cpu", "backend": "sg1"}, Value: 5, Timestamp: 100},
			}
		},
	}

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{})

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}
	if len(mat) != 1 {
		t.Fatalf("expected 1 series from the non-empty backend, got %d: %v", len(mat), mat)
	}
	if mat[0].Metric["backend"] != "sg1" {
		t.Fatalf("expected the sg1 series, got %v", mat[0])
	}
}

// TestB1E2E_DuplicateFullLabelSeriesAcrossGroups covers D2: two backends
// returning a series with IDENTICAL full labels (including the per-group
// external "backend" label -- not just the reduced fingerprint). This is the
// near-impossible-in-practice exact-dup case the spec calls out: the
// always-Matrix path's "keep first" rule must apply without crashing or
// double-counting a collision (the reduced-fingerprint bucket sees an exact
// labelset match, which mergeMatricesDeterministic treats as "keep first",
// not a collision).
func TestB1E2E_DuplicateFullLabelSeriesAcrossGroups(t *testing.T) {
	dup := model.Metric{"__name__": "cpu", "instance": "x", "backend": "shared"}

	api0 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: dup.Clone(), Value: 1, Timestamp: 100},
			}
		},
	}
	api1 := &stubAPI{
		query: func() model.Value {
			return model.Vector{
				{Metric: dup.Clone(), Value: 1, Timestamp: 100},
			}
		},
	}

	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_duplicate_full_label_collisions_total",
	}, []string{"winner", "loser"})

	m := newCrossGroupForTest(t, []CrossGroupBackend{
		{API: api0, Name: "sg0", Labels: model.LabelSet{"backend": "sg0"}},
		{API: api1, Name: "sg1", Labels: model.LabelSet{"backend": "sg1"}},
	}, CrossGroupOpts{Collisions: counter})

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("Query: %v", err)
	}

	mat, err := SeriesSetToMatrix(ss)
	if err != nil {
		t.Fatalf("SeriesSetToMatrix: %v", err)
	}
	if len(mat) != 1 {
		t.Fatalf("expected exact full-label duplicates to collapse to 1 series, got %d: %v", len(mat), mat)
	}
	if mat[0].Metric["backend"] != "shared" {
		t.Fatalf("expected the shared-label series, got %v", mat[0])
	}

	// An exact full-label duplicate is "keep first", not a genuine collision;
	// the collision counter must not be incremented for it.
	if got := testutil.ToFloat64(counter.WithLabelValues("sg0", "sg1")); got != 0 {
		t.Fatalf("exact full-label duplicate should not count as a collision, got %v", got)
	}
}

// regressionEmptySeriesSet is a sanity check that storage.EmptySeriesSet()
// (used as stubAPI's zero value) round-trips through drainToMatrix as a
// non-nil, zero-length model.Matrix with no error -- the shape
// mergeMatricesDeterministic must tolerate when one input is empty.
func TestDrainToMatrix_EmptySeriesSet(t *testing.T) {
	mat, err := drainToMatrix(storage.EmptySeriesSet())
	if err != nil {
		t.Fatalf("drainToMatrix: %v", err)
	}
	if len(mat) != 0 {
		t.Fatalf("expected empty matrix, got %d streams: %v", len(mat), mat)
	}
}
