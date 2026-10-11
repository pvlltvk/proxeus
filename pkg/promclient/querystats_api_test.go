package promclient

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/querystats"
)

// queryStatsInner answers every data call with the set built by mk.
type queryStatsInner struct {
	API
	mk func() storage.SeriesSet
}

func (a *queryStatsInner) Query(context.Context, string, time.Time) storage.SeriesSet {
	return a.mk()
}

func (a *queryStatsInner) QueryRange(context.Context, string, v1.Range) storage.SeriesSet {
	return a.mk()
}

func (a *queryStatsInner) GetValue(context.Context, time.Time, time.Time, []*labels.Matcher) storage.SeriesSet {
	return a.mk()
}

func (a *queryStatsInner) LabelNames(context.Context, []string, time.Time, time.Time) ([]string, v1.Warnings, error) {
	return []string{"a", "b"}, nil, nil
}

func queryStatsSeries(name string, histograms bool, samples int) storage.Series {
	var ss []chunks.Sample
	for i := 0; i < samples; i++ {
		if histograms {
			ss = append(ss, promapi.HistogramSample(int64(i*1000), &histogram.FloatHistogram{Count: float64(i + 1), Sum: float64(i)}))
		} else {
			ss = append(ss, promapi.FloatSample(int64(i*1000), float64(i)))
		}
	}
	return promapi.NewSeries(labels.FromStrings(model.MetricNameLabel, name), ss)
}

// queryStatsSet is 2 float series and 1 native histogram series, 4 samples each.
func queryStatsSet(warnings annotations.Annotations, err error) storage.SeriesSet {
	return promapi.NewSeriesSet([]storage.Series{
		queryStatsSeries("m_a", false, 4),
		queryStatsSeries("m_b", true, 4),
		queryStatsSeries("m_c", false, 4),
	}, warnings, err)
}

func queryStatsDump(ss storage.SeriesSet) []string {
	var out []string
	for ss.Next() {
		s := ss.At()
		line := s.Labels().String() + " =>"
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				ts, v := it.At()
				line += fmt.Sprintf(" %d=%g", ts, v)
			case chunkenc.ValFloatHistogram:
				ts, fh := it.AtFloatHistogram(nil)
				line += fmt.Sprintf(" %d=h%g", ts, fh.Count)
			default:
				line += " ?"
			}
		}
		out = append(out, line)
	}
	return out
}

func queryStatsWarnings(a annotations.Annotations) []string {
	var out []string
	for _, e := range a.AsErrors() {
		out = append(out, e.Error())
	}
	sort.Strings(out)
	return out
}

func newQueryStatsCtx(path string) (context.Context, *querystats.Recorder) {
	rec := querystats.NewRecorder([]querystats.Group{
		{Name: "g0", Ordinal: 0},
		{Name: "g1", Ordinal: 1},
	})
	ctx := querystats.WithPath(querystats.NewContext(context.Background(), rec), path)
	return ctx, rec
}

func TestQueryStatsAPI_NoRecorder_ReturnsInnerSetUntouched(t *testing.T) {
	inner := queryStatsSet(nil, nil)
	a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return inner }}, Ordinal: 1}
	ts := time.Unix(1700000000, 0)

	if got := a.Query(context.Background(), "m", ts); got != inner {
		t.Fatalf("Query without recorder returned a different set: %T", got)
	}
	if got := a.QueryRange(context.Background(), "m", v1.Range{Start: ts, End: ts, Step: time.Second}); got != inner {
		t.Fatalf("QueryRange without recorder returned a different set: %T", got)
	}
	if got := a.GetValue(context.Background(), ts, ts, nil); got != inner {
		t.Fatalf("GetValue without recorder returned a different set: %T", got)
	}
}

func TestQueryStatsAPI_RecordsExactCounts(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return queryStatsSet(nil, nil) }}, Ordinal: 1}

	t.Run("query", func(t *testing.T) {
		ctx, rec := newQueryStatsCtx(querystats.PathPushdown)
		ss := a.Query(ctx, "sum(m)", ts)
		if ss.Err() != nil {
			t.Fatal(ss.Err())
		}

		st := rec.Stats()
		if len(st.Requests) != 1 {
			t.Fatalf("got %d requests, want 1: %+v", len(st.Requests), st.Requests)
		}
		got := st.Requests[0]
		want := querystats.RequestStats{
			Group: "g1", Op: "query", Path: querystats.PathPushdown, Query: "sum(m)",
			Start: 1700000000, End: 1700000000, Series: 3, Samples: 12, Status: querystats.StatusSuccess,
		}
		got.Duration = 0
		if got != want {
			t.Fatalf("request mismatch:\n got  %+v\n want %+v", got, want)
		}
		g := st.Groups[1]
		if g.Requests != 1 || g.Errors != 0 || g.Series != 3 || g.Samples != 12 {
			t.Fatalf("group totals: %+v", g)
		}
		if g0 := st.Groups[0]; g0.Requests != 0 {
			t.Fatalf("other group touched: %+v", g0)
		}
	})

	t.Run("query_range", func(t *testing.T) {
		ctx, rec := newQueryStatsCtx(querystats.PathPushdown)
		end := ts.Add(time.Hour)
		ss := a.QueryRange(ctx, "sum(m)", v1.Range{Start: ts, End: end, Step: 15 * time.Second})
		if ss.Err() != nil {
			t.Fatal(ss.Err())
		}

		st := rec.Stats()
		if len(st.Requests) != 1 {
			t.Fatalf("got %d requests, want 1", len(st.Requests))
		}
		got := st.Requests[0]
		want := querystats.RequestStats{
			Group: "g1", Op: "query_range", Path: querystats.PathPushdown, Query: "sum(m)",
			Start: 1700000000, End: 1700003600, Step: 15, Series: 3, Samples: 12, Status: querystats.StatusSuccess,
		}
		got.Duration = 0
		if got != want {
			t.Fatalf("request mismatch:\n got  %+v\n want %+v", got, want)
		}
	})
}

func TestQueryStatsAPI_CountsAreFullEvenWhenConsumerStopsEarly(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return queryStatsSet(nil, nil) }}}
	ctx, rec := newQueryStatsCtx(querystats.PathRaw)

	ss := a.Query(ctx, "m", ts)
	if !ss.Next() {
		t.Fatal("no first series")
	}
	_ = ss.At().Labels()

	g := rec.Stats().Groups[0]
	if g.Series != 3 || g.Samples != 12 {
		t.Fatalf("after reading only the first series: series=%d samples=%d, want 3 and 12", g.Series, g.Samples)
	}
}

func TestQueryStatsAPI_ReturnsEquivalentSet(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	warn := annotations.Annotations{}
	warn.Add(errors.New("w1"))
	warn.Add(errors.New("w2"))
	mk := func() storage.SeriesSet { return queryStatsSet(warn, nil) }
	a := &QueryStatsAPI{API: &queryStatsInner{mk: mk}}

	want := queryStatsDump(mk())
	if len(want) != 3 {
		t.Fatalf("test setup: %v", want)
	}

	ctx, rec := newQueryStatsCtx(querystats.PathPushdown)
	ss := a.Query(ctx, "m", ts)
	got := queryStatsDump(ss)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("series differ:\n got  %v\n want %v", got, want)
	}
	if ss.Err() != nil {
		t.Fatalf("unexpected error: %v", ss.Err())
	}
	if gw, ww := queryStatsWarnings(ss.Warnings()), queryStatsWarnings(warn); !reflect.DeepEqual(gw, ww) {
		t.Fatalf("warnings differ: got %v, want %v", gw, ww)
	}
	if g := rec.Stats().Groups[0]; g.Series != 3 || g.Samples != 12 {
		t.Fatalf("totals after full drain: %+v", g)
	}
}

func TestQueryStatsAPI_CountsMergedResult(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	mk := func() storage.SeriesSet {
		a := ModelValueToSeriesSet(model.Matrix{stream("m_a", 0, 1, 10, 2), stream("m_b", 0, 1)}, nil, nil)
		b := ModelValueToSeriesSet(model.Matrix{stream("m_b", 0, 1, 10, 2, 20, 3), stream("m_c", 0, 1, 10, 2, 20, 3)}, nil, nil)
		return MergeSeriesSets(0, false, false, a, b)
	}

	var wantSeries, wantSamples int64
	ref := mk()
	for ref.Next() {
		wantSeries++
		it := ref.At().Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			wantSamples++
		}
	}
	if wantSeries != 3 || wantSamples < 7 {
		t.Fatalf("test setup: merged set has %d series / %d samples", wantSeries, wantSamples)
	}

	a := &QueryStatsAPI{API: &queryStatsInner{mk: mk}}
	ctx, rec := newQueryStatsCtx(querystats.PathPushdown)
	ss := a.Query(ctx, "m", ts)
	if !ss.Next() {
		t.Fatal("no series")
	}

	g := rec.Stats().Groups[0]
	if g.Series != wantSeries || g.Samples != wantSamples {
		t.Fatalf("got series=%d samples=%d, want %d and %d (counts of the merged result)", g.Series, g.Samples, wantSeries, wantSamples)
	}
}

func TestQueryStatsAPI_RecordsErrors(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	boom := errors.New("backend down")

	t.Run("error_only", func(t *testing.T) {
		a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return storage.ErrSeriesSet(boom) }}, Ordinal: 1}
		ctx, rec := newQueryStatsCtx(querystats.PathPushdown)

		ss := a.Query(ctx, "m", ts)
		if !errors.Is(ss.Err(), boom) {
			t.Fatalf("caller must still see the error, got %v", ss.Err())
		}
		_ = ss.Next()

		st := rec.Stats()
		if len(st.Requests) != 1 || st.Requests[0].Status != querystats.StatusError || !strings.Contains(st.Requests[0].Error, "backend down") {
			t.Fatalf("request: %+v", st.Requests)
		}
		if g := st.Groups[1]; g.Errors != 1 || g.Requests != 1 {
			t.Fatalf("group: %+v", g)
		}
	})

	t.Run("series_then_error", func(t *testing.T) {
		a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return queryStatsSet(nil, boom) }}}
		ctx, rec := newQueryStatsCtx(querystats.PathRaw)

		ss := a.GetValue(ctx, ts, ts, nil)
		n := len(queryStatsDump(ss))
		if !errors.Is(ss.Err(), boom) {
			t.Fatalf("caller must still see the error, got %v", ss.Err())
		}
		if n != 3 {
			t.Fatalf("caller saw %d series, want the 3 the backend sent before failing", n)
		}

		st := rec.Stats()
		if len(st.Requests) != 1 || st.Requests[0].Status != querystats.StatusError || st.Requests[0].Series != 3 || st.Requests[0].Samples != 12 {
			t.Fatalf("request: %+v", st.Requests)
		}
		if g := st.Groups[0]; g.Errors != 1 {
			t.Fatalf("group: %+v", g)
		}
	})
}

func TestQueryStatsAPI_GetValueRecordsSelector(t *testing.T) {
	a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return queryStatsSet(nil, nil) }}, Ordinal: 1}
	ctx, rec := newQueryStatsCtx(querystats.PathRaw)
	start, end := time.Unix(1700000000, 0), time.Unix(1700000300, 0)
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "up"),
		labels.MustNewMatcher(labels.MatchRegexp, "job", "api.*"),
	}

	_ = a.GetValue(ctx, start, end, matchers)

	st := rec.Stats()
	if len(st.Requests) != 1 {
		t.Fatalf("got %d requests, want 1", len(st.Requests))
	}
	got := st.Requests[0]
	want := querystats.RequestStats{
		Group: "g1", Op: "get_value", Path: querystats.PathRaw, Query: `{__name__="up",job=~"api.*"}`,
		Start: 1700000000, End: 1700000300, Series: 3, Samples: 12, Status: querystats.StatusSuccess,
	}
	got.Duration = 0
	if got != want {
		t.Fatalf("request mismatch:\n got  %+v\n want %+v", got, want)
	}
}

func TestQueryStatsAPI_ForwardedMethodsAreNotRecorded(t *testing.T) {
	a := &QueryStatsAPI{API: &queryStatsInner{mk: func() storage.SeriesSet { return queryStatsSet(nil, nil) }}}
	ctx, rec := newQueryStatsCtx(querystats.PathRaw)

	names, _, err := a.LabelNames(ctx, nil, time.Time{}, time.Time{})
	if err != nil || !reflect.DeepEqual(names, []string{"a", "b"}) {
		t.Fatalf("LabelNames = %v, %v", names, err)
	}

	st := rec.Stats()
	if len(st.Requests) != 0 || st.Groups[0].Requests != 0 {
		t.Fatalf("forwarded call was recorded: %+v", st)
	}
}
