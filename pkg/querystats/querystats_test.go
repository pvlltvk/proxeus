package querystats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
)

func testGroups() []Group {
	return []Group{
		{Name: "victoria", Ordinal: 1, Labels: map[string]string{"backend": "victoria"}},
		{Name: "idle", Ordinal: 2, Labels: map[string]string{"backend": "idle"}},
		{Name: "thanos", Ordinal: 0, Labels: map[string]string{"backend": "thanos"}},
	}
}

func req(ordinal int, query string) Request {
	return Request{
		GroupOrdinal: ordinal,
		Op:           "query",
		Path:         PathPushdown,
		Query:        query,
		Start:        time.Unix(100, 0),
		End:          time.Unix(100, 0),
	}
}

func groupByName(t *testing.T, s Stats, name string) GroupStats {
	t.Helper()
	for _, g := range s.Groups {
		if g.Name == name {
			return g
		}
	}
	t.Fatalf("group %q not in stats", name)
	return GroupStats{}
}

func toMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRequests_AreInBeginOrder_WhenFinishedInReverse(t *testing.T) {
	r := NewRecorder(testGroups())
	var entries []*Entry
	for i := 0; i < 5; i++ {
		entries = append(entries, r.Begin(req(0, fmt.Sprintf("q%d", i))))
	}
	for i := len(entries) - 1; i >= 0; i-- {
		entries[i].Finish(1, 1, nil)
	}
	s := r.Stats()
	if len(s.Requests) != 5 {
		t.Fatalf("got %d requests, want 5", len(s.Requests))
	}
	for i, rs := range s.Requests {
		if want := fmt.Sprintf("q%d", i); rs.Query != want {
			t.Errorf("requests[%d].Query = %q, want %q", i, rs.Query, want)
		}
	}
}

func TestRequests_CappedAtMax_WhenMoreBegun(t *testing.T) {
	const extra = 7
	total := MaxRequests + extra
	r := NewRecorder(testGroups())
	var entries []*Entry
	for i := 0; i < total; i++ {
		entries = append(entries, r.Begin(req(0, fmt.Sprintf("q%d", i))))
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if i%3 == 0 {
			entries[i].Finish(2, 10, errors.New("boom"))
		} else {
			entries[i].Finish(2, 10, nil)
		}
	}
	s := r.Stats()
	if len(s.Requests) != MaxRequests {
		t.Fatalf("got %d requests, want %d", len(s.Requests), MaxRequests)
	}
	for i, rs := range s.Requests {
		if want := fmt.Sprintf("q%d", i); rs.Query != want {
			t.Fatalf("requests[%d].Query = %q, want %q", i, rs.Query, want)
		}
	}
	if s.RequestsTruncated != extra {
		t.Errorf("RequestsTruncated = %d, want %d", s.RequestsTruncated, extra)
	}
	wantErrors := 0
	for i := 0; i < total; i++ {
		if i%3 == 0 {
			wantErrors++
		}
	}
	g := groupByName(t, s, "thanos")
	if g.Requests != total {
		t.Errorf("group requests = %d, want %d", g.Requests, total)
	}
	if g.Series != int64(2*total) {
		t.Errorf("group series = %d, want %d", g.Series, 2*total)
	}
	if g.Samples != int64(10*total) {
		t.Errorf("group samples = %d, want %d", g.Samples, 10*total)
	}
	if g.Errors != wantErrors {
		t.Errorf("group errors = %d, want %d", g.Errors, wantErrors)
	}
}

func TestRequests_NotTruncated_WhenExactlyMax(t *testing.T) {
	r := NewRecorder(testGroups())
	for i := 0; i < MaxRequests; i++ {
		r.Begin(req(0, "q")).Finish(0, 0, nil)
	}
	s := r.Stats()
	if len(s.Requests) != MaxRequests || s.RequestsTruncated != 0 {
		t.Errorf("len=%d truncated=%d, want %d and 0", len(s.Requests), s.RequestsTruncated, MaxRequests)
	}
}

func TestGroupTotals_EqualSumOfEntries(t *testing.T) {
	r := NewRecorder(testGroups())
	e1 := r.Begin(req(0, "a"))
	e2 := r.Begin(req(1, "b"))
	e3 := r.Begin(req(0, "c"))
	e4 := r.Begin(req(0, "d"))
	time.Sleep(3 * time.Millisecond)
	e1.Finish(1, 10, nil)
	e2.Finish(5, 50, nil)
	e3.Finish(2, 20, errors.New("fail"))
	e4.Finish(3, 30, nil)

	s := r.Stats()
	sums := map[string]*GroupStats{}
	for _, rs := range s.Requests {
		g := sums[rs.Group]
		if g == nil {
			g = &GroupStats{}
			sums[rs.Group] = g
		}
		g.Requests++
		g.Series += rs.Series
		g.Samples += rs.Samples
		g.Duration += rs.Duration
		if rs.Status == StatusError {
			g.Errors++
		}
	}
	for name, want := range sums {
		got := groupByName(t, s, name)
		if got.Requests != want.Requests || got.Errors != want.Errors ||
			got.Series != want.Series || got.Samples != want.Samples {
			t.Errorf("%s: got %+v, want sums %+v", name, got, *want)
		}
		if math.Abs(got.Duration-want.Duration) > 1e-9 {
			t.Errorf("%s: duration %v, want sum %v", name, got.Duration, want.Duration)
		}
	}
	th := groupByName(t, s, "thanos")
	if th.Requests != 3 || th.Errors != 1 || th.Series != 6 || th.Samples != 60 {
		t.Errorf("thanos totals wrong: %+v", th)
	}
	if th.Duration <= 0 {
		t.Errorf("thanos duration = %v, want > 0", th.Duration)
	}
}

func TestDuration_IsPositive_AfterSleep(t *testing.T) {
	r := NewRecorder(testGroups())
	e := r.Begin(req(0, "q"))
	time.Sleep(5 * time.Millisecond)
	e.Finish(0, 0, nil)
	s := r.Stats()
	d := s.Requests[0].Duration
	if d < 0.004 {
		t.Errorf("request duration = %v, want >= 0.004", d)
	}
	if g := groupByName(t, s, "thanos"); math.Abs(g.Duration-d) > 1e-9 {
		t.Errorf("group duration = %v, want %v", g.Duration, d)
	}
}

func TestGroups_InOrdinalOrder_WithIdleGroupZeros(t *testing.T) {
	r := NewRecorder(testGroups())
	r.Begin(req(1, "q")).Finish(4, 8, nil)
	s := r.Stats()

	if len(s.Groups) != 3 {
		t.Fatalf("got %d groups, want 3", len(s.Groups))
	}
	for i, name := range []string{"thanos", "victoria", "idle"} {
		g := s.Groups[i]
		if g.Name != name || g.Ordinal != i {
			t.Errorf("groups[%d] = %s/%d, want %s/%d", i, g.Name, g.Ordinal, name, i)
		}
	}
	idle := s.Groups[2]
	if idle.Requests != 0 || idle.Errors != 0 || idle.Series != 0 || idle.Samples != 0 || idle.Duration != 0 {
		t.Errorf("idle group not zero: %+v", idle)
	}
	if !reflect.DeepEqual(idle.Labels, map[string]string{"backend": "idle"}) {
		t.Errorf("idle labels = %v", idle.Labels)
	}
	if s.Requests[0].Group != "victoria" {
		t.Errorf("request group = %q, want victoria", s.Requests[0].Group)
	}
}

func TestStats_ListsGroups_WhenNoRequests(t *testing.T) {
	s := NewRecorder(testGroups()).Stats()
	if len(s.Groups) != 3 || len(s.Requests) != 0 || s.RequestsTruncated != 0 {
		t.Errorf("unexpected stats: %+v", s)
	}
}

func TestFinish_RecordsError(t *testing.T) {
	r := NewRecorder(testGroups())
	r.Begin(req(0, "bad")).Finish(0, 0, errors.New("backend down"))
	r.Begin(req(0, "good")).Finish(1, 2, nil)
	s := r.Stats()

	bad, good := s.Requests[0], s.Requests[1]
	if bad.Status != StatusError || bad.Error != "backend down" {
		t.Errorf("bad = %+v", bad)
	}
	if good.Status != StatusSuccess || good.Error != "" {
		t.Errorf("good = %+v", good)
	}
	if g := groupByName(t, s, "thanos"); g.Errors != 1 || g.Requests != 2 {
		t.Errorf("group = %+v", g)
	}

	m := toMap(t, s)
	reqs := m["requests"].([]any)
	if _, ok := reqs[1].(map[string]any)["error"]; ok {
		t.Error("success request has an error key in JSON")
	}
	if got := reqs[0].(map[string]any)["error"]; got != "backend down" {
		t.Errorf("error json = %v", got)
	}
	if got := reqs[0].(map[string]any)["status"]; got != "error" {
		t.Errorf("status json = %v", got)
	}
}

func TestFinish_CountsOnce_WhenCalledTwice(t *testing.T) {
	r := NewRecorder(testGroups())
	e := r.Begin(req(0, "q"))
	e.Finish(3, 30, nil)
	e.Finish(7, 70, errors.New("late"))
	s := r.Stats()
	g := groupByName(t, s, "thanos")
	if g.Requests != 1 || g.Series != 3 || g.Samples != 30 || g.Errors != 0 {
		t.Errorf("group = %+v", g)
	}
	if rs := s.Requests[0]; rs.Status != StatusSuccess || rs.Series != 3 || rs.Samples != 30 {
		t.Errorf("request = %+v", rs)
	}
}

func TestRequestFields_RoundTrip(t *testing.T) {
	r := NewRecorder(testGroups())
	r.Begin(Request{
		GroupOrdinal: 0,
		Op:           "query_range",
		Path:         PathRaw,
		Query:        `up{job="x"}`,
		Start:        time.Unix(1760190000, 500_000_000),
		End:          time.Unix(1760193600, 0),
		Step:         14 * time.Second,
	}).Finish(1, 2, nil)
	r.Begin(Request{GroupOrdinal: 0, Op: "query", Query: "up", Start: time.Unix(5, 0), End: time.Unix(5, 0)}).Finish(0, 0, nil)

	s := r.Stats()
	a := s.Requests[0]
	if a.Op != "query_range" || a.Path != "raw" || a.Query != `up{job="x"}` {
		t.Errorf("request = %+v", a)
	}
	if a.Start != 1760190000.5 || a.End != 1760193600 || a.Step != 14 {
		t.Errorf("times = %v %v %v", a.Start, a.End, a.Step)
	}

	m := toMap(t, s)
	reqs := m["requests"].([]any)
	first := reqs[0].(map[string]any)
	for _, k := range []string{"group", "op", "path", "query", "start", "end", "step", "duration", "series", "samples", "status"} {
		if _, ok := first[k]; !ok {
			t.Errorf("first request missing key %q", k)
		}
	}
	second := reqs[1].(map[string]any)
	if _, ok := second["step"]; ok {
		t.Error("step present in JSON when zero")
	}
	if _, ok := second["path"]; ok {
		t.Error("path present in JSON when empty")
	}
	if second["start"] != float64(5) || second["end"] != float64(5) {
		t.Errorf("instant start/end = %v/%v", second["start"], second["end"])
	}
}

func TestStatsJSON_FieldNames(t *testing.T) {
	r := NewRecorder(testGroups())
	r.Begin(req(0, "q")).Finish(1, 2, nil)
	m := toMap(t, r.Stats())

	for _, k := range []string{"groups", "requests", "requestsTruncated"} {
		if _, ok := m[k]; !ok {
			t.Errorf("top-level key %q missing", k)
		}
	}
	groups := m["groups"].([]any)
	g := groups[0].(map[string]any)
	for _, k := range []string{"name", "ordinal", "labels", "requests", "errors", "series", "samples", "duration"} {
		if _, ok := g[k]; !ok {
			t.Errorf("group key %q missing", k)
		}
	}
	if g["name"] != "thanos" || g["ordinal"] != float64(0) {
		t.Errorf("group = %v", g)
	}
	if l, _ := g["labels"].(map[string]any); l["backend"] != "thanos" {
		t.Errorf("labels = %v", g["labels"])
	}
	if m["requestsTruncated"] != float64(0) {
		t.Errorf("requestsTruncated = %v", m["requestsTruncated"])
	}
}

func TestRecorder_ConcurrentRecording_TotalsExact(t *testing.T) {
	const workers, perWorker = 16, 20
	r := NewRecorder(testGroups())
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				e := r.Begin(req(w%2, fmt.Sprintf("w%d-%d", w, i)))
				var err error
				if i%5 == 0 {
					err = errors.New("x")
				}
				e.Finish(1, 3, err)
				_ = r.Stats()
			}
		}(w)
	}
	wg.Wait()

	s := r.Stats()
	total := workers * perWorker
	var reqs, errs int
	var series, samples int64
	for _, g := range s.Groups {
		reqs += g.Requests
		errs += g.Errors
		series += g.Series
		samples += g.Samples
	}
	if reqs != total || series != int64(total) || samples != int64(3*total) || errs != workers*(perWorker/5) {
		t.Errorf("totals reqs=%d errs=%d series=%d samples=%d", reqs, errs, series, samples)
	}
	if len(s.Requests) != MaxRequests || s.RequestsTruncated != total-MaxRequests {
		t.Errorf("len=%d truncated=%d", len(s.Requests), s.RequestsTruncated)
	}
}

func TestRecorders_Isolated_WhenUsedConcurrently(t *testing.T) {
	a, b := NewRecorder(testGroups()), NewRecorder(testGroups())
	var wg sync.WaitGroup
	for _, tc := range []struct {
		r      *Recorder
		prefix string
	}{{a, "a"}, {b, "b"}} {
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(r *Recorder, prefix string) {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					r.Begin(req(0, prefix)).Finish(1, 1, nil)
				}
			}(tc.r, tc.prefix)
		}
	}
	wg.Wait()

	for _, tc := range []struct {
		r      *Recorder
		prefix string
	}{{a, "a"}, {b, "b"}} {
		s := tc.r.Stats()
		if len(s.Requests) != 40 {
			t.Errorf("%s: got %d requests, want 40", tc.prefix, len(s.Requests))
		}
		for _, rs := range s.Requests {
			if rs.Query != tc.prefix {
				t.Fatalf("recorder %s saw entry %q", tc.prefix, rs.Query)
			}
		}
	}
}

func TestFromContext_Nil_WhenNoRecorder(t *testing.T) {
	if r := FromContext(context.Background()); r != nil {
		t.Errorf("FromContext = %v, want nil", r)
	}
}

func TestContext_RecorderRoundTrip(t *testing.T) {
	r := NewRecorder(nil)
	if got := FromContext(NewContext(context.Background(), r)); got != r {
		t.Errorf("FromContext = %p, want %p", got, r)
	}
}

func TestContext_PathRoundTrip(t *testing.T) {
	if got := PathFromContext(context.Background()); got != "" {
		t.Errorf("default path = %q, want empty", got)
	}
	ctx := WithPath(context.Background(), PathRaw)
	if got := PathFromContext(ctx); got != PathRaw {
		t.Errorf("path = %q, want %q", got, PathRaw)
	}
	if got := PathFromContext(WithPath(ctx, PathPushdown)); got != PathPushdown {
		t.Errorf("overridden path = %q, want %q", got, PathPushdown)
	}
}
