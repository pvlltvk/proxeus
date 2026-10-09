package promclient

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

type timeFilterTestCase struct {
	validTimes   []time.Time
	invalidTimes []time.Time

	validRanges   []v1.Range
	invalidRanges []v1.Range
}

func timefilterTest(t *testing.T, api API, testCase timeFilterTestCase) {
	for i, validTime := range testCase.validTimes {
		t.Run(fmt.Sprintf("validTime_%d", i), func(t *testing.T) {
			t.Run("query", func(t *testing.T) {
				if err := api.Query(context.TODO(), "", validTime).Err(); err == nil {
					t.Fatalf("Missing call to API")
				}
			})
		})
	}
	for i, invalidTime := range testCase.invalidTimes {
		t.Run(fmt.Sprintf("invalidTime_%d", i), func(t *testing.T) {
			t.Run("query", func(t *testing.T) {
				if err := api.Query(context.TODO(), "", invalidTime).Err(); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})
		})
	}

	for i, r := range testCase.validRanges {
		t.Run(fmt.Sprintf("validRange_%d", i), func(t *testing.T) {
			t.Run("label_names", func(t *testing.T) {
				if _, _, err := api.LabelNames(context.TODO(), []string{"a"}, r.Start, r.End); err == nil {
					t.Fatalf("Missing call to API")
				}
			})

			t.Run("label_values", func(t *testing.T) {
				if _, _, err := api.LabelValues(context.TODO(), "__name__", []string{"a"}, r.Start, r.End); err == nil {
					t.Fatalf("Missing call to API")
				}
			})

			t.Run("query_range", func(t *testing.T) {
				if err := api.QueryRange(context.TODO(), "", v1.Range{Start: r.Start, End: r.End}).Err(); err == nil {
					t.Fatalf("Missing call to API")
				}
			})
			t.Run("series", func(t *testing.T) {
				if _, _, err := api.Series(context.TODO(), nil, r.Start, r.End); err == nil {
					t.Fatalf("Missing call to API")
				}
			})
			t.Run("getvalue", func(t *testing.T) {
				if err := api.GetValue(context.TODO(), r.Start, r.End, nil).Err(); err == nil {
					t.Fatalf("Missing call to API")
				}
			})
		})
	}
	for i, r := range testCase.invalidRanges {
		t.Run(fmt.Sprintf("invalidRange_%d", i), func(t *testing.T) {
			t.Run("label_names", func(t *testing.T) {
				if _, _, err := api.LabelNames(context.TODO(), []string{"a"}, r.Start, r.End); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})

			t.Run("label_values", func(t *testing.T) {
				if _, _, err := api.LabelValues(context.TODO(), "__name__", []string{"a"}, r.Start, r.End); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})

			t.Run("query_range", func(t *testing.T) {
				if err := api.QueryRange(context.TODO(), "", v1.Range{Start: r.Start, End: r.End}).Err(); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})
			t.Run("series", func(t *testing.T) {
				if _, _, err := api.Series(context.TODO(), nil, r.Start, r.End); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})
			t.Run("getvalue", func(t *testing.T) {
				if err := api.GetValue(context.TODO(), r.Start, r.End, nil).Err(); err != nil {
					t.Fatalf("Unexpected call to API")
				}
			})
		})
	}
}

func TestAbsoluteTimeFilter(t *testing.T) {
	now := time.Now()

	start := now.Add(time.Hour * -2)
	end := now.Add(time.Hour * -1)

	t.Run(fmt.Sprintf("start=%s end=%s", start, end), func(t *testing.T) {
		api := &AbsoluteTimeFilter{
			API:   &recoverAPI{nil},
			Start: start,
			End:   end,
		}
		timefilterTest(t, api, timeFilterTestCase{
			validTimes: []time.Time{
				start,
				end,
				start.Add(time.Minute),
			},
			invalidTimes: []time.Time{
				now,
				start.Add(time.Minute * -1),
			},
			validRanges: []v1.Range{
				{Start: start, End: end},
				{Start: start.Add(time.Hour * -1), End: end},
				{Start: start, End: end.Add(time.Hour)},
			},
			invalidRanges: []v1.Range{
				{Start: now, End: now},
				{Start: start.Add(time.Hour * -10), End: end.Add(time.Hour * -9)},
			},
		})
	})

	end = time.Time{}

	t.Run(fmt.Sprintf("start=%s end=%s", start, end), func(t *testing.T) {
		api := &AbsoluteTimeFilter{
			API:   &recoverAPI{nil},
			Start: start,
			End:   end,
		}
		timefilterTest(t, api, timeFilterTestCase{
			validTimes: []time.Time{
				start,
				start.Add(time.Minute),
				now,
			},
			invalidTimes: []time.Time{
				start.Add(time.Minute * -1),
			},
			validRanges: []v1.Range{
				{Start: start, End: now},
				{Start: start.Add(time.Hour * -1), End: now},
				{Start: start, End: now.Add(time.Hour)},
				{Start: now, End: now},
			},
			invalidRanges: []v1.Range{
				{Start: start.Add(time.Hour * -10), End: now.Add(time.Hour * -9)},
			},
		})
	})
}

func TestRelativeTimeFilter(t *testing.T) {
	now := time.Now()

	startOffset := time.Hour * -2
	endOffset := time.Hour * -1

	start := now.Add(startOffset)
	end := now.Add(endOffset)

	api := &RelativeTimeFilter{
		API:   &recoverAPI{nil},
		Start: &startOffset,
		End:   &endOffset,
	}
	timefilterTest(t, api, timeFilterTestCase{
		validTimes: []time.Time{
			start.Add(time.Minute),
			end,
		},
		invalidTimes: []time.Time{
			now,
			start.Add(time.Minute * -1),
		},
		validRanges: []v1.Range{
			{Start: start, End: end},
			{Start: start.Add(time.Hour * -1), End: end},
			{Start: start, End: end.Add(time.Hour)},
		},
		invalidRanges: []v1.Range{
			{Start: now, End: now},
			{Start: start.Add(time.Hour * -10), End: end.Add(time.Hour * -9)},
		},
	})

}

// For time truncation we need to ensure that any new range's start aligns with a multiple of the step from the overall query start
// otherwise we get into a LOT of trouble with LookbackDelta as the timestamps of the result won't align properly
func TestAbsoluteTimeFilterStepAlignment(t *testing.T) {
	var r v1.Range
	stub := &stubAPI{
		queryRange: func(_ string, rng v1.Range) model.Value {
			r = rng
			return nil
		},
	}
	filterStart, _ := time.Parse(time.RFC3339, "2024-07-01T00:00:00Z")
	api := &AbsoluteTimeFilter{
		API:      stub,
		Start:    filterStart,
		Truncate: true,
	}

	start := time.Unix(1719738435, 0)
	end := time.Unix(1719879274, 0)

	api.QueryRange(context.TODO(), "foo", v1.Range{
		Start: start,
		End:   end,
		Step:  563 * time.Second,
	})

	remainder := r.Start.Sub(start) % r.Step
	if remainder > 0 {
		t.Fatalf("unexpected step misalignment!")
	}
}

// For time truncation we need to ensure that any new range's start aligns with a multiple of the step from the overall query start
// otherwise we get into a LOT of trouble with LookbackDelta as the timestamps of the result won't align properly
func TestRelativeTimeFilterStepAlignment(t *testing.T) {
	var r v1.Range
	stub := &stubAPI{
		queryRange: func(_ string, rng v1.Range) model.Value {
			r = rng
			return nil
		},
	}
	dur, _ := time.ParseDuration("-2h")
	api := &RelativeTimeFilter{
		API:      stub,
		Start:    &dur,
		Truncate: true,
	}

	now := time.Now()
	start := now.Add(-1 * time.Hour * 24)
	end := now

	api.QueryRange(context.TODO(), "foo", v1.Range{
		Start: start,
		End:   end,
		Step:  563 * time.Second,
	})

	remainder := r.Start.Sub(start) % r.Step
	if remainder > 0 {
		t.Fatalf("unexpected step misalignment!")
	}
}

// windowRecorder remembers the window of the last call that reached it.
type windowRecorder struct {
	API
	start, end time.Time
}

func (w *windowRecorder) LabelNames(_ context.Context, _ []string, start, end time.Time) ([]string, v1.Warnings, error) {
	w.start, w.end = start, end
	return nil, nil, nil
}

func (w *windowRecorder) LabelValues(_ context.Context, _ string, _ []string, start, end time.Time) (model.LabelValues, v1.Warnings, error) {
	w.start, w.end = start, end
	return nil, nil, nil
}

func (w *windowRecorder) QueryRange(_ context.Context, _ string, r v1.Range) storage.SeriesSet {
	w.start, w.end = r.Start, r.End
	return storage.EmptySeriesSet()
}

func (w *windowRecorder) Series(_ context.Context, _ []string, start, end time.Time) ([]model.LabelSet, v1.Warnings, error) {
	w.start, w.end = start, end
	return nil, nil, nil
}

func (w *windowRecorder) GetValue(_ context.Context, start, end time.Time, _ []*labels.Matcher) storage.SeriesSet {
	w.start, w.end = start, end
	return storage.EmptySeriesSet()
}

func (w *windowRecorder) QueryExemplars(_ context.Context, _ string, start, end time.Time) ([]v1.ExemplarQueryResult, error) {
	w.start, w.end = start, end
	return nil, nil
}

// The filters clamp a request to the configured window and leave a bound that
// is already inside it, or not configured, alone. Relative windows are
// computed from time.Now(), so a clamped bound is compared with a tolerance.
func TestTimeFilterTruncateBounds(t *testing.T) {
	const tolerance = 10 * time.Second
	hour := time.Hour

	type request struct {
		name       string
		start, end time.Duration // relative to now
		// wantStart/wantEnd are offsets from now; the filter's own bounds
		// are expected where the request sticks out of the window.
		wantStart, wantEnd time.Duration
	}
	type window struct {
		name       string
		start, end *time.Duration // offsets from now
		requests   []request
	}
	windows := []window{
		{
			name:  "both bounds",
			start: ptr(-2 * hour), end: ptr(-hour),
			requests: []request{
				{name: "inside", start: -90 * time.Minute, end: -70 * time.Minute, wantStart: -90 * time.Minute, wantEnd: -70 * time.Minute},
				{name: "end past the window", start: -90 * time.Minute, end: 0, wantStart: -90 * time.Minute, wantEnd: -hour},
				{name: "start before the window", start: -5 * hour, end: -70 * time.Minute, wantStart: -2 * hour, wantEnd: -70 * time.Minute},
				{name: "covers the window", start: -5 * hour, end: 0, wantStart: -2 * hour, wantEnd: -hour},
			},
		},
		{
			name:  "start only",
			start: ptr(-2 * hour),
			requests: []request{
				{name: "start before the window", start: -5 * hour, end: 0, wantStart: -2 * hour, wantEnd: 0},
				{name: "inside", start: -hour, end: -time.Minute, wantStart: -hour, wantEnd: -time.Minute},
			},
		},
		{
			name: "end only",
			end:  ptr(-hour),
			requests: []request{
				{name: "end past the window", start: -5 * hour, end: 0, wantStart: -5 * hour, wantEnd: -hour},
				{name: "inside", start: -5 * hour, end: -2 * hour, wantStart: -5 * hour, wantEnd: -2 * hour},
			},
		},
	}

	calls := map[string]func(API, time.Time, time.Time){
		"label_names": func(a API, s, e time.Time) { _, _, _ = a.LabelNames(context.Background(), nil, s, e) },
		"label_values": func(a API, s, e time.Time) {
			_, _, _ = a.LabelValues(context.Background(), "l", nil, s, e)
		},
		"series":    func(a API, s, e time.Time) { _, _, _ = a.Series(context.Background(), nil, s, e) },
		"get_value": func(a API, s, e time.Time) { a.GetValue(context.Background(), s, e, nil) },
		"exemplars": func(a API, s, e time.Time) { _, _ = a.QueryExemplars(context.Background(), "q", s, e) },
	}

	for _, w := range windows {
		for _, kind := range []string{"absolute", "relative"} {
			for _, req := range w.requests {
				for method, call := range calls {
					t.Run(kind+"/"+w.name+"/"+req.name+"/"+method, func(t *testing.T) {
						now := time.Now()
						rec := &windowRecorder{}
						var api API
						if kind == "relative" {
							api = &RelativeTimeFilter{API: rec, Start: w.start, End: w.end, Truncate: true}
						} else {
							af := &AbsoluteTimeFilter{API: rec, Truncate: true}
							if w.start != nil {
								af.Start = now.Add(*w.start)
							}
							if w.end != nil {
								af.End = now.Add(*w.end)
							}
							api = af
						}

						call(api, now.Add(req.start), now.Add(req.end))

						if d := rec.start.Sub(now.Add(req.wantStart)).Abs(); d > tolerance {
							t.Errorf("forwarded start = now%+v, want now%+v", rec.start.Sub(now), req.wantStart)
						}
						if d := rec.end.Sub(now.Add(req.wantEnd)).Abs(); d > tolerance {
							t.Errorf("forwarded end = now%+v, want now%+v", rec.end.Sub(now), req.wantEnd)
						}
					})
				}
			}
		}
	}
}

func ptr(d time.Duration) *time.Duration { return &d }

// Aligning the start up to the step grid can move it past the clamped end; the
// backend rejects such a range, so the filter must answer empty instead.
func TestTimeFilterTruncateEmptiedByStepAlignment(t *testing.T) {
	now := time.Now()
	sec := time.Second
	newAPI := map[string]func(rec API) API{
		"absolute": func(rec API) API {
			return &AbsoluteTimeFilter{API: rec, Start: now.Add(5 * sec), End: now.Add(9 * sec), Truncate: true}
		},
		"relative": func(rec API) API {
			return &RelativeTimeFilter{API: rec, Start: ptr(5 * sec), End: ptr(9 * sec), Truncate: true}
		},
	}
	tests := []struct {
		name      string
		start     time.Time
		end       time.Time
		step      time.Duration
		wantEmpty bool
	}{
		{name: "aligned start lands past the end", start: now, end: now.Add(10 * sec), step: 10 * sec, wantEmpty: true},
		{name: "aligned start still inside", start: now, end: now.Add(10 * sec), step: 2 * sec},
	}

	for kind, build := range newAPI {
		for _, tt := range tests {
			t.Run(kind+"/"+tt.name, func(t *testing.T) {
				rec := &windowRecorder{}
				build(rec).QueryRange(context.Background(), "q", v1.Range{Start: tt.start, End: tt.end, Step: tt.step})

				reached := !rec.start.IsZero()
				if reached == tt.wantEmpty {
					t.Fatalf("backend reached = %v, want %v", reached, !tt.wantEmpty)
				}
				if reached && rec.start.After(rec.end) {
					t.Errorf("forwarded start %v is after end %v", rec.start, rec.end)
				}
			})
		}
	}
}
