package promclient

import (
	"context"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
	"github.com/pvlltvk/proxeus/pkg/querystats"
)

// QueryStatsAPI records each data call to one server group into the
// querystats.Recorder of the call's context, if there is one: what was asked,
// how long it took, and how many series and samples came back. Without a
// recorder every call is forwarded untouched.
type QueryStatsAPI struct {
	API
	// Ordinal is the server group's ordinal, as in querystats.Group.
	Ordinal int
}

// Query performs a query for the given time.
func (a *QueryStatsAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	rec := querystats.FromContext(ctx)
	if rec == nil {
		return a.API.Query(ctx, query, ts)
	}
	e := rec.Begin(a.request(ctx, "query", query, ts, ts, 0))
	return finish(e, a.API.Query(ctx, query, ts))
}

// QueryRange performs a query for the given range.
func (a *QueryStatsAPI) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	rec := querystats.FromContext(ctx)
	if rec == nil {
		return a.API.QueryRange(ctx, query, r)
	}
	e := rec.Begin(a.request(ctx, "query_range", query, r.Start, r.End, r.Step))
	return finish(e, a.API.QueryRange(ctx, query, r))
}

// GetValue loads the raw data for a given set of matchers in the time range
func (a *QueryStatsAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	rec := querystats.FromContext(ctx)
	if rec == nil {
		return a.API.GetValue(ctx, start, end, matchers)
	}
	// The selector is only for display; a matcher it can't render shouldn't
	// fail the fetch.
	selector, _ := promhttputil.MatcherToString(matchers)
	e := rec.Begin(a.request(ctx, "get_value", selector, start, end, 0))
	return finish(e, a.API.GetValue(ctx, start, end, matchers))
}

func (a *QueryStatsAPI) request(ctx context.Context, op, query string, start, end time.Time, step time.Duration) querystats.Request {
	return querystats.Request{
		GroupOrdinal: a.Ordinal,
		Op:           op,
		Path:         querystats.PathFromContext(ctx),
		Query:        query,
		Start:        start,
		End:          end,
		Step:         step,
	}
}

// finish drains ss so the counts cover everything the group returned, even if
// the engine stops reading early, and hands back the same series. The
// backends' responses are already decoded in memory by the time a call
// returns, so this keeps only the series slice on top of them.
func finish(e *querystats.Entry, ss storage.SeriesSet) storage.SeriesSet {
	var (
		series  []storage.Series
		samples int64
		it      chunkenc.Iterator
	)
	for ss.Next() {
		s := ss.At()
		series = append(series, s)
		it = s.Iterator(it)
		for it.Next() != chunkenc.ValNone {
			samples++
		}
	}
	e.Finish(int64(len(series)), samples, ss.Err())
	return promapi.NewSeriesSet(series, ss.Warnings(), ss.Err())
}
