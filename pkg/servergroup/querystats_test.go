package servergroup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/pvlltvk/proxeus/pkg/querystats"
)

const queryStatsOrdinal = 3

func newQueryStatsGroup(t *testing.T, backend http.HandlerFunc, tweak func(*Config)) *ServerGroup {
	t.Helper()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)

	sg, err := NewServerGroup()
	if err != nil {
		t.Fatalf("failed to create servergroup: %v", err)
	}
	t.Cleanup(sg.Cancel)

	cfg := &Config{
		Ordinal:    queryStatsOrdinal,
		Scheme:     "http",
		HTTPConfig: HTTPClientConfig{DialTimeout: 200 * time.Millisecond},
	}
	tweak(cfg)
	if err := sg.ApplyConfig(cfg); err != nil {
		t.Fatalf("failed to apply config: %v", err)
	}
	host := strings.TrimPrefix(server.URL, "http://")
	if err := sg.loadTargetGroupMap(map[string][]*targetgroup.Group{
		"x": {{Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(host)}}}},
	}); err != nil {
		t.Fatalf("loadTargetGroupMap: %v", err)
	}
	return sg
}

func queryStatsRecorderCtx() (context.Context, *querystats.Recorder) {
	rec := querystats.NewRecorder([]querystats.Group{
		{Name: "other", Ordinal: 0},
		{Name: "under-test", Ordinal: queryStatsOrdinal},
	})
	return querystats.WithPath(querystats.NewContext(context.Background(), rec), querystats.PathPushdown), rec
}

func queryStatsGroup(t *testing.T, st querystats.Stats) querystats.GroupStats {
	t.Helper()
	for _, g := range st.Groups {
		if g.Ordinal == queryStatsOrdinal {
			return g
		}
	}
	t.Fatalf("no group with ordinal %d in %+v", queryStatsOrdinal, st.Groups)
	return querystats.GroupStats{}
}

func TestQueryStatsServerGroup_IgnoredErrorIsStillRecorded(t *testing.T) {
	failing := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}
	r := v1.Range{Start: time.Unix(0, 0), End: time.Unix(60, 0), Step: 15 * time.Second}

	t.Run("control_without_ignore_error_fails", func(t *testing.T) {
		sg := newQueryStatsGroup(t, failing, func(*Config) {})
		ss := sg.QueryRange(context.Background(), "up", r)
		for ss.Next() {
		}
		if ss.Err() == nil {
			t.Fatal("test setup: the failing backend should make the group return an error")
		}
	})

	t.Run("ignore_error", func(t *testing.T) {
		sg := newQueryStatsGroup(t, failing, func(c *Config) { c.IgnoreError = true })
		ctx, rec := queryStatsRecorderCtx()

		ss := sg.QueryRange(ctx, "up", r)
		for ss.Next() {
		}
		if err := ss.Err(); err != nil {
			t.Fatalf("ignore_error group leaked the error: %v", err)
		}

		st := rec.Stats()
		g := queryStatsGroup(t, st)
		if g.Errors != 1 || g.Requests != 1 {
			t.Fatalf("group: %+v, want requests=1 errors=1", g)
		}
		if other := st.Groups[0]; other.Requests != 0 {
			t.Fatalf("another group was charged: %+v", other)
		}
		if len(st.Requests) != 1 || st.Requests[0].Status != querystats.StatusError || st.Requests[0].Error == "" {
			t.Fatalf("requests: %+v", st.Requests)
		}
	})

	t.Run("downgrade_error", func(t *testing.T) {
		sg := newQueryStatsGroup(t, failing, func(c *Config) { c.DowngradeError = true })
		ctx, rec := queryStatsRecorderCtx()

		ss := sg.Query(ctx, "up", time.Unix(60, 0))
		for ss.Next() {
		}
		if err := ss.Err(); err != nil {
			t.Fatalf("downgrade_error group leaked the error: %v", err)
		}
		if g := queryStatsGroup(t, rec.Stats()); g.Errors != 1 {
			t.Fatalf("group: %+v, want errors=1", g)
		}
	})
}

func TestQueryStatsServerGroup_HealthyBackendCountsSeriesAndSamples(t *testing.T) {
	healthy := func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[` +
			`{"metric":{"__name__":"up","i":"1"},"values":[[0,"1"],[15,"1"],[30,"1"]]},` +
			`{"metric":{"__name__":"up","i":"2"},"values":[[0,"1"],[15,"1"],[30,"1"]]}]}}`))
	}
	sg := newQueryStatsGroup(t, healthy, func(c *Config) { c.IgnoreError = true })
	ctx, rec := queryStatsRecorderCtx()

	ss := sg.QueryRange(ctx, "up", v1.Range{Start: time.Unix(0, 0), End: time.Unix(30, 0), Step: 15 * time.Second})
	n := 0
	for ss.Next() {
		n++
	}
	if ss.Err() != nil || n != 2 {
		t.Fatalf("query through the group: %d series, err %v", n, ss.Err())
	}

	st := rec.Stats()
	g := queryStatsGroup(t, st)
	if g.Requests != 1 || g.Errors != 0 || g.Series != 2 || g.Samples != 6 {
		t.Fatalf("group: %+v, want requests=1 errors=0 series=2 samples=6", g)
	}
	if len(st.Requests) != 1 || st.Requests[0].Op != "query_range" || st.Requests[0].Query != "up" || st.Requests[0].Step != 15 {
		t.Fatalf("requests: %+v", st.Requests)
	}
}
