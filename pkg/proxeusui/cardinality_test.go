package proxeusui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// tsdbBackend serves a recorded /api/v1/status/tsdb response and records the
// query string of every request it gets.
type tsdbBackend struct {
	*httptest.Server
	mu      sync.Mutex
	queries []url.Values
}

func newTSDBBackend(t *testing.T, status int, fixture string) *tsdbBackend {
	t.Helper()
	var body []byte
	if fixture != "" {
		var err error
		if body, err = os.ReadFile("testdata/" + fixture); err != nil {
			t.Fatal(err)
		}
	}
	b := &tsdbBackend{}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/api/v1/status/tsdb" {
			http.NotFound(w, r)
			return
		}
		b.mu.Lock()
		b.queries = append(b.queries, r.URL.Query())
		b.mu.Unlock()
		w.WriteHeader(status)
		w.Write(body) //nolint:errcheck
	}))
	t.Cleanup(b.Close)
	return b
}

func (b *tsdbBackend) base() string { return b.URL + "/prefix" }

func (b *tsdbBackend) requests() []url.Values {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.queries
}

// downURL is a base URL nothing listens on.
func downURL(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	u := srv.URL
	srv.Close()
	return u
}

func cardinalityClient() *http.Client { return &http.Client{Timeout: 2 * time.Second} }

func TestCardinality_NormalizesBothShapes(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	vm := newTSDBBackend(t, http.StatusOK, "tsdb_victoriametrics.json")

	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
		{Name: "victoria", BackendType: servergroup.BackendVictoriaMetrics, Client: cardinalityClient(), BaseURLs: []string{vm.base()}},
	}, cardinalityQuery{TopN: 3})

	if len(groups) != 2 || groups[0].Name != "thanos" || groups[1].Name != "victoria" {
		t.Fatalf("groups = %+v, want thanos and victoria in config order", groups)
	}

	th := groups[0]
	if th.Error != "" {
		t.Fatalf("thanos error: %s", th.Error)
	}
	if th.Target != thanos.base() {
		t.Errorf("thanos target = %q, want %q", th.Target, thanos.base())
	}
	if th.TotalSeries != 32433 || th.TotalLabelValuePairs != 2305 {
		t.Errorf("thanos totals = %d/%d, want 32433/2305", th.TotalSeries, th.TotalLabelValuePairs)
	}
	if th.Coverage == nil || th.Coverage.Kind != "head" || th.Coverage.MinTime != 1792317602044 || th.Coverage.MaxTime != 1792331810490 {
		t.Errorf("thanos coverage = %+v, want head 1792317602044..1792331810490", th.Coverage)
	}
	if want := (CardinalityStat{Name: "avalanche_gauge_metric_mmmmm_0_1", Value: 375}); len(th.SeriesCountByMetricName) != 3 || th.SeriesCountByMetricName[0] != want {
		t.Errorf("thanos seriesCountByMetricName = %+v", th.SeriesCountByMetricName)
	}
	if len(th.LabelValueCountByLabelName) != 3 || len(th.SeriesCountByLabelValuePair) != 3 || len(th.MemoryInBytesByLabelName) != 3 {
		t.Errorf("thanos lists = %d/%d/%d, want 3 each", len(th.LabelValueCountByLabelName), len(th.SeriesCountByLabelValuePair), len(th.MemoryInBytesByLabelName))
	}
	if th.SeriesCountByLabelName != nil || th.SeriesCountByFocusLabelValue != nil {
		t.Errorf("thanos has VictoriaMetrics-only lists: %+v / %+v", th.SeriesCountByLabelName, th.SeriesCountByFocusLabelValue)
	}

	v := groups[1]
	if v.Error != "" {
		t.Fatalf("victoria error: %s", v.Error)
	}
	if v.TotalSeries != 72498 || v.TotalLabelValuePairs != 1103834 {
		t.Errorf("victoria totals = %d/%d, want 72498/1103834", v.TotalSeries, v.TotalLabelValuePairs)
	}
	today := time.Now().UTC().Format(time.DateOnly)
	if v.Coverage == nil || v.Coverage.Kind != "day" || v.Coverage.Date != today {
		t.Errorf("victoria coverage = %+v, want day %s", v.Coverage, today)
	}
	if want := (CardinalityStat{Name: "flag", Value: 2901}); len(v.SeriesCountByMetricName) != 3 || v.SeriesCountByMetricName[0] != want {
		t.Errorf("victoria seriesCountByMetricName = %+v", v.SeriesCountByMetricName)
	}
	if len(v.SeriesCountByLabelName) != 3 || len(v.SeriesCountByFocusLabelValue) != 3 || len(v.LabelValueCountByLabelName) != 3 {
		t.Errorf("victoria lists = %d/%d/%d, want 3 each", len(v.SeriesCountByLabelName), len(v.SeriesCountByFocusLabelValue), len(v.LabelValueCountByLabelName))
	}
	if v.MemoryInBytesByLabelName != nil {
		t.Errorf("victoria has memoryInBytesByLabelName: %+v", v.MemoryInBytesByLabelName)
	}
}

func TestCardinality_ShapeFromBodyNotConfig(t *testing.T) {
	vm := newTSDBBackend(t, http.StatusOK, "tsdb_victoriametrics.json")
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "undeclared", Client: cardinalityClient(), BaseURLs: []string{vm.base()}},
	}, cardinalityQuery{TopN: 10})
	if len(groups) != 1 || groups[0].Error != "" || groups[0].TotalSeries != 72498 || groups[0].Coverage == nil || groups[0].Coverage.Kind != "day" {
		t.Fatalf("groups = %+v, want the VictoriaMetrics body parsed", groups)
	}
}

func TestCardinality_FailingGroupDoesNotBreakOthers(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "down", BackendType: servergroup.BackendVictoriaMetrics, Client: cardinalityClient(), BaseURLs: []string{downURL(t), downURL(t)}},
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
	}, cardinalityQuery{TopN: 10})
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(groups))
	}
	if groups[0].Error == "" || groups[0].TotalSeries != 0 || groups[0].SeriesCountByMetricName != nil {
		t.Errorf("down group = %+v, want an error and no data", groups[0])
	}
	if groups[1].Error != "" || groups[1].TotalSeries != 32433 {
		t.Errorf("thanos group = %+v, want full data", groups[1])
	}
}

func TestCardinality_NoTargets(t *testing.T) {
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "empty", Client: cardinalityClient()},
	}, cardinalityQuery{TopN: 10})
	if len(groups) != 1 || groups[0].Error == "" {
		t.Fatalf("groups = %+v, want an error for a group without targets", groups)
	}
}

func TestCardinality_FirstWorkingReplica(t *testing.T) {
	broken := newTSDBBackend(t, http.StatusInternalServerError, "")
	garbage := newTSDBBackend(t, http.StatusOK, "")
	good := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{broken.base(), garbage.base(), good.base()}},
	}, cardinalityQuery{TopN: 10})
	if len(groups) != 1 || groups[0].Error != "" || groups[0].TotalSeries != 32433 {
		t.Fatalf("groups = %+v, want data from the third target", groups)
	}
	if groups[0].Target != good.base() {
		t.Errorf("target = %q, want %q", groups[0].Target, good.base())
	}
	if len(broken.requests()) != 1 || len(garbage.requests()) != 1 {
		t.Errorf("requests broken/garbage = %d/%d, want 1/1", len(broken.requests()), len(garbage.requests()))
	}
}

func TestCardinality_StopsAtFirstWorkingReplica(t *testing.T) {
	first := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	second := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{first.base(), second.base()}},
	}, cardinalityQuery{TopN: 10})
	if len(first.requests()) != 1 || len(second.requests()) != 0 {
		t.Errorf("requests first/second = %d/%d, want 1/0", len(first.requests()), len(second.requests()))
	}
}

func TestCardinality_FilterRouting(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	vm := newTSDBBackend(t, http.StatusOK, "tsdb_victoriametrics.json")
	undeclared := newTSDBBackend(t, http.StatusOK, "tsdb_victoriametrics.json")
	q := cardinalityQuery{TopN: 3, Match: []string{`up`, `{job="a"}`}, FocusLabel: "job", Date: "2026-10-17"}

	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
		{Name: "victoria", BackendType: servergroup.BackendVictoriaMetrics, Client: cardinalityClient(), BaseURLs: []string{vm.base()}},
		{Name: "undeclared", Client: cardinalityClient(), BaseURLs: []string{undeclared.base()}},
	}, q)

	wantThanos := url.Values{"limit": {"3"}}
	if got := thanos.requests(); len(got) != 1 || !reflect.DeepEqual(got[0], wantThanos) {
		t.Errorf("thanos query = %v, want %v", got, wantThanos)
	}
	wantVM := url.Values{"topN": {"3"}, "match[]": {`up`, `{job="a"}`}, "focusLabel": {"job"}, "date": {"2026-10-17"}}
	if got := vm.requests(); len(got) != 1 || !reflect.DeepEqual(got[0], wantVM) {
		t.Errorf("victoria query = %v, want %v", got, wantVM)
	}
	wantUndeclared := url.Values{"limit": {"3"}, "topN": {"3"}}
	if got := undeclared.requests(); len(got) != 1 || !reflect.DeepEqual(got[0], wantUndeclared) {
		t.Errorf("undeclared query = %v, want %v", got, wantUndeclared)
	}

	wantNotApplied := []string{"match[]", "focusLabel", "date"}
	if !reflect.DeepEqual(groups[0].NotApplied, wantNotApplied) {
		t.Errorf("thanos notApplied = %v, want %v", groups[0].NotApplied, wantNotApplied)
	}
	if groups[1].NotApplied != nil {
		t.Errorf("victoria notApplied = %v, want none", groups[1].NotApplied)
	}
	if !reflect.DeepEqual(groups[2].NotApplied, wantNotApplied) {
		t.Errorf("undeclared notApplied = %v, want %v", groups[2].NotApplied, wantNotApplied)
	}
	if groups[1].Coverage == nil || groups[1].Coverage.Date != "2026-10-17" {
		t.Errorf("victoria coverage = %+v, want date 2026-10-17", groups[1].Coverage)
	}
}

func TestCardinality_OnlySetFiltersReported(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
	}, cardinalityQuery{TopN: 10, FocusLabel: "job"})
	if want := []string{"focusLabel"}; !reflect.DeepEqual(groups[0].NotApplied, want) {
		t.Errorf("notApplied = %v, want %v", groups[0].NotApplied, want)
	}
}

func TestCardinality_PartialPassedThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","isPartial":true,"data":{"totalSeries":5,"totalLabelValuePairs":7,"seriesCountByMetricName":[{"name":"up","value":5}]}}`)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "victoria", BackendType: servergroup.BackendVictoriaMetrics, Client: cardinalityClient(), BaseURLs: []string{srv.URL}},
	}, cardinalityQuery{TopN: 10})
	if !groups[0].Partial || groups[0].TotalSeries != 5 {
		t.Errorf("group = %+v, want partial with 5 series", groups[0])
	}
}

func TestCardinality_ErrorStatusInBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"boom"}`)) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)
	groups := collectCardinality(context.Background(), []cardinalityBackend{
		{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{srv.URL}},
	}, cardinalityQuery{TopN: 10})
	if groups[0].Error == "" {
		t.Errorf("group = %+v, want an error", groups[0])
	}
}
