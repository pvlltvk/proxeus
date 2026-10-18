package proxeusui

import (
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// newHandlerForTest builds a Handler with a fixed Inventory injected via
// inventoryFn, decoupled from ProxyStorage entirely.
func newHandlerForTest(inv Inventory) *Handler {
	tmpl := template.Must(template.New("inventory.html").Parse(
		`<!DOCTYPE html><html><body>proxeus test {{.GeneratedAt}}</body></html>`,
	))
	return &Handler{
		tmpl:        tmpl,
		inventoryFn: func() Inventory { return inv },
	}
}

func TestHandlerBackendsJSON_Envelope(t *testing.T) {
	now := time.Date(2026, 5, 23, 14, 0, 0, 0, time.UTC)
	inv := Inventory{
		GeneratedAt: now,
		Groups: []GroupInfo{
			{
				Name:    "thanos",
				Ordinal: 0,
				Labels:  map[string]string{"backend": "thanos"},
				Targets: []TargetInfo{
					{
						URL: "http://thanos-query-frontend:10901",
						ProbeResult: ProbeResult{
							Healthy:     true,
							LastError:   "",
							LastProbeAt: now,
							Version:     "0.36.0",
						},
						BackendType: servergroup.BackendThanos,
					},
				},
			},
		},
	}

	h := newHandlerForTest(inv)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/proxeus/api/backends.json", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("expected application/json Content-Type, got %q", ct)
	}

	var envelope Inventory
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if envelope.GeneratedAt.IsZero() {
		t.Error("generatedAt must not be zero")
	}
	if len(envelope.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(envelope.Groups))
	}
	g := envelope.Groups[0]
	if g.Name != "thanos" {
		t.Errorf("expected group name 'thanos', got %q", g.Name)
	}
	if g.Ordinal != 0 {
		t.Errorf("expected ordinal 0, got %d", g.Ordinal)
	}
	if len(g.Targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(g.Targets))
	}
	tgt := g.Targets[0]
	if tgt.URL != "http://thanos-query-frontend:10901" {
		t.Errorf("unexpected target URL %q", tgt.URL)
	}
	if !tgt.Healthy {
		t.Error("expected target to be healthy")
	}
	if tgt.BackendType != servergroup.BackendThanos {
		t.Errorf("expected backendType 'thanos', got %q", tgt.BackendType)
	}
	if tgt.Version != "0.36.0" {
		t.Errorf("expected version '0.36.0', got %q", tgt.Version)
	}
}

func TestHandlerBackends_Returns200HTML(t *testing.T) {
	inv := Inventory{GeneratedAt: time.Now()}
	h := newHandlerForTest(inv)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/proxeus/backends", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("expected text/html Content-Type, got %q", ct)
	}
	if !strings.Contains(w.Body.String(), "<!DOCTYPE html>") {
		t.Errorf("response body missing DOCTYPE: %q", w.Body.String())
	}
}

func TestHandlerRoot_RedirectsToBackends(t *testing.T) {
	inv := Inventory{GeneratedAt: time.Now()}
	h := newHandlerForTest(inv)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/proxeus/", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc != "/proxeus/backends" {
		t.Errorf("expected redirect to /proxeus/backends, got %q", loc)
	}
}

func TestHandlerRedirect_HonorsRoutePrefix(t *testing.T) {
	cases := []struct {
		name        string
		routePrefix string
		reqPath     string
		wantLoc     string
	}{
		{name: "root prefix", routePrefix: "", reqPath: "/proxeus/", wantLoc: "/proxeus/backends"},
		{name: "root prefix slash", routePrefix: "/", reqPath: "/proxeus/", wantLoc: "/proxeus/backends"},
		{name: "sub-path prefix", routePrefix: "/foo", reqPath: "/foo/proxeus/", wantLoc: "/foo/proxeus/backends"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHandlerForTest(Inventory{GeneratedAt: time.Now()})
			h.routePrefix = tc.routePrefix

			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, tc.reqPath, nil)
			h.ServeHTTP(w, r)

			if w.Code != http.StatusFound {
				t.Fatalf("expected 302, got %d", w.Code)
			}
			if loc := w.Header().Get("Location"); loc != tc.wantLoc {
				t.Errorf("expected redirect to %q, got %q", tc.wantLoc, loc)
			}
		})
	}
}

func TestHandlerBackendsJSON_EmptyGroups(t *testing.T) {
	inv := Inventory{
		GeneratedAt: time.Now(),
		Groups:      []GroupInfo{},
	}
	h := newHandlerForTest(inv)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/proxeus/api/backends.json", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var envelope Inventory
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(envelope.Groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(envelope.Groups))
	}
}

func newCardinalityHandlerForTest(backends ...cardinalityBackend) *Handler {
	h := newHandlerForTest(Inventory{GeneratedAt: time.Now()})
	h.cardinalityBackendsFn = func() []cardinalityBackend { return backends }
	return h
}

func TestHandlerCardinalityJSON(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	vm := newTSDBBackend(t, http.StatusOK, "tsdb_victoriametrics.json")
	h := newCardinalityHandlerForTest(
		cardinalityBackend{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
		cardinalityBackend{Name: "victoria", BackendType: servergroup.BackendVictoriaMetrics, Client: cardinalityClient(), BaseURLs: []string{vm.base()}},
	)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/proxeus/api/cardinality.json?topN=3&match[]=up&match[]=%7Bjob%3D%22a%22%7D&focusLabel=job&date=2026-10-17", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var resp struct {
		Groups []CardinalityGroup `json:"groups"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Groups) != 2 || resp.Groups[0].TotalSeries != 32433 || resp.Groups[1].TotalSeries != 72498 {
		t.Fatalf("groups = %+v", resp.Groups)
	}
	got := vm.requests()
	if len(got) != 1 || got[0].Get("topN") != "3" || strings.Join(got[0]["match[]"], ",") != `up,{job="a"}` || got[0].Get("focusLabel") != "job" || got[0].Get("date") != "2026-10-17" {
		t.Errorf("victoria query = %v", got)
	}
}

func TestHandlerCardinalityJSON_DefaultTopN(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	h := newCardinalityHandlerForTest(
		cardinalityBackend{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
	)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/proxeus/api/cardinality.json", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := thanos.requests(); len(got) != 1 || got[0].Get("limit") != "10" {
		t.Errorf("thanos query = %v, want limit=10", got)
	}
}

func TestHandlerCardinalityJSON_InvalidInput(t *testing.T) {
	for _, q := range []string{"topN=0", "topN=-1", "topN=abc", "topN=1001", "topN=5000", "date=2026-13-01", "date=yesterday", "date=2026-10-17T00:00:00Z"} {
		t.Run(q, func(t *testing.T) {
			h := newCardinalityHandlerForTest()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/proxeus/api/cardinality.json?"+q, nil))
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestHandlerCardinalityJSON_RoutePrefix(t *testing.T) {
	thanos := newTSDBBackend(t, http.StatusOK, "tsdb_thanos.json")
	h := newCardinalityHandlerForTest(
		cardinalityBackend{Name: "thanos", BackendType: servergroup.BackendThanos, Client: cardinalityClient(), BaseURLs: []string{thanos.base()}},
	)
	h.routePrefix = "/foo"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/foo/proxeus/api/cardinality.json", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"groups"`) {
		t.Fatalf("status = %d body %s, want 200 with groups", w.Code, w.Body.String())
	}
}
