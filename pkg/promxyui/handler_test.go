package promxyui

import (
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newHandlerForTest builds a Handler with a fixed Inventory injected via
// inventoryFn, decoupled from ProxyStorage entirely.
func newHandlerForTest(inv Inventory) *Handler {
	tmpl := template.Must(template.New("inventory.html").Parse(
		`<!DOCTYPE html><html><body>promxy test {{.GeneratedAt}}</body></html>`,
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
						URL:         "http://thanos-query-frontend:10901",
						Healthy:     true,
						LastError:   "",
						LastProbeAt: now,
						BackendType: BackendThanos,
						Version:     "0.36.0",
					},
				},
			},
		},
	}

	h := newHandlerForTest(inv)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/promxy/api/backends.json", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("expected application/json Content-Type, got %q", ct)
	}

	var envelope jsonResponse
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
	if tgt.BackendType != BackendThanos {
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
	r := httptest.NewRequest(http.MethodGet, "/promxy/backends", nil)
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
	r := httptest.NewRequest(http.MethodGet, "/promxy/", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", w.Code)
	}
	loc := w.Header().Get("Location")
	if loc != "/promxy/backends" {
		t.Errorf("expected redirect to /promxy/backends, got %q", loc)
	}
}

func TestHandlerBackendsJSON_EmptyGroups(t *testing.T) {
	inv := Inventory{
		GeneratedAt: time.Now(),
		Groups:      []GroupInfo{},
	}
	h := newHandlerForTest(inv)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/promxy/api/backends.json", nil)
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var envelope jsonResponse
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(envelope.Groups) != 0 {
		t.Errorf("expected 0 groups, got %d", len(envelope.Groups))
	}
}
