package proxystorage

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// These tests pin the JSON envelope shape for /api/v1/status/walreplay (F5)
// and /api/v1/status/flags (F6). Both handlers are static — they intentionally
// don't touch ProxyStorage state — so a zero-value receiver is fine.

func TestWalReplayHandler_Shape(t *testing.T) {
	p := &ProxyStorage{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/walreplay", nil)

	p.WalReplayHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var env struct {
		Status string `json:"status"`
		Data   struct {
			Min     *int `json:"min"`
			Max     *int `json:"max"`
			Current *int `json:"current"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not valid JSON: %v (body=%q)", err, rr.Body.String())
	}
	if env.Status != "success" {
		t.Errorf("status field = %q, want success", env.Status)
	}
	if env.Data.Min == nil || env.Data.Max == nil || env.Data.Current == nil {
		t.Fatalf("data must contain min/max/current keys, body=%q", rr.Body.String())
	}
	if *env.Data.Min != 0 || *env.Data.Max != 0 || *env.Data.Current != 0 {
		t.Errorf("data = %+v, want all zeros", env.Data)
	}
}

func TestFlagsHandler_Shape(t *testing.T) {
	p := &ProxyStorage{}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/status/flags", nil)

	p.FlagsHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var env struct {
		Status string          `json:"status"`
		Data   json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &env); err != nil {
		t.Fatalf("response is not valid JSON: %v (body=%q)", err, rr.Body.String())
	}
	if env.Status != "success" {
		t.Errorf("status field = %q, want success", env.Status)
	}
	// data must be an object, not a struct dump with Go-style field names.
	var dataObj map[string]interface{}
	if err := json.Unmarshal(env.Data, &dataObj); err != nil {
		t.Fatalf("data must be a JSON object: %v (raw=%q)", err, string(env.Data))
	}
	if len(dataObj) != 0 {
		t.Errorf("data = %v, want empty object {} (regression guard for Go struct-field leak)", dataObj)
	}
}
