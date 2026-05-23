package promxyui

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

func makeResp(statusCode int, serverHeader, body string) *http.Response {
	h := make(http.Header)
	if serverHeader != "" {
		h.Set("Server", serverHeader)
	}
	return &http.Response{
		StatusCode: statusCode,
		Header:     h,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestSniffBuildInfo(t *testing.T) {
	cases := []struct {
		name         string
		statusCode   int
		serverHeader string
		body         string
		wantType     BackendType
		wantVersion  string
	}{
		{
			name:       "thanos query",
			statusCode: 200,
			body: `{"status":"success","data":{"version":"0.36.0","revision":"abc","branch":"main",` +
				`"goVersion":"go1.21.0","application":"thanos query"}}`,
			wantType:    BackendThanos,
			wantVersion: "0.36.0",
		},
		{
			name:       "prometheus",
			statusCode: 200,
			body:       `{"status":"success","data":{"version":"2.48.0","revision":"abc","branch":"main","goVersion":"go1.21.0","application":"prometheus"}}`,
			wantType:    BackendPrometheus,
			wantVersion: "2.48.0",
		},
		{
			name:       "prometheus no application field",
			statusCode: 200,
			body:       `{"status":"success","data":{"version":"2.45.0","revision":"abc","branch":"main","goVersion":"go1.21.0"}}`,
			wantType:    BackendPrometheus,
			wantVersion: "2.45.0",
		},
		{
			name:        "victoriametrics 404",
			statusCode:  404,
			body:        `404 page not found`,
			wantType:    BackendVictoriaMetrics,
			wantVersion: "",
		},
		{
			name:        "victoriametrics server header",
			statusCode:  200,
			serverHeader: "VictoriaMetrics/1.95.1",
			body:        `{"status":"success","data":{"version":"1.95.1"}}`,
			wantType:    BackendVictoriaMetrics,
			wantVersion: "",
		},
		{
			name:        "non-success status field",
			statusCode:  200,
			body:        `{"status":"error","error":"not implemented"}`,
			wantType:    BackendVictoriaMetrics,
			wantVersion: "",
		},
		{
			name:        "malformed body",
			statusCode:  200,
			body:        `not json`,
			wantType:    BackendUnknown,
			wantVersion: "",
		},
		{
			name:       "unknown application",
			statusCode: 200,
			body:       `{"status":"success","data":{"version":"1.0.0","application":"cortex"}}`,
			wantType:    BackendUnknown,
			wantVersion: "1.0.0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := makeResp(tc.statusCode, tc.serverHeader, tc.body)
			body, _ := io.ReadAll(resp.Body)
			resp.Body = io.NopCloser(bytes.NewBuffer(body))

			gotType, gotVersion := sniffBuildInfo(resp, body)
			if gotType != tc.wantType {
				t.Errorf("BackendType: got %q, want %q", gotType, tc.wantType)
			}
			if gotVersion != tc.wantVersion {
				t.Errorf("Version: got %q, want %q", gotVersion, tc.wantVersion)
			}
		})
	}
}
