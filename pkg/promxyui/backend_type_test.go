package promxyui

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

func makeResp(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString(body)),
	}
}

func TestExtractVersion(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantVersion string
	}{
		{
			name:        "thanos buildinfo (no application field)",
			body:        `{"status":"success","data":{"version":"0.41.0","revision":"abc","branch":"HEAD","goVersion":"go1.25.7"}}`,
			wantVersion: "0.41.0",
		},
		{
			name:        "prometheus buildinfo",
			body:        `{"status":"success","data":{"version":"2.48.0","revision":"abc","branch":"main","goVersion":"go1.21.0"}}`,
			wantVersion: "2.48.0",
		},
		{
			name:        "victoriametrics minimal buildinfo",
			body:        `{"status":"success","data":{"version":"2.24.0"}}`,
			wantVersion: "2.24.0",
		},
		{
			name:        "non-success envelope",
			body:        `{"status":"error","error":"not implemented"}`,
			wantVersion: "",
		},
		{
			name:        "malformed body",
			body:        `not json`,
			wantVersion: "",
		},
		{
			name:        "envelope without version",
			body:        `{"status":"success","data":{}}`,
			wantVersion: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := makeResp(http.StatusOK, tc.body)
			if got := extractVersion(resp); got != tc.wantVersion {
				t.Errorf("Version: got %q, want %q", got, tc.wantVersion)
			}
		})
	}
}
