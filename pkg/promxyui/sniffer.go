package promxyui

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// BackendType is the detected type of a downstream backend.
type BackendType string

const (
	BackendThanos          BackendType = "thanos"
	BackendVictoriaMetrics BackendType = "victoriametrics"
	BackendPrometheus      BackendType = "prometheus"
	BackendUnknown         BackendType = "unknown"
)

// buildInfoData matches the "data" object of /api/v1/status/buildinfo.
type buildInfoData struct {
	Version     string `json:"version"`
	Application string `json:"application"`
}

// buildInfoEnvelope is the outer Prometheus API envelope.
type buildInfoEnvelope struct {
	Status string        `json:"status"`
	Data   buildInfoData `json:"data"`
}

// sniffBuildInfo classifies the backend and extracts the version from the HTTP
// response to GET /api/v1/status/buildinfo.
//
// Classification rules:
//  1. HTTP 404 or a non-"success" status field → victoriametrics (vmselect
//     does not implement this endpoint cleanly).
//  2. data.application contains "thanos" (case-insensitive) → thanos.
//  3. data.application is empty or contains "prometheus" → prometheus.
//  4. Any other parseable response → unknown.
//
// The Server response header is checked first: "VictoriaMetrics" in that
// header is definitive regardless of body shape.
func sniffBuildInfo(resp *http.Response, body []byte) (BackendType, string) {
	// Server header shortcut for VictoriaMetrics.
	if strings.Contains(resp.Header.Get("Server"), "VictoriaMetrics") {
		return BackendVictoriaMetrics, ""
	}

	if resp.StatusCode == http.StatusNotFound {
		return BackendVictoriaMetrics, ""
	}

	var env buildInfoEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return BackendUnknown, ""
	}

	if env.Status != "success" {
		// Non-success envelope is the VM fallback heuristic.
		return BackendVictoriaMetrics, ""
	}

	app := strings.ToLower(env.Data.Application)
	switch {
	case strings.Contains(app, "thanos"):
		return BackendThanos, env.Data.Version
	case app == "" || strings.Contains(app, "prometheus"):
		return BackendPrometheus, env.Data.Version
	default:
		return BackendUnknown, env.Data.Version
	}
}

// drainAndClassify reads the full response body and delegates to sniffBuildInfo.
// It is the entry point used by the prober.
func drainAndClassify(resp *http.Response) (BackendType, string) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return BackendUnknown, ""
	}
	return sniffBuildInfo(resp, body)
}
