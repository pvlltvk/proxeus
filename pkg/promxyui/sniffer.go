package promxyui

import (
	"encoding/json"
	"io"
	"net/http"
)

// BackendType is the type a downstream backend is declared as in the
// server_group config. The set is open — any string is allowed — but these
// constants cover the backends we currently care about. BackendUnknown is
// used when the config omits backend_type.
type BackendType string

const (
	BackendThanos          BackendType = "thanos"
	BackendVictoriaMetrics BackendType = "victoriametrics"
	BackendPrometheus      BackendType = "prometheus"
	BackendCortex          BackendType = "cortex"
	BackendMimir           BackendType = "mimir"
	BackendUnknown         BackendType = "unknown"
)

// buildInfoData matches the "data" object of /api/v1/status/buildinfo.
type buildInfoData struct {
	Version string `json:"version"`
}

// buildInfoEnvelope is the outer Prometheus API envelope.
type buildInfoEnvelope struct {
	Status string        `json:"status"`
	Data   buildInfoData `json:"data"`
}

// extractVersion reads the body of a GET /api/v1/status/buildinfo response
// and returns the version string, or "" if it can't be parsed. Type detection
// is handled by the server_group's declared backend_type config field, not
// here — Thanos and VictoriaMetrics buildinfo bodies don't carry an
// application identifier we can rely on.
func extractVersion(resp *http.Response) string {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}
	var env buildInfoEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return ""
	}
	if env.Status != "success" {
		return ""
	}
	return env.Data.Version
}
