package proxeusui

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// BackendUnknown is what the inventory shows for a server_group that declares
// no backend_type. It is a display value only -- the configurable set lives in
// servergroup.BackendType.
const BackendUnknown servergroup.BackendType = "unknown"

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
//
// The caller (probeTarget) owns closing resp.Body; extractVersion only reads it.
func extractVersion(resp *http.Response) string {
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
