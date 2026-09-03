// Package mcp serves proxeus's Model Context Protocol (MCP) endpoint.
//
// The tools are the read-only slice of the Prometheus HTTP API plus
// list_server_groups, which describes the fan-out behind an answer. Tool names,
// argument names and descriptions mirror github.com/prometheus/prometheus-mcp,
// so agents and skills written for that server work against proxeus unchanged.
//
// The tools reach the data through proxeus's own /api/v1 on the internal
// loopback listener rather than through the storage layer directly, so dedup,
// pushdown, the backend metrics and the access log see MCP traffic exactly as
// they see any other HTTP client.
package mcp

import (
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/version"

	"github.com/pvlltvk/proxeus/pkg/proxeusui"
)

// instructions is handed to the client on initialize: what proxeus is, and the
// two things a model gets wrong about it (partial results, raw selectors).
const instructions = "Proxeus is a single Prometheus-compatible PromQL endpoint in front of several backends " +
	"(Thanos, VictoriaMetrics, Prometheus, Mimir): every query is fanned out to all of them and the results are " +
	"merged and deduplicated, so an answer can be partial when a backend is down or only covers part of the " +
	"requested time range -- call list_server_groups to see which backends exist, whether they are healthy and " +
	"what time range they hold before concluding that a metric does not exist. Prefer aggregated queries " +
	"(sum/rate ... by (...)) over raw selectors: aggregations are pushed down to the backends, while a raw " +
	"selector drags every matching series across the network. Results are capped server-side and say so " +
	"(truncated, returned, total_before_truncation); when a result is truncated, narrow the matchers or " +
	"aggregate rather than asking for more, and pass truncation_limit to keep exploratory calls small."

// Config configures the MCP endpoint; it is built from the --mcp.* flags.
type Config struct {
	// APIURL is the base URL of the Prometheus API the tools call, i.e.
	// proxeus's own internal listener.
	APIURL string

	// MaxSeries and MaxSamples cap what a single tool call returns, in
	// series (or entries, for the label and series tools) and in samples.
	// A per-call truncation_limit may lower MaxSeries but never raise it.
	// Zero means uncapped.
	MaxSeries  int
	MaxSamples int

	// QueryTimeout bounds a single API call.
	QueryTimeout time.Duration

	// Inventory returns the server_group snapshot list_server_groups reports.
	Inventory func() proxeusui.Inventory
}

// Server is the MCP endpoint as an http.Handler. Mount it at
// <route-prefix>/mcp; it runs the streamable HTTP transport in stateless mode
// (no session state, so replicas behind a load balancer are interchangeable),
// where the SDK answers everything but POST with 405.
type Server struct {
	http.Handler

	api v1.API
	cfg Config
}

// New builds the MCP endpoint. The only failure mode is an unusable APIURL.
func New(cfg Config) (*Server, error) {
	client, err := api.NewClient(api.Config{Address: cfg.APIURL})
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus api client: %w", err)
	}

	s := &Server{api: v1.NewAPI(client), cfg: cfg}

	m := mcp.NewServer(
		&mcp.Implementation{Name: "proxeus", Title: "Proxeus", Version: version.Version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	addTool(m, queryToolDef, s.query)
	addTool(m, rangeQueryToolDef, s.rangeQuery)
	addTool(m, seriesToolDef, s.series)
	addTool(m, labelNamesToolDef, s.labelNames)
	addTool(m, labelValuesToolDef, s.labelValues)
	addTool(m, metricMetadataToolDef, s.metricMetadata)
	addTool(m, buildInfoToolDef, s.buildInfo)
	addTool(m, listServerGroupsToolDef, s.listServerGroups)

	s.Handler = mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return m },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
	return s, nil
}

// seriesLimit is the number of entries a call may return: the server cap,
// lowered (never raised) by the per-call truncation_limit. Zero means no cap.
func (s *Server) seriesLimit(arg int) int {
	if arg > 0 && (s.cfg.MaxSeries <= 0 || arg < s.cfg.MaxSeries) {
		return arg
	}
	return s.cfg.MaxSeries
}
