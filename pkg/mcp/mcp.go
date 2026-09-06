// Package mcp serves proxeus's Model Context Protocol (MCP) endpoint.
//
// The tools are the read-only slice of the Prometheus HTTP API plus
// list_server_groups, which describes the fan-out behind an answer. Tool names,
// argument names and descriptions mirror github.com/prometheus/prometheus-mcp,
// so agents and skills written for that server work against proxeus unchanged.
//
// The tools reach the data through proxeus's own /api/v1 handler rather than
// through the storage layer directly, so dedup, pushdown and the backend
// metrics see MCP traffic exactly as they see any other API request. The calls
// are served in this process (see handlerTransport), not over the network.
package mcp

import (
	"context"
	"errors"
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
	// Handler is the Prometheus API handler the tools call. Requests to it
	// are served in-process rather than over HTTP.
	Handler http.Handler

	// RoutePrefix is proxeus's --web.route-prefix. Handler mounts its routes
	// under it, so the tools have to call it through the prefix too.
	RoutePrefix string

	// MaxSeries and MaxSamples cap what a single tool call returns, in
	// series (or entries, for the label and series tools) and in samples.
	// A per-call truncation_limit may lower MaxSeries but never raise it.
	// Zero means uncapped.
	MaxSeries  int
	MaxSamples int

	// QueryTimeout bounds a single tool call. Zero means no timeout.
	QueryTimeout time.Duration

	// Inventory returns the server_group snapshot list_server_groups reports.
	Inventory func() proxeusui.Inventory

	// Metadata returns the metric metadata merged across the server_groups.
	// It does not go through Handler like the other tools do: the Prometheus
	// handler serves /api/v1/metadata from the scrape manager, which proxeus
	// never runs, so that endpoint always answers with an empty set.
	Metadata func(ctx context.Context, metric, limit string) (map[string][]v1.Metadata, error)
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

// New builds the MCP endpoint.
func New(cfg Config) (*Server, error) {
	// The SDK does not recover panics in tool handlers, so a missing
	// dependency has to fail here rather than on the first tool call.
	if cfg.Handler == nil {
		return nil, errors.New("mcp: Config.Handler is required")
	}
	if cfg.Inventory == nil {
		return nil, errors.New("mcp: Config.Inventory is required")
	}
	if cfg.Metadata == nil {
		return nil, errors.New("mcp: Config.Metadata is required")
	}

	// The host is never resolved -- handlerTransport serves the request
	// straight from cfg.Handler -- but the client needs a well-formed URL.
	client, err := api.NewClient(api.Config{
		Address:      "http://proxeus" + cfg.RoutePrefix,
		RoundTripper: handlerTransport{handler: cfg.Handler},
	})
	if err != nil {
		return nil, fmt.Errorf("error creating prometheus api client: %w", err)
	}

	s := &Server{api: v1.NewAPI(client), cfg: cfg}

	m := mcp.NewServer(
		&mcp.Implementation{Name: "proxeus", Title: "Proxeus", Version: version.Version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	addTool(s, m, queryToolDef, s.query)
	addTool(s, m, rangeQueryToolDef, s.rangeQuery)
	addTool(s, m, seriesToolDef, s.series)
	addTool(s, m, labelNamesToolDef, s.labelNames)
	addTool(s, m, labelValuesToolDef, s.labelValues)
	addTool(s, m, metricMetadataToolDef, s.metricMetadata)
	addTool(s, m, buildInfoToolDef, s.buildInfo)
	addTool(s, m, listServerGroupsToolDef, s.listServerGroups)

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
