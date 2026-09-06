package mcp

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// toolCalls counts tool calls by outcome. `truncated` is a successful call
// whose result hit one of the caps -- worth watching, since the model only
// sees part of the answer.
var toolCalls = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "proxeus_mcp_tool_calls_total",
		Help: "Number of MCP tool calls by tool and result (ok, truncated, error).",
	},
	[]string{"tool", "result"},
)

var toolCallDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name: "proxeus_mcp_tool_call_duration_seconds",
		Help: "Duration of MCP tool calls, by tool.",
		// Up to 102.4s, so the default --mcp.query-timeout of 60s falls
		// inside the buckets rather than in +Inf.
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	},
	[]string{"tool"},
)

// truncatable is implemented by the tool outputs that embed truncation.
type truncatable interface {
	wasTruncated() bool
}

// addTool registers a tool on m, bounded by the configured query timeout and
// instrumented: one duration observation and one call counted per invocation.
// A panic anywhere in the tool fails that one call: the SDK runs tool handlers
// on goroutines of its own that nothing else recovers on, and the Prometheus
// handler the tools call runs in-process, so a panic in it would otherwise
// take proxeus down.
func addTool[In, Out any](s *Server, m *mcp.Server, def *mcp.Tool, h mcp.ToolHandlerFor[In, Out]) {
	mcp.AddTool(m, def, func(ctx context.Context, req *mcp.CallToolRequest, in In) (res *mcp.CallToolResult, out Out, err error) {
		defer func() {
			if r := recover(); r != nil {
				res, err = nil, fmt.Errorf("mcp: tool %s panicked: %v\n%s", def.Name, r, debug.Stack())
				toolCalls.WithLabelValues(def.Name, "error").Inc()
			}
		}()
		if s.cfg.QueryTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, s.cfg.QueryTimeout)
			defer cancel()
		}

		start := time.Now()
		res, out, err = h(ctx, req, in)
		toolCallDuration.WithLabelValues(def.Name).Observe(time.Since(start).Seconds())
		toolCalls.WithLabelValues(def.Name, callResult(out, err)).Inc()
		return res, out, err
	})
}

func callResult(out any, err error) string {
	switch t, ok := out.(truncatable); {
	case err != nil:
		return "error"
	case ok && t.wasTruncated():
		return "truncated"
	default:
		return "ok"
	}
}
