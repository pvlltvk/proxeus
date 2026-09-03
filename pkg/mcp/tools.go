package mcp

import (
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// emptyInputSchema is an explicit JSON Schema for the tools that take no
// arguments. The SDK's inferred schema for an empty struct omits the
// "properties" key, which OpenAI's stricter validation rejects; an empty
// "properties" object is semantically identical.
// See https://github.com/prometheus/prometheus-mcp/issues/119.
var emptyInputSchema = json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`)

// Tool definitions. Names, argument names and descriptions are those of
// prometheus-mcp, except for list_server_groups which has no counterpart there.
var (
	queryToolDef = &mcp.Tool{
		Name:        "query",
		Description: "Execute an instant query against the Prometheus datasource, returning one value per series at a single point in time",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Instant Query",
			ReadOnlyHint: true,
		},
	}

	rangeQueryToolDef = &mcp.Tool{
		Name:        "range_query",
		Description: "Execute a range query against the Prometheus datasource, returning values over a time window; use for graphing and trend analysis",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Range Query",
			ReadOnlyHint: true,
		},
	}

	seriesToolDef = &mcp.Tool{
		Name:        "series",
		Description: "Finds series by label matches",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Find Series",
			ReadOnlyHint: true,
		},
	}

	labelNamesToolDef = &mcp.Tool{
		Name:        "label_names",
		Description: "Returns the unique label names present in the block in sorted order by given time range and matches",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Label Names",
			ReadOnlyHint: true,
		},
	}

	labelValuesToolDef = &mcp.Tool{
		Name:        "label_values",
		Description: "Performs a query for the values of the given label, time range and matches",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Label Values",
			ReadOnlyHint: true,
		},
	}

	metricMetadataToolDef = &mcp.Tool{
		Name:        "metric_metadata",
		Description: "Returns metadata about metrics currently scraped by the metric name.",
		Annotations: &mcp.ToolAnnotations{
			Title:        "Metric Metadata",
			ReadOnlyHint: true,
		},
	}

	buildInfoToolDef = &mcp.Tool{
		Name:        "build_info",
		Description: "Get Prometheus build information",
		InputSchema: emptyInputSchema,
		Annotations: &mcp.ToolAnnotations{
			Title:        "Build Info",
			ReadOnlyHint: true,
		},
	}

	listServerGroupsToolDef = &mcp.Tool{
		Name: "list_server_groups",
		Description: "List the server_groups proxeus fans every query out to: name, backend type, per-target health, " +
			"the labels proxeus adds to their series, the time range they are configured to answer for and whether " +
			"they are read over remote_read. Call this to explain a result: a metric can be missing or a range " +
			"partial because the backend holding it is unhealthy or configured for a different time range.",
		InputSchema: emptyInputSchema,
		Annotations: &mcp.ToolAnnotations{
			Title:        "List Server Groups",
			ReadOnlyHint: true,
		},
	}
)

// Tool inputs. Fields without omitempty are required; descriptions come from
// the jsonschema tag.

// emptyInput is the input of the tools that take no arguments.
type emptyInput struct{}

// timeRangeInput provides the optional start/end times of the time-bounded tools.
type timeRangeInput struct {
	StartTime string `json:"start_time,omitempty" jsonschema:"start timestamp for the query. Accepts: Unix epoch seconds, RFC3339, or a duration string relative to now (e.g. 5m, 1h30m, etc). Defaults to 5m ago."`
	EndTime   string `json:"end_time,omitempty" jsonschema:"end timestamp for the query. Accepts: Unix epoch seconds, RFC3339, or a duration string relative to now (e.g. 5m, 1h30m, etc). Defaults to current time."`
}

// truncatableInput provides the optional per-call truncation limit. Unlike
// prometheus-mcp, truncation cannot be disabled: the limit only lowers the
// server-wide --mcp.max-series cap, it never raises it.
type truncatableInput struct {
	TruncationLimit int `json:"truncation_limit,omitempty" jsonschema:"truncation limit for query response in number of entries; use lower limits (e.g. 50-100) for initial exploration. Capped by the server-wide limit, which applies when this is unset."`
}

type queryInput struct {
	Query     string `json:"query" jsonschema:"the PromQL query to execute"`
	Timestamp string `json:"timestamp,omitempty" jsonschema:"evaluation timestamp for the instant query. Accepts: Unix epoch seconds, RFC3339, or a duration string relative to now e.g. 5m, 1h30m, etc. Defaults to current time."`
	truncatableInput
}

type rangeQueryInput struct {
	Query string `json:"query" jsonschema:"the PromQL query to execute"`
	Step  string `json:"step,omitempty" jsonschema:"query resolution step width in Go duration format (e.g. '30s', '5m', '1h'), auto-set if unspecified"`
	timeRangeInput
	truncatableInput
}

type seriesInput struct {
	Matches []string `json:"matches" jsonschema:"series selector arguments that select the series to return,required"`
	timeRangeInput
	truncatableInput
}

type labelNamesInput struct {
	Matches []string `json:"matches,omitempty" jsonschema:"series selector arguments to filter label names"`
	timeRangeInput
	truncatableInput
}

type labelValuesInput struct {
	Label   string   `json:"label" jsonschema:"the label to query values for,required"`
	Matches []string `json:"matches,omitempty" jsonschema:"series selector arguments to filter label values"`
	timeRangeInput
	truncatableInput
}

type metricMetadataInput struct {
	Metric string `json:"metric,omitempty" jsonschema:"metric name to retrieve metadata for, all metrics if empty"`
	Limit  string `json:"limit,omitempty" jsonschema:"maximum number of metrics to return"`
}

// Tool outputs. Every one of them is JSON, both as structured content and as
// the text fallback the SDK derives from it.

// truncation reports what a tool call had to leave out, so the model can tell
// "there is no more data" from "you asked for too much". Counts are entries:
// series for the query tools, label names/values or series for the others.
type truncation struct {
	Truncated             bool   `json:"truncated"`
	Returned              int    `json:"returned"`
	TotalBeforeTruncation int    `json:"total_before_truncation"`
	Note                  string `json:"note,omitempty"`
}

// wasTruncated satisfies the interface addTool uses to label the call metric.
func (t truncation) wasTruncated() bool { return t.Truncated }

// samplePoint is one sample. Values are strings, as in the Prometheus API, so
// NaN, +Inf and full float64 precision survive the round trip.
type samplePoint struct {
	Timestamp float64 `json:"timestamp"`
	Value     string  `json:"value"`
}

// seriesResult is one series of a query result. Instant queries put a single
// sample in Values.
type seriesResult struct {
	Metric map[string]string `json:"metric"`
	Values []samplePoint     `json:"values"`
}

type queryOutput struct {
	ResultType string         `json:"result_type"`
	Result     []seriesResult `json:"result"`
	Warnings   []string       `json:"warnings,omitempty"`
	truncation
}

type seriesOutput struct {
	Series   []map[string]string `json:"series"`
	Warnings []string            `json:"warnings,omitempty"`
	truncation
}

type labelNamesOutput struct {
	Names    []string `json:"names"`
	Warnings []string `json:"warnings,omitempty"`
	truncation
}

type labelValuesOutput struct {
	Values   []string `json:"values"`
	Warnings []string `json:"warnings,omitempty"`
	truncation
}

type metricMetadataOutput struct {
	Metadata map[string][]metadataEntry `json:"metadata"`
	truncation
}

type metadataEntry struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

type buildInfoOutput struct {
	Version   string `json:"version"`
	Revision  string `json:"revision"`
	Branch    string `json:"branch"`
	BuildUser string `json:"build_user"`
	BuildDate string `json:"build_date"`
	GoVersion string `json:"go_version"`
}

type serverGroupsOutput struct {
	GeneratedAt string            `json:"generated_at"`
	Groups      []serverGroupInfo `json:"groups"`
}

type serverGroupInfo struct {
	Name        string              `json:"name"`
	BackendType string              `json:"backend_type"`
	Labels      map[string]string   `json:"labels,omitempty"`
	TimeRange   string              `json:"time_range,omitempty"`
	RemoteRead  bool                `json:"remote_read"`
	Targets     []serverGroupTarget `json:"targets"`
}

type serverGroupTarget struct {
	URL         string `json:"url"`
	Healthy     bool   `json:"healthy"`
	LastError   string `json:"last_error,omitempty"`
	LastProbeAt string `json:"last_probe_at,omitempty"`
	Version     string `json:"version,omitempty"`
}
