package servergroup

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
)

// BackendType is the kind of backend a server_group talks to. It selects which
// typed dialect block (thanos, victoriametrics, mimir) may be configured and is
// displayed by the inventory UI. Empty means the operator didn't declare one.
// Auto-detection from /api/v1/status/buildinfo is unreliable (Thanos and VM omit
// the application field), so this is the canonical signal.
type BackendType string

const (
	BackendPrometheus      BackendType = "prometheus"
	BackendThanos          BackendType = "thanos"
	BackendVictoriaMetrics BackendType = "victoriametrics"
	BackendCortex          BackendType = "cortex"
	BackendMimir           BackendType = "mimir"
)

// backendTypes is the set of values accepted for backend_type, in the order
// they are listed back to the operator on a validation error.
var backendTypes = []BackendType{
	BackendPrometheus,
	BackendThanos,
	BackendVictoriaMetrics,
	BackendCortex,
	BackendMimir,
}

// dialect is a typed per-backend-family config block. Each implementation
// translates its knobs into the query params and HTTP headers that flavor of
// backend expects, so operators don't have to know the raw parameter names.
// queryParams returns a fresh, non-nil set the caller is free to add to.
type dialect interface {
	queryParams() url.Values
	headers() map[string]string
	validate() error
}

// ThanosConfig configures the Thanos-specific query options of a server group.
// Only valid with `backend_type: thanos`.
type ThanosConfig struct {
	// Dedup enables Thanos' own replica deduplication (`dedup`). Unset means
	// true -- the same default Thanos itself uses.
	Dedup *bool `yaml:"dedup,omitempty"`
	// PartialResponse allows Thanos to return partial results when some of its
	// stores are unavailable (`partial_response`). Unset leaves the choice to
	// the downstream default.
	PartialResponse *bool `yaml:"partial_response,omitempty"`
	// MaxSourceResolution caps the downsampling resolution Thanos may serve
	// from (`max_source_resolution`); "auto", "0s" or a duration (e.g. "5m").
	MaxSourceResolution string `yaml:"max_source_resolution,omitempty"`
	// ReplicaLabels overrides the labels Thanos dedups on, sent as one
	// `replicaLabels[]` param per label.
	ReplicaLabels []string `yaml:"replica_labels,omitempty"`
}

func (c *ThanosConfig) queryParams() url.Values {
	v := url.Values{}
	v.Set("dedup", strconv.FormatBool(c.Dedup == nil || *c.Dedup))
	if c.PartialResponse != nil {
		v.Set("partial_response", strconv.FormatBool(*c.PartialResponse))
	}
	if c.MaxSourceResolution != "" {
		v.Set("max_source_resolution", c.MaxSourceResolution)
	}
	if len(c.ReplicaLabels) > 0 {
		v["replicaLabels[]"] = c.ReplicaLabels
	}
	return v
}

func (c *ThanosConfig) headers() map[string]string { return nil }

func (c *ThanosConfig) validate() error {
	if c.MaxSourceResolution != "" && c.MaxSourceResolution != "auto" {
		if _, err := model.ParseDuration(c.MaxSourceResolution); err != nil {
			return fmt.Errorf("invalid thanos max_source_resolution %q: must be \"auto\" or a duration", c.MaxSourceResolution)
		}
	}
	return nil
}

// VictoriaMetricsConfig configures the VictoriaMetrics-specific query options of
// a server group. Only valid with `backend_type: victoriametrics`.
type VictoriaMetricsConfig struct {
	// NoCache disables VictoriaMetrics' response cache (`nocache`).
	NoCache *bool `yaml:"nocache,omitempty"`
	// ExtraFilters are label-matcher sets (e.g. `{env="prod"}`) added to every
	// selector of every query by the downstream (`extra_filters`), sent as one
	// param per filter.
	ExtraFilters []string `yaml:"extra_filters,omitempty"`
	// MaxLookback overrides the downstream lookback-delta (`max_lookback`).
	MaxLookback string `yaml:"max_lookback,omitempty"`
	// DenyPartialResponse makes VictoriaMetrics error out instead of returning
	// partial results when some of its storage nodes are unavailable
	// (`denyPartialResponse`).
	DenyPartialResponse *bool `yaml:"denyPartialResponse,omitempty"`
}

func (c *VictoriaMetricsConfig) queryParams() url.Values {
	v := url.Values{}
	if c.NoCache != nil {
		v.Set("nocache", vmBool(*c.NoCache))
	}
	if len(c.ExtraFilters) > 0 {
		v["extra_filters"] = c.ExtraFilters
	}
	if c.MaxLookback != "" {
		v.Set("max_lookback", c.MaxLookback)
	}
	if c.DenyPartialResponse != nil {
		v.Set("denyPartialResponse", vmBool(*c.DenyPartialResponse))
	}
	return v
}

func (c *VictoriaMetricsConfig) headers() map[string]string { return nil }

func (c *VictoriaMetricsConfig) validate() error {
	for _, filter := range c.ExtraFilters {
		if _, err := parser.ParseMetricSelector(filter); err != nil {
			return fmt.Errorf("error parsing victoriametrics extra_filters entry %q: %w", filter, err)
		}
	}
	if c.MaxLookback != "" {
		if _, err := model.ParseDuration(c.MaxLookback); err != nil {
			return fmt.Errorf("invalid victoriametrics max_lookback %q: %w", c.MaxLookback, err)
		}
	}
	return nil
}

// MimirConfig configures the Mimir/Cortex-specific options of a server group.
// Only valid with `backend_type: mimir` or `backend_type: cortex`.
//
// NOTE: a server_group represents a set of endpoints holding the *same* data, so
// this is a single tenant. Model each (backend, tenant) pair as its own
// server_group rather than fanning one group out over several tenants.
type MimirConfig struct {
	// Tenant is sent as the X-Scope-OrgID header multi-tenant Mimir/Cortex
	// requires. Required when the block is present.
	Tenant string `yaml:"tenant"`
}

func (c *MimirConfig) queryParams() url.Values { return url.Values{} }

func (c *MimirConfig) headers() map[string]string {
	return map[string]string{"X-Scope-OrgID": c.Tenant}
}

func (c *MimirConfig) validate() error {
	if c.Tenant == "" {
		return fmt.Errorf("mimir tenant is required")
	}
	return nil
}

// dialect returns the configured dialect block, or nil if there is none.
func (c *Config) dialect() dialect {
	switch {
	case c.Thanos != nil:
		return c.Thanos
	case c.VictoriaMetrics != nil:
		return c.VictoriaMetrics
	case c.Mimir != nil:
		return c.Mimir
	}
	return nil
}

// validateDialect checks that backend_type is a known value and that any typed
// dialect block matches it.
func (c *Config) validateDialect() error {
	switch c.BackendType {
	case "", BackendPrometheus, BackendThanos, BackendVictoriaMetrics, BackendCortex, BackendMimir:
	default:
		return fmt.Errorf("invalid backend_type %q: must be one of %v (or empty)", c.BackendType, backendTypes)
	}

	if c.Thanos != nil && c.BackendType != BackendThanos {
		return fmt.Errorf("thanos block requires backend_type: thanos, got %q", c.BackendType)
	}
	if c.VictoriaMetrics != nil && c.BackendType != BackendVictoriaMetrics {
		return fmt.Errorf("victoriametrics block requires backend_type: victoriametrics, got %q", c.BackendType)
	}
	if c.Mimir != nil && c.BackendType != BackendMimir && c.BackendType != BackendCortex {
		return fmt.Errorf("mimir block requires backend_type: mimir or cortex, got %q", c.BackendType)
	}

	if d := c.dialect(); d != nil {
		return d.validate()
	}
	return nil
}

// queryParams returns the query params to add to every downstream HTTP call:
// those derived from the dialect block first, then the generic query_params map
// which overrides them on key conflict.
func (c *Config) queryParams() url.Values {
	params := url.Values{}
	if d := c.dialect(); d != nil {
		params = d.queryParams()
	}
	for k, v := range c.QueryParams {
		params.Set(k, v)
	}
	return params
}

// httpHeaders returns the headers to set on every downstream HTTP call: those
// derived from the dialect block first, then the generic http_headers map which
// overrides them on key conflict.
func (c *Config) httpHeaders() map[string]string {
	headers := make(map[string]string, len(c.HTTPClientHeaders))
	if d := c.dialect(); d != nil {
		for k, v := range d.headers() {
			headers[k] = v
		}
	}
	for k, v := range c.HTTPClientHeaders {
		headers[k] = v
	}
	return headers
}

// vmBool renders a bool the way VictoriaMetrics' query API expects it.
func vmBool(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
