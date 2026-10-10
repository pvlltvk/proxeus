package proxyconfig

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/prometheus/prometheus/config"
	yaml "gopkg.in/yaml.v2"

	"github.com/pvlltvk/proxeus/pkg/auth"
	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// DefaultProxeusConfig is the default proxeus config that the config file
// is loaded into
var DefaultProxeusConfig = ProxeusConfig{CrossGroupDedupFillGaps: true}

// ValidateUniqueServerGroupLabels ensures that every server_group carries a
// non-empty labels set and that no two groups share the same label fingerprint.
// It uses the same model.LabelSet.FastFingerprint algorithm that NewMultiAPI uses
// internally so the check is consistent with the one inside promclient.
//
// Single-group configurations are exempt: with only one group there is no
// cross-group identity to disambiguate and no dedup partner, so empty labels
// are unambiguous.
func ValidateUniqueServerGroupLabels(groups []*servergroup.Config) error {
	if len(groups) < 2 {
		return nil
	}

	type entry struct {
		name   string
		labels model.LabelSet
	}
	seen := make(map[model.Fingerprint][]entry)

	for i, cfg := range groups {
		name := cfg.Name
		if name == "" {
			name = fmt.Sprintf("sg-%d", i)
		}
		if len(cfg.Labels) == 0 {
			return fmt.Errorf(
				"server_group label collision: group %s has empty labels — every server_group must declare a unique non-empty 'labels' set",
				name,
			)
		}
		fp := cfg.Labels.FastFingerprint()
		seen[fp] = append(seen[fp], entry{name: name, labels: cfg.Labels})
	}

	for _, entries := range seen {
		if len(entries) < 2 {
			continue
		}
		parts := make([]string, len(entries))
		for i, e := range entries {
			parts[i] = fmt.Sprintf("%s (labels=%s)", e.name, e.labels)
		}
		return fmt.Errorf(
			"server_group label collision: groups [%s] share the same labels — every server_group must declare a unique non-empty 'labels' set",
			strings.Join(parts, ", "),
		)
	}
	return nil
}

func validateCrossGroupDedupIgnoreLabels(names []string) error {
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("cross_group_dedup_ignore_labels: empty label name")
		}
		if name == model.MetricNameLabel {
			return fmt.Errorf("cross_group_dedup_ignore_labels: %q cannot be ignored", model.MetricNameLabel)
		}
		if !model.LabelName(name).IsValid() {
			return fmt.Errorf("cross_group_dedup_ignore_labels: %q is not a valid label name", name)
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("cross_group_dedup_ignore_labels: duplicate label name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// ConfigFromFile loads a config file at path
func ConfigFromFile(path string) (*Config, error) {
	configBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error loading config: %v", err)
	}
	return ConfigFromBytes(configBytes)
}

// ConfigFromBytes loads a config from raw YAML bytes.
func ConfigFromBytes(configBytes []byte) (*Config, error) {
	cfg := &Config{
		PromConfig:    config.DefaultConfig,
		ProxeusConfig: DefaultProxeusConfig,
	}
	if err := yaml.Unmarshal(configBytes, cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %v", err)
	}

	if err := rejectWriteSide(&cfg.PromConfig); err != nil {
		return nil, err
	}

	// Validate here rather than only in ProxyStorage.ApplyConfig so --check-config
	// and a SIGHUP reload reject the same configs startup does, before any
	// Reloadable sees them.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// rejectWriteSide refuses the Prometheus sections proxeus does not act on, so
// a config that expects rules or remote_write fails at load, not silently.
func rejectWriteSide(c *config.Config) error {
	switch {
	case len(c.RuleFiles) > 0:
		return fmt.Errorf("rule_files is not supported: proxeus evaluates no rules; run a ruler that queries proxeus")
	case len(c.RemoteWriteConfigs) > 0:
		return fmt.Errorf("remote_write is not supported: proxeus is read-only")
	case len(c.AlertingConfig.AlertmanagerConfigs) > 0 || len(c.AlertingConfig.AlertRelabelConfigs) > 0:
		return fmt.Errorf("alerting is not supported: proxeus sends no alerts")
	}
	return nil
}

// Config is the entire config file. This includes both the Prometheus Config
// as well as the Proxeus config. This is done by "inline-ing" the proxeus
// config into the prometheus config under the "proxeus" key
type Config struct {
	// Prometheus configs: global settings and tracing.
	PromConfig config.Config `yaml:",inline"`

	// Proxeus specific configuration -- under its own namespace
	ProxeusConfig `yaml:"proxeus"`

	WebConfig web.TLSConfig `yaml:"tls_server_config"`
}

func (c *Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("<error creating config string: %s>", err)
	}
	return string(b)
}

// ProxeusConfig is the configuration for Proxeus itself
type ProxeusConfig struct {
	// Config for each of the server groups proxeus is configured to aggregate
	ServerGroups []*servergroup.Config `yaml:"server_groups"`

	// Auth configures authentication of incoming requests. When absent every
	// request is anonymous, which is proxeus' historical behavior. Unlike the
	// rest of this config it is read at startup only -- changing it requires a
	// restart.
	Auth *auth.Config `yaml:"auth,omitempty"`

	// CrossGroupDedup, when true, enables deterministic cross-server_group
	// deduplication: series that match modulo each server_group's external
	// `labels` collapse to one. The lower-ordinal server_group wins; ordinal
	// is the index in `server_groups[]` (YAML order). Default false preserves
	// historical behavior — both backends' series are returned.
	//
	// Scope: dedup applies to raw selector results only. Pushed-down
	// aggregations (sum(up), count(...) etc.) fan out per-group PARTIALS that
	// the engine re-combines, so those are unioned, never deduped — a series
	// present in multiple groups appears once in `up` but contributes to every
	// group's partial in `count(up)`. If exact results over overlapping
	// groups matter, enable CrossGroupExact.
	CrossGroupDedup bool `yaml:"cross_group_dedup"`

	// CrossGroupExact, when true, makes queries exact over
	// overlapping server_groups and migration seams: proxeus declines all
	// pushdown while more than one server_group is configured, so the engine
	// evaluates the whole query over deduplicated, gap-filled raw series
	// instead of re-combining per-group results. `count(up)` then matches `up`,
	// and rate() across a seam sees both sides. Requires CrossGroupDedup to
	// also be true; proxeus will refuse to start otherwise. Default false
	// preserves historical behavior.
	//
	// Cost: raw samples cross the network instead of per-group partials and
	// step-aligned results, which is the expensive path — a wide range now
	// transfers every sample of every series the query reads.
	CrossGroupExact bool `yaml:"cross_group_exact"`

	// CrossGroupDedupMetadata extends the same reduced-fingerprint dedup to
	// /api/v1/series so Grafana label browsers and dashboards don't show one
	// row per backend for what is logically a single target. Requires
	// CrossGroupDedup to also be true; proxeus will refuse to start otherwise.
	// Default false preserves historical behavior.
	CrossGroupDedupMetadata bool `yaml:"cross_group_dedup_metadata"`

	// CrossGroupPartialResponse, when true, lets a query succeed when only some
	// server_groups respond: results from the healthy backends are returned and
	// a warning is attached for each that failed (Grafana surfaces it). When
	// false (default) any single backend error fails the whole query — correct
	// for HA replicas, but undesirable for federating disjoint data, where a
	// Thanos outage should not blank out VM-sourced series. Only affects the
	// cross-group fan-out, so it requires CrossGroupDedup to also be true.
	CrossGroupPartialResponse bool `yaml:"cross_group_partial_response"`

	// CrossGroupDedupIgnoreLabels are labels the backends stamp themselves
	// (Thanos Receive's receive_replica and tenant_id, say) that, like the
	// server_group labels keys, do not count toward a series' identity. Kept on
	// output. Requires CrossGroupDedup to also be true.
	CrossGroupDedupIgnoreLabels []string `yaml:"cross_group_dedup_ignore_labels"`

	// CrossGroupDedupFillGaps fills gaps in the dedup winner's samples from
	// lower-priority backends. On by default; no effect without CrossGroupDedup.
	CrossGroupDedupFillGaps bool `yaml:"cross_group_dedup_fill_gaps"`

	// CrossGroupDedupGap is the sample interval that counts as a gap; 0 derives
	// it from the winner's own sample spacing. Requires CrossGroupDedupFillGaps.
	CrossGroupDedupGap time.Duration `yaml:"cross_group_dedup_gap"`
}

// Validate checks the cross-group flag dependencies. CrossGroupDedupMetadata,
// CrossGroupExact and CrossGroupPartialResponse only affect the
// cross-group fan-out/dedup machinery, so none makes sense without
// CrossGroupDedup also being enabled.
func (c *ProxeusConfig) Validate() error {
	if c.CrossGroupDedupMetadata && !c.CrossGroupDedup {
		return fmt.Errorf("cross_group_dedup_metadata: true requires cross_group_dedup: true")
	}
	// Without dedup the raw fan-out returns both groups' series, so the
	// locally computed aggregate double-counts anyway — the flag would look
	// like a fix while changing nothing but the query cost.
	if c.CrossGroupExact && !c.CrossGroupDedup {
		return fmt.Errorf("cross_group_exact: true requires cross_group_dedup: true")
	}
	if c.CrossGroupPartialResponse && !c.CrossGroupDedup {
		return fmt.Errorf("cross_group_partial_response: true requires cross_group_dedup: true")
	}
	if len(c.CrossGroupDedupIgnoreLabels) > 0 && !c.CrossGroupDedup {
		return fmt.Errorf("cross_group_dedup_ignore_labels: requires cross_group_dedup: true")
	}
	if err := validateCrossGroupDedupIgnoreLabels(c.CrossGroupDedupIgnoreLabels); err != nil {
		return err
	}
	if c.CrossGroupDedupGap != 0 && !c.CrossGroupDedupFillGaps {
		return fmt.Errorf("cross_group_dedup_gap: requires cross_group_dedup_fill_gaps: true")
	}
	if c.CrossGroupDedupGap < 0 {
		return fmt.Errorf("cross_group_dedup_gap: must not be negative")
	}
	// Dedup keys series identity on these labels, so a collision would silently
	// merge unrelated series. Checked here as well as in ApplyConfig so that
	// --check-config and a reload reject it instead of deferring to a startup
	// fatal. Without dedup it stays a warning, which ApplyConfig still emits.
	if c.CrossGroupDedup {
		if err := ValidateUniqueServerGroupLabels(c.ServerGroups); err != nil {
			return err
		}
	}
	return nil
}
