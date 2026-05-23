package promxyui

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	proxyconfig "github.com/jacksontj/promxy/pkg/config"
	"github.com/jacksontj/promxy/pkg/proxystorage"
	"github.com/jacksontj/promxy/pkg/servergroup"
)

const (
	defaultProbeInterval = 30 * time.Second
	defaultProbeTimeout  = 5 * time.Second
)

// probeKey uniquely identifies a (group, target) pair.
type probeKey struct {
	group  string
	target string
}

// ProbeResult holds the outcome of a single health probe.
//
// BackendType is not stored here — it comes from the server_group's declared
// backend_type config field, joined in by Inventory() at read time. That way
// type info is correct before the first probe completes and stays in sync
// with config reloads.
type ProbeResult struct {
	Healthy     bool
	LastError   string
	LastProbeAt time.Time
	Version     string
}

// TargetInfo combines resolved URL with the latest probe result.
type TargetInfo struct {
	URL         string
	Healthy     bool
	LastError   string
	LastProbeAt time.Time
	BackendType BackendType
	Version     string
}

// GroupInfo carries the full inventory snapshot for a single server_group.
type GroupInfo struct {
	Name    string
	Ordinal int
	Labels  map[string]string
	Targets []TargetInfo
}

// Inventory is the snapshot returned to the HTTP handler.
type Inventory struct {
	GeneratedAt time.Time
	Groups      []GroupInfo
}

// storageAccessor is the subset of ProxyStorage the prober needs.
// Defined as an interface so tests can inject a stub.
type storageAccessor interface {
	ServerGroups() []*servergroup.ServerGroup
	Config() *proxyconfig.Config
}

// Prober runs background health probes for all (group, target) pairs and
// maintains the last result in an in-memory map.
type Prober struct {
	ps       storageAccessor
	client   *http.Client
	interval time.Duration

	mu      sync.RWMutex
	results map[probeKey]ProbeResult
}

// NewProber creates a Prober backed by ps. Call Run to start background probing.
func NewProber(ps *proxystorage.ProxyStorage) *Prober {
	return newProber(ps)
}

// newProber is the internal constructor that accepts the interface (used by tests).
func newProber(ps storageAccessor) *Prober {
	return &Prober{
		ps: ps,
		client: &http.Client{
			Timeout: defaultProbeTimeout,
		},
		interval: defaultProbeInterval,
		results:  make(map[probeKey]ProbeResult),
	}
}

// Run starts the probe loop. It probes immediately on entry, then every interval.
// It returns when ctx is cancelled.
func (p *Prober) Run(ctx context.Context) {
	p.probeAll(ctx)
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.probeAll(ctx)
		}
	}
}

// probeAll walks every (group, target) in the current configuration.
func (p *Prober) probeAll(ctx context.Context) {
	cfg := p.ps.Config()
	if cfg == nil {
		return
	}
	sgs := p.ps.ServerGroups()
	// Build a name→ServerGroup map for lookup.
	sgByName := make(map[string]*servergroup.ServerGroup, len(sgs))
	for _, sg := range sgs {
		if sg.Cfg != nil {
			sgByName[sg.Cfg.Name] = sg
		}
	}

	for _, sgCfg := range cfg.PromxyConfig.ServerGroups {
		sg, ok := sgByName[sgCfg.Name]
		if !ok {
			continue
		}
		sgState := sg.State()
		if sgState == nil {
			continue
		}
		for _, target := range sgState.Targets {
			key := probeKey{group: sgCfg.Name, target: target}
			result := p.probeTarget(ctx, sgCfg, target)
			p.mu.Lock()
			p.results[key] = result
			p.mu.Unlock()
		}
	}
}

// probeTarget performs a single health check against one target (host:port).
// The URL is built from sgCfg.GetScheme() + target + sgCfg.PathPrefix, so
// backends with a non-root path prefix (e.g. VictoriaMetrics at
// /select/0/prometheus) are probed at the correct endpoint.
func (p *Prober) probeTarget(ctx context.Context, sgCfg *servergroup.Config, rawTarget string) ProbeResult {
	base := backendBaseURL(sgCfg, rawTarget)
	buildInfoURL := base + "/api/v1/status/buildinfo"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildInfoURL, nil)
	if err != nil {
		return ProbeResult{
			Healthy:     false,
			LastError:   err.Error(),
			LastProbeAt: time.Now(),
		}
	}

	resp, err := p.client.Do(req)
	probeAt := time.Now()
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "context deadline exceeded") || strings.Contains(errStr, "timeout") {
			errStr = "probe timed out"
		}
		logrus.WithFields(logrus.Fields{"target": rawTarget}).Debugf("promxyui probe error: %s", errStr)
		return ProbeResult{
			Healthy:     false,
			LastError:   errStr,
			LastProbeAt: probeAt,
		}
	}

	if resp.StatusCode != http.StatusOK {
		// buildinfo non-200 — try /api/v1/labels as a liveness fallback.
		// Drain and close the buildinfo response so the connection can be reused.
		resp.Body.Close()
		healthy, fallbackErr := p.checkLabelsEndpoint(ctx, base)
		return ProbeResult{
			Healthy:     healthy,
			LastError:   fallbackErr,
			LastProbeAt: probeAt,
		}
	}

	version := extractVersion(resp)
	return ProbeResult{
		Healthy:     true,
		LastError:   "",
		LastProbeAt: probeAt,
		Version:     version,
	}
}

// checkLabelsEndpoint hits /api/v1/labels (under the same base URL) to confirm
// the target is reachable. Returns (healthy, errorString).
func (p *Prober) checkLabelsEndpoint(ctx context.Context, base string) (bool, string) {
	labelsURL := base + "/api/v1/labels"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, labelsURL, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, ""
	}
	return false, fmt.Sprintf("buildinfo and labels both unreachable (labels HTTP %d)", resp.StatusCode)
}

// Inventory builds a snapshot from the stored probe results and current config.
func (p *Prober) Inventory() Inventory {
	cfg := p.ps.Config()
	sgs := p.ps.ServerGroups()

	inv := Inventory{GeneratedAt: time.Now()}
	if cfg == nil {
		return inv
	}

	sgByName := make(map[string]*servergroup.ServerGroup, len(sgs))
	for _, sg := range sgs {
		if sg.Cfg != nil {
			sgByName[sg.Cfg.Name] = sg
		}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	for i, sgCfg := range cfg.PromxyConfig.ServerGroups {
		gi := GroupInfo{
			Name:    sgCfg.Name,
			Ordinal: i,
			Labels:  labelSetToMap(sgCfg),
		}

		backendType := BackendType(sgCfg.BackendType)
		if backendType == "" {
			backendType = BackendUnknown
		}

		sg, ok := sgByName[sgCfg.Name]
		if ok {
			if sgState := sg.State(); sgState != nil {
				for _, target := range sgState.Targets {
					key := probeKey{group: sgCfg.Name, target: target}
					result := p.results[key]
					gi.Targets = append(gi.Targets, TargetInfo{
						URL:         backendBaseURL(sgCfg, target),
						Healthy:     result.Healthy,
						LastError:   result.LastError,
						LastProbeAt: result.LastProbeAt,
						BackendType: backendType,
						Version:     result.Version,
					})
				}
			}
		}

		inv.Groups = append(inv.Groups, gi)
	}

	return inv
}

func labelSetToMap(sgCfg *servergroup.Config) map[string]string {
	m := make(map[string]string, len(sgCfg.Labels))
	for k, v := range sgCfg.Labels {
		m[string(k)] = string(v)
	}
	return m
}

// backendBaseURL returns the full base URL that promxy uses to talk to a
// target: scheme://host{path_prefix}. The path_prefix is included verbatim
// (without trailing slash) so callers can append e.g. "/api/v1/...".
func backendBaseURL(sgCfg *servergroup.Config, host string) string {
	scheme := sgCfg.GetScheme()
	if scheme == "" {
		scheme = "http"
	}
	prefix := strings.TrimRight(sgCfg.PathPrefix, "/")
	return scheme + "://" + host + prefix
}
