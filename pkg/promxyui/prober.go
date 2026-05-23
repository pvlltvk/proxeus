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
type ProbeResult struct {
	Healthy     bool
	LastError   string
	LastProbeAt time.Time
	BackendType BackendType
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
			result := p.probeTarget(ctx, sgCfg.GetScheme(), target)
			p.mu.Lock()
			p.results[key] = result
			p.mu.Unlock()
		}
	}
}

// probeTarget performs a single health check against one target (host:port).
func (p *Prober) probeTarget(ctx context.Context, scheme, rawTarget string) ProbeResult {
	if scheme == "" {
		scheme = "http"
	}
	buildInfoURL := fmt.Sprintf("%s://%s/api/v1/status/buildinfo", scheme, rawTarget)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildInfoURL, nil)
	if err != nil {
		return ProbeResult{
			Healthy:     false,
			LastError:   err.Error(),
			LastProbeAt: time.Now(),
			BackendType: BackendUnknown,
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
			BackendType: BackendUnknown,
		}
	}

	bt, version := drainAndClassify(resp)

	// buildinfo returned a non-200 (e.g. VM 404) — target may still be healthy.
	if resp.StatusCode != http.StatusOK {
		healthy := p.checkLabelsEndpoint(ctx, scheme, rawTarget)
		return ProbeResult{
			Healthy:     healthy,
			LastError:   "",
			LastProbeAt: probeAt,
			BackendType: bt,
			Version:     version,
		}
	}

	return ProbeResult{
		Healthy:     true,
		LastError:   "",
		LastProbeAt: probeAt,
		BackendType: bt,
		Version:     version,
	}
}

// checkLabelsEndpoint hits /api/v1/labels to confirm the target is reachable.
func (p *Prober) checkLabelsEndpoint(ctx context.Context, scheme, rawTarget string) bool {
	labelsURL := fmt.Sprintf("%s://%s/api/v1/labels", scheme, rawTarget)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, labelsURL, nil)
	if err != nil {
		return false
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
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

		sg, ok := sgByName[sgCfg.Name]
		if ok {
			if sgState := sg.State(); sgState != nil {
				for _, target := range sgState.Targets {
					key := probeKey{group: sgCfg.Name, target: target}
					result, found := p.results[key]
					if !found {
						result = ProbeResult{BackendType: BackendUnknown}
					}
					gi.Targets = append(gi.Targets, TargetInfo{
						URL:         buildTargetURL(sgCfg.GetScheme(), target),
						Healthy:     result.Healthy,
						LastError:   result.LastError,
						LastProbeAt: result.LastProbeAt,
						BackendType: result.BackendType,
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

func buildTargetURL(scheme, host string) string {
	if scheme == "" {
		scheme = "http"
	}
	return scheme + "://" + host
}
