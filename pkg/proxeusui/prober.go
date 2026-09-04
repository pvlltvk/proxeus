package proxeusui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/proxystorage"
	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

const (
	defaultProbeInterval = 30 * time.Second
	defaultProbeTimeout  = 5 * time.Second
	maxProbeConcurrency  = 8
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
	Healthy     bool      `json:"healthy"`
	LastError   string    `json:"lastError"`
	LastProbeAt time.Time `json:"lastProbeAt"`
	Version     string    `json:"version"`
}

// TargetInfo combines resolved URL with the latest probe result.
type TargetInfo struct {
	URL string `json:"url"`
	ProbeResult
	BackendType servergroup.BackendType `json:"backendType"`
}

// GroupInfo carries the full inventory snapshot for a single server_group.
//
// Like BackendType, RemoteRead and TimeRange are joined in from the
// server_group config at read time rather than probed.
type GroupInfo struct {
	Name    string            `json:"name"`
	Ordinal int               `json:"ordinal"`
	Labels  map[string]string `json:"labels"`
	// RemoteRead reports whether raw data is read from this group over the
	// remote read API instead of the query API.
	RemoteRead bool `json:"remoteRead"`
	// TimeRange is the window the group is configured to answer for, e.g.
	// "now-90d to now-3d". Empty when the group has no time range configured.
	TimeRange string       `json:"timeRange,omitempty"`
	Targets   []TargetInfo `json:"targets"`
}

// Inventory is the snapshot returned to the HTTP handler.
type Inventory struct {
	GeneratedAt time.Time   `json:"generatedAt"`
	Groups      []GroupInfo `json:"groups"`
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
	timeout  time.Duration

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
		timeout:  defaultProbeTimeout,
		results:  make(map[probeKey]ProbeResult),
	}
}

// Run starts the probe loop. It probes immediately on entry, then every interval.
// It returns when ctx is canceled.
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
// Per-target probes run concurrently, bounded by maxProbeConcurrency.
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

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxProbeConcurrency)

	for _, sgCfg := range cfg.ServerGroups {
		sg, ok := sgByName[sgCfg.Name]
		if !ok {
			continue
		}
		sgState := sg.State()
		if sgState == nil {
			continue
		}
		client := p.clientForGroup(sg)
		for _, target := range sgState.Targets {
			// Capture loop variables for the goroutine.
			sgCfg, target := sgCfg, target
			key := probeKey{group: sgCfg.Name, target: target}
			g.Go(func() error {
				if gctx.Err() != nil {
					return nil
				}
				result := p.probeTarget(gctx, client, sgCfg, target)
				p.mu.Lock()
				p.results[key] = result
				p.mu.Unlock()
				return nil
			})
		}
	}
	// Wait for all probes; errors are suppressed (probeTarget never returns
	// one — results carry their own error strings).
	g.Wait() //nolint:errcheck
}

// clientForGroup returns an HTTP client that probes through the server_group's
// own transport, so probes inherit its TLS settings, auth (basic/bearer/SigV4),
// and custom headers and therefore succeed against HTTPS or authenticated
// backends exactly as real queries do. The probe timeout is reapplied via the
// returned client. Falls back to the bare default client when no group transport
// is available (e.g. in tests).
func (p *Prober) clientForGroup(sg *servergroup.ServerGroup) *http.Client {
	if sg == nil {
		return p.client
	}
	return &http.Client{Transport: sg, Timeout: p.timeout}
}

// probeTarget performs a single health check against one target (host:port)
// using the supplied client (built from the server_group's transport).
// The URL is built from sgCfg.GetScheme() + target + sgCfg.PathPrefix, so
// backends with a non-root path prefix (e.g. VictoriaMetrics at
// /select/0/prometheus) are probed at the correct endpoint.
func (p *Prober) probeTarget(ctx context.Context, client *http.Client, sgCfg *servergroup.Config, rawTarget string) ProbeResult {
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

	resp, err := client.Do(req)
	probeAt := time.Now()
	if err != nil {
		errStr := classifyProbeError(err)
		logrus.WithError(err).WithFields(logrus.Fields{"target": rawTarget}).Debugf("proxeusui probe error: %s", errStr)
		return ProbeResult{
			Healthy:     false,
			LastError:   errStr,
			LastProbeAt: probeAt,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// buildinfo non-200 — try /api/v1/labels as a liveness fallback.
		// Drain the buildinfo response so the connection can be reused; the
		// deferred Close above releases it.
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		healthy, fallbackErr := p.checkLabelsEndpoint(ctx, client, base)
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

// classifyProbeError turns a probe transport error into the LastError string
// surfaced to the UI. Context cancellation/deadline and any net.Error
// reporting Timeout() are normalized to "probe timed out" so flaky-network
// blips don't leak raw dial/transport error text into the inventory.
func classifyProbeError(err error) string {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		(errors.As(err, &netErr) && netErr.Timeout()) {
		return "probe timed out"
	}
	return err.Error()
}

// checkLabelsEndpoint hits /api/v1/labels (under the same base URL) to confirm
// the target is reachable. Returns (healthy, errorString).
func (p *Prober) checkLabelsEndpoint(ctx context.Context, client *http.Client, base string) (bool, string) {
	labelsURL := base + "/api/v1/labels"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, labelsURL, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		io.Copy(io.Discard, resp.Body) //nolint:errcheck
		return true, ""
	}
	io.Copy(io.Discard, resp.Body) //nolint:errcheck
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

	for i, sgCfg := range cfg.ServerGroups {
		gi := GroupInfo{
			Name:       sgCfg.Name,
			Ordinal:    i,
			Labels:     labelSetToMap(sgCfg),
			RemoteRead: sgCfg.RemoteRead,
			TimeRange:  timeRangeDescription(sgCfg),
		}

		backendType := sgCfg.BackendType
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
						ProbeResult: result,
						BackendType: backendType,
					})
				}
			}
		}

		inv.Groups = append(inv.Groups, gi)
	}

	return inv
}

// timeRangeDescription renders the time range a server_group is configured to
// answer for, e.g. "now-90d to now-3d" or "2024-01-01T00:00:00Z to +inf".
// Unconfigured bounds are reported as -inf/+inf; the empty string means the
// group has no time range at all and is queried for everything. A group with
// both an absolute and a relative range is filtered by both, so both are
// rendered, joined by "and".
func timeRangeDescription(sgCfg *servergroup.Config) string {
	var ranges []string
	if tr := sgCfg.AbsoluteTimeRangeConfig; tr != nil {
		ranges = append(ranges, absoluteBound(tr.Start, "-inf")+" to "+absoluteBound(tr.End, "+inf"))
	}
	if tr := sgCfg.RelativeTimeRangeConfig; tr != nil {
		ranges = append(ranges, relativeBound(tr.Start, "-inf")+" to "+relativeBound(tr.End, "+inf"))
	}
	return strings.Join(ranges, " and ")
}

func absoluteBound(t time.Time, unset string) string {
	if t.IsZero() {
		return unset
	}
	return t.UTC().Format(time.RFC3339)
}

// relativeBound renders a bound configured as an offset from now, e.g. -90d
// becomes "now-90d".
func relativeBound(d *time.Duration, unset string) string {
	if d == nil {
		return unset
	}
	if *d >= 0 {
		return "now+" + model.Duration(*d).String()
	}
	return "now" + model.Duration(*d).String()
}

func labelSetToMap(sgCfg *servergroup.Config) map[string]string {
	m := make(map[string]string, len(sgCfg.Labels))
	for k, v := range sgCfg.Labels {
		m[string(k)] = string(v)
	}
	return m
}

// backendBaseURL returns the full base URL that proxeus uses to talk to a
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
