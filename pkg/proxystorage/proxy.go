package proxystorage

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/agent"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/logging"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promclient"
	"github.com/pvlltvk/proxeus/pkg/proxyquerier"
	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// noopScrapeManager satisfies remote.ReadyScrapeManager for proxeus, which has
// no local scrape manager. Returning an error here causes upstream's
// remote_write to skip metadata sending, which is the behavior we want.
type noopScrapeManager struct{}

func (noopScrapeManager) Get() (*scrape.Manager, error) {
	return nil, errors.New("proxeus has no scrape manager")
}

// metricNameWorkaroundLabel is a workaround from https://github.com/jacksontj/promxy/issues/274
const metricNameWorkaroundLabel = "__name"

type proxyStorageState struct {
	sgs           []*servergroup.ServerGroup
	client        promclient.API
	cfg           *proxyconfig.Config
	remoteStorage *remote.Storage
	// agentDB writes the remote_write WAL that remoteStorage's queue managers
	// tail. It is nil when no remote_write endpoint is configured.
	agentDB *agent.DB
	// appendable hands out appenders for the rule manager. Backed by agentDB
	// when remote_write is configured, otherwise by a no-op stub.
	appendable     storage.Appendable
	appenderCloser func() error
}

// Ready blocks until all servergroups are ready
func (p *proxyStorageState) Ready() {
	for i, sg := range p.sgs {
		select {
		case <-time.After(time.Second * 5):
			logrus.Debugf("Servergroup %d taking a long time to be Ready (still waiting)", i)
			<-sg.Ready
		case <-sg.Ready:
			continue
		}

	}
}

// Cancel this state
func (p *proxyStorageState) Cancel(n *proxyStorageState) {
	if p.sgs != nil {
		for _, sg := range p.sgs {
			sg.Cancel()
		}
	}
	// Close the remote_write storage (agent WAL + queue managers) unless the
	// new state is reusing the same instance.
	if p.appenderCloser != nil && (n == nil || p.remoteStorage != n.remoteStorage) {
		p.appenderCloser()
	}
}

// NewProxyStorage creates a new ProxyStorage. If localStoragePath is
// non-empty, it is used as the base directory for the remote_write WAL
// (durable across restarts); otherwise a temporary directory is created
// per remote_write configuration and removed on shutdown.
func NewProxyStorage(NoStepSubqueryIntervalFn func(rangeMillis int64) int64, localStoragePath string) (*ProxyStorage, error) {
	return &ProxyStorage{
		NoStepSubqueryIntervalFn: NoStepSubqueryIntervalFn,
		localStoragePath:         localStoragePath,
	}, nil
}

// ProxyStorage implements prometheus' Storage interface
type ProxyStorage struct {
	NoStepSubqueryIntervalFn func(rangeMillis int64) int64
	localStoragePath         string
	state                    atomic.Value
}

// GetState returns the current state of the ProxyStorage
func (p *ProxyStorage) GetState() *proxyStorageState {
	tmp := p.state.Load()
	if sg, ok := tmp.(*proxyStorageState); ok {
		return sg
	}
	return &proxyStorageState{}
}

// ApplyConfig updates the current state of this ProxyStorage
func (p *ProxyStorage) ApplyConfig(c *proxyconfig.Config) error {
	if err := c.Validate(); err != nil {
		return err
	}

	oldState := p.GetState() // Fetch the old state

	failed := false

	// Assign ordinals and default names before anything else, since both the
	// label-uniqueness check below and the server group build loop depend on
	// them.
	for i, sgCfg := range c.ServerGroups {
		sgCfg.Ordinal = i
		if sgCfg.Name == "" {
			sgCfg.Name = fmt.Sprintf("sg-%d", i)
		}
	}

	// Pre-flight: every server_group should carry a unique, non-empty labels set.
	// This is mandatory when cross_group_dedup is on (dedup uses these labels for
	// series identity — a collision would silently merge unrelated series). With
	// dedup off it's only a hygiene concern (ambiguous provenance), so we warn
	// rather than refuse to start, preserving historical proxeus behavior.
	if err := validateUniqueServerGroupLabels(c.ServerGroups); err != nil {
		if c.CrossGroupDedup {
			return err
		}
		logrus.Warnf("%s (not fatal without cross_group_dedup, but provenance will be ambiguous)", err)
	}

	apis := make([]promclient.API, len(c.ServerGroups))
	newState := &proxyStorageState{
		sgs: make([]*servergroup.ServerGroup, len(c.ServerGroups)),
		cfg: c,
	}
	for i, sgCfg := range c.ServerGroups {
		tmp, err := servergroup.NewServerGroup()
		if err != nil {
			failed = true
			logrus.Errorf("Error creating server group (%d): %s", i, err)
			// We can continue to other as we don't need to cancel this as we didn't ApplyConfig()
			continue
		}
		// Once the ServerGroup was created; we want to add this to the lists as there
		// are background goroutines launched (so we'd need to cancel)
		newState.sgs[i] = tmp
		apis[i] = tmp

		if err := tmp.ApplyConfig(sgCfg); err != nil {
			failed = true
			logrus.Errorf("Error applying config to server group (%d): %s", i, err)
		}
	}

	// If there was a failure anywhere, we need to cancel the newState and return an error
	if failed {
		newState.Cancel(nil)
		return fmt.Errorf("error applying config to one or more server group(s)")
	}

	var (
		multiAPI *promclient.MultiAPI
		err      error
	)
	if c.CrossGroupDedup {
		backends := make([]promclient.CrossGroupBackend, len(c.ServerGroups))
		for i, sg := range c.ServerGroups {
			backends[i] = promclient.CrossGroupBackend{
				API:    apis[i],
				Name:   sg.Name,
				Labels: sg.Labels,
			}
		}
		logrus.Infof("cross_group_dedup enabled (metadata_dedup=%t, partial_response=%t, exact_aggregates=%t)",
			c.CrossGroupDedupMetadata, c.CrossGroupPartialResponse, c.CrossGroupExactAggregates)
		multiAPI, err = promclient.NewCrossGroupMultiAPI(backends, promclient.CrossGroupOpts{
			DedupMetadata:      c.CrossGroupDedupMetadata,
			PartialResponse:    c.CrossGroupPartialResponse,
			Collisions:         crossGroupDedupCollisions,
			MetadataCollisions: crossGroupDedupMetadataCollisions,
		})
	} else {
		multiAPI, err = promclient.NewMultiAPI(apis, model.TimeFromUnix(0), false, nil, len(apis), false)
	}
	if err != nil {
		newState.Cancel(nil)
		return err
	}

	newState.client = promclient.NewTimeTruncate(multiAPI)

	// Check for remote_write (for appender)
	if c.PromConfig.RemoteWriteConfigs != nil {
		if oldState.remoteStorage != nil {
			if err := oldState.remoteStorage.ApplyConfig(&c.PromConfig); err != nil {
				return err
			}
			newState.remoteStorage = oldState.remoteStorage
			newState.agentDB = oldState.agentDB
			newState.appendable = oldState.appendable
			newState.appenderCloser = oldState.appenderCloser
		} else {
			walDir := p.localStoragePath
			ephemeral := walDir == ""
			if ephemeral {
				dir, err := os.MkdirTemp("", "proxeus-remote-wal-")
				if err != nil {
					// Minimal images (scratch/distroless) have no writable
					// temp dir, so this fails with a bare "stat /tmp: no such
					// file or directory" that says nothing about the fix.
					return fmt.Errorf("creating a temporary remote_write WAL dir failed (%w); set --storage.path to a writable directory, which is required for remote_write in containers without a writable temp dir", err)
				}
				walDir = dir
			}
			rwLogger := logging.NewLogger(logrus.WithField("component", "remote_write").Logger)
			rs := remote.NewStorage(
				rwLogger,
				prometheus.DefaultRegisterer,
				func() (int64, error) { return 0, nil },
				walDir,
				1*time.Second,
				noopScrapeManager{},
			)
			// proxeus has no local TSDB writing a WAL, so remote.Storage's queue
			// managers have nothing to tail. Run an agent-mode WAL-only DB whose
			// appender writes the WAL that those queue managers consume. Without
			// this the WAL watcher fails with "error tailing WAL ... no such file
			// or directory" and no samples are ever shipped (see issue #771).
			db, err := agent.Open(rwLogger, prometheus.DefaultRegisterer, rs, walDir, agent.DefaultOptions())
			if err != nil {
				if ephemeral {
					os.RemoveAll(walDir)
				}
				return fmt.Errorf("creating remote_write WAL: %w", err)
			}
			// Wake the queue managers' WAL watchers as soon as samples are committed.
			db.SetWriteNotified(rs)
			if err := rs.ApplyConfig(&c.PromConfig); err != nil {
				db.Close()
				if ephemeral {
					os.RemoveAll(walDir)
				}
				return err
			}
			newState.remoteStorage = rs
			newState.agentDB = db
			newState.appendable = db
			newState.appenderCloser = func() error {
				dbErr := db.Close()
				rsErr := rs.Close()
				if ephemeral {
					os.RemoveAll(walDir)
				}
				if dbErr != nil {
					return dbErr
				}
				return rsErr
			}
		}
	} else {
		newState.appendable = appendableStub{}
	}

	newState.Ready()        // Wait for the newstate to be ready
	p.state.Store(newState) // Store the new state
	if oldState != nil {
		oldState.Cancel(newState) // Cancel the old one
	}

	return nil
}

// ConfigHandler is an implementation of the config handler within the prometheus API
func (p *ProxyStorage) ConfigHandler(w http.ResponseWriter, r *http.Request) {
	state := p.GetState()
	v := map[string]interface{}{
		"status": "success",
		"data": map[string]string{
			"yaml": state.cfg.String(),
		},
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(v)
	if err != nil {
		logrus.WithError(err).Error("error marshaling json response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		logrus.WithError(err).WithField("bytesWritten", n).Error("error writing response")
	}
}

// Metadata returns the metric metadata merged across all server_groups.
// The state is resolved per call, so a config reload is picked up.
func (p *ProxyStorage) Metadata(ctx context.Context, metric, limit string) (map[string][]v1.Metadata, error) {
	return p.GetState().client.Metadata(ctx, metric, limit)
}

// MetadataHandler is an implementation of the metadata handler within the prometheus API
func (p *ProxyStorage) MetadataHandler(w http.ResponseWriter, r *http.Request) {
	// Check that "limit" is valid
	var limit *int
	if s := r.FormValue("limit"); s != "" {
		i, err := strconv.Atoi(s)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		limit = &i
	}

	// Do the metadata lookup
	metadata, err := p.Metadata(r.Context(), r.FormValue("metric"), r.FormValue("limit"))

	// Trim the results to the requested limit
	if limit != nil && len(metadata) > *limit {
		count := 0
		for k := range metadata {
			if count < *limit {
				count++
			} else {
				delete(metadata, k)
			}
		}
	}

	var v map[string]interface{}
	if err != nil {
		v = map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}
	} else {
		v = map[string]interface{}{
			"status": "success",
			"data":   metadata,
		}
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	b, err := json.Marshal(v)
	if err != nil {
		logrus.WithError(err).Error("error marshaling json response")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		logrus.WithError(err).WithField("bytesWritten", n).Error("error writing response")
	}

}

// WalReplayHandler implements /api/v1/status/walreplay. Proxeus has no WAL,
// so it reports a steady "fully replayed" state to keep Grafana's health
// checks quiet. Without this override, upstream's handler returns HTTP 500
// with a malformed double-JSON body.
func (p *ProxyStorage) WalReplayHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(`{"status":"success","data":{"min":0,"max":0,"current":0}}`)); err != nil {
		logrus.WithError(err).Error("error writing response")
	}
}

// FlagsHandler implements /api/v1/status/flags. Upstream's handler returns
// proxeus's Go struct field names (e.g. `BindAddr`, `RoutePrefix`) instead of
// Prometheus' dotted CLI names (`web.listen-address`, `web.route-prefix`),
// which breaks any client that probes flags to detect server capabilities.
// Proxeus is not a Prometheus server, so we return an empty data object —
// honest about the absence rather than misleading about its shape.
func (p *ProxyStorage) FlagsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(`{"status":"success","data":{}}`)); err != nil {
		logrus.WithError(err).Error("error writing response")
	}
}

// Querier returns a new Querier on the storage.
func (p *ProxyStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	state := p.GetState()
	return &proxyquerier.ProxyQuerier{
		Start: timestamp.Time(mint).UTC(),
		End:   timestamp.Time(maxt).UTC(),
		// rawAPI counts what local evaluation pulls from the backends; this
		// querier is only used for subtrees NodeReplacer did not push down.
		Client: rawAPI{state.client},

		Cfg: &state.cfg.ProxeusConfig,
	}, nil
}

// StartTime returns the oldest timestamp stored in the storage.
func (p *ProxyStorage) StartTime() (int64, error) {
	return 0, nil
}

// Appender returns a new appender against the storage. When remote_write is
// configured this is backed by the agent WAL (single-use, pooled appender), so
// a fresh appender is returned on every call rather than a shared instance.
func (p *ProxyStorage) Appender(ctx context.Context) storage.Appender {
	return p.GetState().appendable.Appender(ctx)
}

// Close releases the resources of the Querier.
func (p *ProxyStorage) Close() error { return nil }

// ChunkQuerier returns a new ChunkQuerier on the storage.
func (p *ProxyStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("not implemented")
}

// Implement web.LocalStorage
func (p *ProxyStorage) CleanTombstones() (err error) { return nil }
func (p *ProxyStorage) Delete(_ context.Context, mint, maxt int64, ms ...*labels.Matcher) error {
	return nil
}
func (p *ProxyStorage) Snapshot(dir string, withHead bool) error { return nil }
func (p *ProxyStorage) Stats(statsByLabelName string, _ int) (*tsdb.Stats, error) {
	return &tsdb.Stats{IndexPostingStats: &index.PostingsStats{}}, nil
}
func (p *ProxyStorage) WALReplayStatus() (tsdb.WALReplayStatus, error) {
	return tsdb.WALReplayStatus{}, errors.New("not implemented")
}

// ServerGroups returns the resolved server groups from the current state.
// Returns nil if no state has been applied yet.
func (p *ProxyStorage) ServerGroups() []*servergroup.ServerGroup {
	state := p.GetState()
	if state.sgs == nil {
		return nil
	}
	return state.sgs
}

// Config returns the current configuration. Returns nil before the first
// ApplyConfig call.
func (p *ProxyStorage) Config() *proxyconfig.Config {
	state := p.GetState()
	if state.cfg == nil {
		return nil
	}
	return state.cfg
}

// vectorToStepMatrix converts an instant-query SeriesSet (one sample per
// series at the @ time) into a range-query-shaped SeriesSet: each input
// series' single sample is replicated at every step time in [start, end]
// (inclusive of start, inclusive of end when (end-start) is a multiple of
// step). Histograms are replicated the same way. This is only valid when the
// underlying expression is step-invariant — currently used by the NodeReplacer
// when the subtree below has an @ modifier.
//
// The replicated FloatHistogram pointer is shared across steps; that is safe
// because promapi.NewSeries hands out copy-on-read iterators, so the engine
// can't observe an aliased histogram.
func vectorToStepMatrix(vec storage.SeriesSet, start, end time.Time, step time.Duration) storage.SeriesSet {
	if step <= 0 {
		return promapi.NewSeriesSet(nil, vec.Warnings(), vec.Err())
	}
	startMs := timestamp.FromTime(start)
	endMs := timestamp.FromTime(end)
	stepMs := int64(step / time.Millisecond)
	if stepMs <= 0 || endMs < startMs {
		return promapi.NewSeriesSet(nil, vec.Warnings(), vec.Err())
	}
	n := int((endMs-startMs)/stepMs) + 1
	var out []storage.Series
	for vec.Next() {
		src := vec.At()
		lbls := src.Labels().Copy()

		// Instant query → at most one sample per series. Read it (the last
		// one wins if, defensively, more than one is present).
		var (
			haveFloat bool
			fval      float64
			fh        *histogram.FloatHistogram
		)
		it := src.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				_, fval = it.At()
				haveFloat, fh = true, nil
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				_, fh = it.AtFloatHistogram(nil)
				haveFloat = false
			}
		}
		if err := it.Err(); err != nil {
			return promapi.NewSeriesSet(nil, vec.Warnings(), err)
		}

		samples := make([]chunks.Sample, 0, n)
		for i := 0; i < n; i++ {
			ts := startMs + int64(i)*stepMs
			switch {
			case fh != nil:
				samples = append(samples, promapi.HistogramSample(ts, fh))
			case haveFloat:
				samples = append(samples, promapi.FloatSample(ts, fval))
			}
		}
		out = append(out, promapi.NewSeries(lbls, samples))
	}
	return promapi.NewSeriesSet(out, vec.Warnings(), vec.Err())
}

// unwrapParens strips the ParenExpr layers the parser keeps around an
// expression, e.g. the `(("v"))` of count_values((("v")), metric).
func unwrapParens(e parser.Expr) parser.Expr {
	for {
		p, ok := e.(*parser.ParenExpr)
		if !ok {
			return e
		}
		e = p.Expr
	}
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

// validateUniqueServerGroupLabels ensures that every server_group carries a
// non-empty labels set and that no two groups share the same label fingerprint.
// It uses the same model.LabelSet.FastFingerprint algorithm that NewMultiAPI uses
// internally so the check is consistent with the one inside promclient.
//
// Single-group configurations are exempt: with only one group there is no
// cross-group identity to disambiguate and no dedup partner, so empty labels
// are unambiguous.
func validateUniqueServerGroupLabels(groups []*servergroup.Config) error {
	if len(groups) < 2 {
		return nil
	}

	type entry struct {
		name   string
		labels model.LabelSet
	}
	seen := make(map[model.Fingerprint][]entry)

	for _, cfg := range groups {
		if len(cfg.Labels) == 0 {
			return fmt.Errorf(
				"server_group label collision: group %s has empty labels — every server_group must declare a unique non-empty 'labels' set",
				cfg.Name,
			)
		}
		fp := cfg.Labels.FastFingerprint()
		seen[fp] = append(seen[fp], entry{name: cfg.Name, labels: cfg.Labels})
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

// fillStaleNaNGaps inserts StaleNaN markers at the step timestamps where the
// downstream's range response did not include a sample. We need this because
// proxeus substitutes a *parser.Call (e.g. present_over_time, last_over_time)
// with a synthetic VectorSelector whose UnexpandedSeriesSet exposes the
// downstream-computed samples. When the engine re-evaluates that VectorSelector
// it uses the engine-wide lookback (default 5m) to find samples — even though
// the substituted node represents the call's already-computed step-pinned
// output. Without explicit "no value at this step" markers, an isolated sample
// at one step T_k bleeds forward to every subsequent step within the lookback,
// turning sparse outputs (present_over_time returning 1 at only one step) into
// dense ones. StaleNaN at the empty step timestamps tells vectorSelectorSingle
// to bail out at that step (see promql.IsStaleNaN check in engine.go).
//
// startTs/endTs/interval are in milliseconds. startTs and endTs are inclusive.
// If interval is <= 0 (instant query) ss is returned unchanged. Otherwise the
// set is materialized into a fresh, re-iterable copy with the StaleNaN markers
// added — the source cursor is consumed, and the result still flows on to
// UnexpandedSeriesSet.
func fillStaleNaNGaps(ss storage.SeriesSet, startTs, endTs, interval int64) storage.SeriesSet {
	if interval <= 0 {
		return ss
	}
	stale := math.Float64frombits(value.StaleNaN)
	var out []storage.Series
	for ss.Next() {
		s := ss.At()
		lbls := s.Labels().Copy()
		var samples []chunks.Sample
		present := make(map[int64]struct{})
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				t, v := it.At()
				samples = append(samples, promapi.FloatSample(t, v))
				present[t] = struct{}{}
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				t, fh := it.AtFloatHistogram(nil)
				samples = append(samples, promapi.HistogramSample(t, fh))
				present[t] = struct{}{}
			}
		}
		if err := it.Err(); err != nil {
			return promapi.NewSeriesSet(nil, ss.Warnings(), err)
		}

		// Bound the fill defensively: a bogus start/end from upstream
		// shouldn't make us allocate a giant slice. If the eval window is
		// far larger than the actual returned data, skip the fill — the
		// existing samples stay correct (the engine's lookback can still
		// misbehave, but that's strictly no worse than before).
		expected := (endTs-startTs)/interval + 1
		if expected > 0 && expected <= int64(len(present))+10_000 {
			for ts := startTs; ts <= endTs; ts += interval {
				if _, ok := present[ts]; ok {
					continue
				}
				samples = append(samples, promapi.FloatSample(ts, stale))
			}
			// promapi.NewSeries' list iterator must walk samples in
			// timestamp order; the appended StaleNaN points can sit out of
			// order relative to the originals.
			sortSamplesByTime(samples)
		}
		out = append(out, promapi.NewSeries(lbls, samples))
	}
	return promapi.NewSeriesSet(out, ss.Warnings(), ss.Err())
}

// sortSamplesByTime sorts samples by timestamp using insertion sort — a step
// range is typically a handful of points, so a stdlib sort.Slice would pay
// disproportionate cost for the closure call.
func sortSamplesByTime(vs []chunks.Sample) {
	for i := 1; i < len(vs); i++ {
		for j := i; j > 0 && vs[j-1].T() > vs[j].T(); j-- {
			vs[j-1], vs[j] = vs[j], vs[j-1]
		}
	}
}
