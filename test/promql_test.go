package test

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	yaml "gopkg.in/yaml.v2"

	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	_ "github.com/prometheus/prometheus/discovery/install" // Register service discovery implementations.
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/notifications"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/prometheus/promql/parser"

	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/proxystorage"
)

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	parser.EnableExperimentalFunctions = true
}

// Config templates take the bound API address(es) as %s. We pick ports
// dynamically per subtest so concurrent / back-to-back tests don't collide on
// the same TCP port (TIME_WAIT, OS bind races).
const rawPSConfig = `
proxeus:
  http_client:
    tls_config:
      insecure_skip_verify: true
  server_groups:
    - static_configs:
        - targets:
          - %s
`

const rawPSRemoteReadConfig = `
proxeus:
  server_groups:
    - static_configs:
        - targets:
          - %s
      remote_read: true
      http_client:
        tls_config:
          insecure_skip_verify: true
`

const rawDoublePSConfig = `
proxeus:
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        az: a
    - static_configs:
        - targets:
          - %s
      labels:
        az: b
`

const rawDoublePSConfigRR = `
proxeus:
  server_groups:
    - static_configs:
        - targets:
          - %s
      labels:
        az: a
      remote_read: true
    - static_configs:
        - targets:
          - %s
      labels:
        az: b
      remote_read: true
`

func getProxyStorage(cfg string) *proxystorage.ProxyStorage {
	// Create proxeus in front of it
	pstorageConfig := &proxyconfig.Config{}
	if err := yaml.Unmarshal([]byte(cfg), &pstorageConfig); err != nil {
		panic(err)
	}

	ps, err := proxystorage.NewProxyStorage(func(rangeMillis int64) int64 {
		return int64(config.DefaultGlobalConfig.EvaluationInterval) / int64(time.Millisecond)
	}, "")
	if err != nil {
		logrus.Fatalf("Error creating proxy: %v", err)
	}

	if err := ps.ApplyConfig(pstorageConfig); err != nil {
		logrus.Fatalf("Unable to apply config: %v", err)
	}
	return ps
}

// startAPIForTest binds an OS-assigned localhost port and starts a v1 API
// server on it. The listener is bound synchronously before the function
// returns, so callers can issue requests immediately without racing the
// server goroutine. Returns the bound "host:port" plus a Shutdown handle and
// a stop channel that closes once Serve returns.
func startAPIForTest(s storage.Storage) (*http.Server, string, chan struct{}) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Errorf("startAPIForTest: bind: %w", err))
	}
	addr := ln.Addr().String()

	apiRouter := newAPIHandler(s, testAPIEngine(), addr)

	stopChan := make(chan struct{})
	srv := &http.Server{Handler: apiRouter}

	go func() {
		defer close(stopChan)
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Println("Error serving on", addr, err)
		}
	}()

	return srv, addr, stopChan
}

// testAPIEngine is the query engine the test API servers evaluate with. It has
// no NodeReplacer: those servers stand in for plain prometheus backends.
func testAPIEngine() *promql.Engine {
	return promql.NewEngine(promql.EngineOpts{
		Timeout:                  10 * time.Minute,
		MaxSamples:               50000000,
		NoStepSubqueryIntervalFn: func(int64) int64 { return (1 * time.Minute).Milliseconds() },
		EnableAtModifier:         true,
		EnableNegativeOffset:     true,
		EnableDelayedNameRemoval: true,
	})
}

// newAPIHandler builds a prometheus v1 API router over s, evaluating queries
// with eng. addr is only used for the API's global-URL rewriting. Split out of
// startAPIForTest so benchmarks can front a ProxyStorage with an engine that
// has its NodeReplacer wired in -- i.e. a real proxeus API server.
func newAPIHandler(s storage.Storage, eng *promql.Engine, addr string) http.Handler {
	cfgFunc := func() config.Config { return config.DefaultConfig }
	readyFunc := func(f http.HandlerFunc) http.HandlerFunc { return f }
	// nil registerer: this server is created many times per test run and the
	// notification metrics would collide on a shared one.
	notifs := notifications.NewNotifications(16, nil)

	api := v1.NewAPI(
		eng,
		s.(storage.SampleAndChunkQueryable),
		nil, // appendable
		nil, // exemplarQueryable
		nil, // scrapePoolsRetriever
		nil, // targetRetriever
		nil, // alertmanagerRetriever
		cfgFunc,
		nil, // flagsMap
		v1.GlobalURLOptions{
			ListenAddress: addr,
			Host:          "localhost",
			Scheme:        "http",
		},
		readyFunc,
		nil,        // db (TSDBAdminStats)
		"",         // dbDir
		false,      // enableAdmin
		nil,        // logger
		nil,        // rulesRetriever
		50000000,   // remoteReadSampleLimit
		1000,       // remoteReadConcurrencyLimit
		1048576,    // remoteReadMaxBytesInFrame
		false,      // isAgent
		nil,        // corsOrigin
		nil,        // runtimeInfo
		nil,        // buildInfo
		notifs.Get, // notificationsGetter
		notifs.Sub, // notificationsSub
		nil,        // gatherer
		nil,        // registerer
		nil,        // statsRenderer
		false,      // rwEnabled
		nil,        // acceptRemoteWriteProtoMsgs
		false,      // otlpEnabled
		false,      // otlpDeltaToCumulative
		false,      // otlpNativeDeltaIngestion
		false,      // ctZeroIngestionEnabled
	)

	apiRouter := route.New()
	api.Register(apiRouter.WithPrefix("/api/v1"))
	return apiRouter
}

// upstreamSkippedFiles are the upstream fixtures proxeus can't run at all yet,
// each with the gap it waits on.
var upstreamSkippedFiles = map[string]string{
	// Upstream uses StaleNaN to mark a series gone; the v1 API filters stale
	// samples out of range vectors, and even on remote_read the range-eval
	// fan-out emits an extra {__name__="metric"} sample at the boundary that
	// proxeus's merge can't dedupe.
	"staleness.test": "staleness handling: no way to get a raw dump of stale samples through the proxy",
	// 3.x's EnableDelayedNameRemoval interacts with proxeus's
	// metricNameWorkaroundLabel rewrite in ways the rewrite doesn't model.
	"name_label_dropping.test": "__name__ propagation through aggregations isn't modeled by the avg rewrite",
	"duration_expression.test": "3.x duration-expression syntax isn't handled by the pushdown rewrites",
	"type_and_unit.test":       "__type__/__unit__ labels aren't preserved through the rewrite paths",
	// cmd.start = 4s makes the upstream atModifierTestCases sweep issue a
	// range query starting at -59.2s; prometheus/common's
	// model.Time.UnmarshalJSON mis-decodes pre-epoch sub-second timestamps,
	// and proxeus now errors on those rather than returning shifted data. No
	// production query (non-negative timestamps) is affected.
	"collision.test": "the @-modifier sweep generates pre-epoch sub-second range starts, which pushdown rejects",
}

// upstreamRemoteReadOnlyFiles are the fixtures that only pass on the
// remote_read config. Their remaining failures on the HTTP-only config are all
// the same native-histogram fidelity loss: the JSON API encodes a histogram as
// SampleHistogram (flat bucket list, schema collapsed to custom buckets), so
// what comes back can't match the original FloatHistogram. NodeReplacer opts
// out of pushdown for histogram-bearing subtrees, but the GetValue fallback
// still goes over JSON unless the server group has remote_read configured.
var upstreamRemoteReadOnlyFiles = map[string]string{
	"histograms.test":        "histogram samples lose schema fidelity over the JSON API",
	"native_histograms.test": "histogram samples lose schema fidelity over the JSON API",
	"functions.test":         "evals returning native histograms lose schema fidelity over the JSON API",
	"operators.test":         "evals returning native histograms lose schema fidelity over the JSON API",
	"subquery.test":          "evals returning native histograms lose schema fidelity over the JSON API",
	"limit.test":             "evals returning native histograms lose schema fidelity over the JSON API",
}

// excludedEvals lists, per fixture file, the individual eval expressions we
// blank out before running it -- the rest of the file still runs. Keyed by the
// file's base name; fixtures with no entry run whole.
var excludedEvals = map[string][]string{
	"aggregators.test": {
		// eval_fail compares the message verbatim, and proxeus wraps the
		// downstream error ("expanding series: error in servergroup ord=0:
		// ...") around it. Needs the fan-out to unwrap backend query errors.
		`count_values("a\xc5z", version)`,
		// The avg -> sum/count rewrite overflows to +/-Inf on values near
		// MaxFloat64; upstream's avg keeps an incremental mean. Needs a
		// rewrite that doesn't sum first. bigzero sums to 0 or -Inf
		// depending on the order the fan-out returns the series in, so it
		// also fails intermittently.
		`avg(data{test="big"})`,
		`avg(data{test="-big"})`,
		`avg(data{test="bigzero"})`,
	},
}

func TestUpstreamEvaluations(t *testing.T) {
	// ~15m for the two configs, and more memory than `go test -race ./...`
	// leaves us: `make test-upstream` (and the CI job that calls it) set this.
	if os.Getenv("PROXEUS_UPSTREAM_PROMQL") == "" {
		t.Skip("set PROXEUS_UPSTREAM_PROMQL=1 (or run make test-upstream) to run the upstream promql fixtures")
	}

	dir := upstreamTestdataDir(t)
	files, err := filepath.Glob(filepath.Join(dir, "*.test"))
	if err != nil {
		t.Fatal(err)
	}
	// A silently empty suite is worse than a failing one: the glob used to
	// point at ../vendor, which stopped existing when the fork de-vendored.
	if len(files) == 0 {
		t.Fatalf("no upstream .test fixtures found in %s", dir)
	}

	for i, psConfig := range []string{rawPSConfig, rawPSRemoteReadConfig} {
		for _, fn := range files {
			// Name the subtest after the file, not its module-cache path,
			// so the names stay stable (and -run-able) across version bumps.
			base := filepath.Base(fn)
			t.Run(strconv.Itoa(i)+base, func(t *testing.T) {
				if reason, ok := upstreamSkippedFiles[base]; ok {
					t.Skip(reason)
				}
				if reason, ok := upstreamRemoteReadOnlyFiles[base]; ok && psConfig != rawPSRemoteReadConfig {
					t.Skip(reason + " (remote_read config only)")
				}
				runProxyTest(t, fn, psConfig, 1)
			})
		}
	}
}

// excludeEvals blanks out the eval commands of content whose expression is in
// exprs, together with the expectation lines that follow them. Blanking rather
// than deleting keeps the line numbering -- and so the line numbers promqltest
// reports -- intact. An entry that matches nothing is an error: a fixture
// change must not turn an exclusion into a silent no-op.
func excludeEvals(content string, exprs []string) (string, error) {
	if len(exprs) == 0 {
		return content, nil
	}

	indented := func(line string) bool {
		return strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")
	}

	matched := make(map[string]bool, len(exprs))
	lines := strings.Split(content, "\n")
	for i := 0; i < len(lines); i++ {
		if !strings.HasPrefix(lines[i], "eval") {
			continue
		}
		found := false
		for _, expr := range exprs {
			if strings.HasSuffix(lines[i], expr) {
				matched[expr] = true
				found = true
				break
			}
		}
		if !found {
			continue
		}
		// The command owns every indented line below it.
		lines[i] = ""
		for i+1 < len(lines) && indented(lines[i+1]) {
			i++
			lines[i] = ""
		}
	}

	for _, expr := range exprs {
		if !matched[expr] {
			return "", fmt.Errorf("excluded eval %q is not in the fixture anymore", expr)
		}
	}
	return strings.Join(lines, "\n"), nil
}

func TestExcludeEvals(t *testing.T) {
	const content = `load 5m
	metric 1 2 3

eval instant at 5m sum(metric)
	{} 2

eval_fail instant at 5m bad(metric)
  expected_fail_message nope
`

	got, err := excludeEvals(content, []string{"bad(metric)"})
	if err != nil {
		t.Fatalf("excludeEvals: %s", err)
	}
	// The excluded command and its message line are blanked, not removed: the
	// line count -- and so every line number promqltest reports -- is the same.
	want := "load 5m\n\tmetric 1 2 3\n\neval instant at 5m sum(metric)\n\t{} 2\n\n\n\n"
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}

	if _, err := excludeEvals(content, []string{"gone(metric)"}); err == nil {
		t.Error("expected an error for an exclusion that matches nothing")
	}
}

// upstreamTestdataDir returns the promqltest fixture directory of the
// prometheus module this build links against -- the proxeus-prometheus fork,
// via the replace directive -- so the suite keeps working across version bumps.
func upstreamTestdataDir(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("go", "list", "-f", "{{.Dir}}", "github.com/prometheus/prometheus/promql/promqltest").Output()
	if err != nil {
		t.Fatalf("locating the promqltest package: %v", err)
	}
	return filepath.Join(strings.TrimSpace(string(out)), "testdata")
}

func TestEvaluations(t *testing.T) {
	files, err := filepath.Glob("testdata/*.test")
	if err != nil {
		t.Fatal(err)
	}
	for i, psConfig := range []string{rawDoublePSConfig, rawDoublePSConfigRR} {
		for _, fn := range files {
			// Skip files with expectations that assume single-server-group
			// fan-out behavior — they emit 2x sums and `az` labels that the
			// test data does not list. Re-author the expected sets to
			// account for the double-PS doubling before re-enabling.
			if strings.HasSuffix(fn, "aggregators.test") {
				continue
			}
			t.Run(strconv.Itoa(i)+fn, func(t *testing.T) {
				runProxyTest(t, fn, psConfig, 2)
			})
		}
	}
}

// runProxyTest loads a .test file, stands up nServers backend API servers over
// a shared test storage, wires a proxeus ProxyStorage in front (psConfig takes
// nServers address args), and runs the file's eval assertions through the proxy
// engine with pushdown (NodeReplacer) enabled.
//
// v3.5.0 removed the old promqltest.Test struct (SetStorage/QueryEngine/Run), so
// the storage interposition now happens via RunTestWithStorage's newStorage
// hook: load commands append through LayeredStorage to the backend, eval queries
// read back through the proxy.
func runProxyTest(t *testing.T, fn, psConfig string, nServers int) {
	raw, err := os.ReadFile(fn)
	if err != nil {
		t.Skipf("error reading test %s: %s (likely uses syntax not supported by proxeus)", fn, err)
	}
	content, err := excludeEvals(string(raw), excludedEvals[filepath.Base(fn)])
	if err != nil {
		t.Fatalf("excluding evals from %s: %s", fn, err)
	}

	eng := promqltest.NewTestEngine(t, false, 0, 50000000)

	var servers []*http.Server
	var stopChans []chan struct{}
	newStorage := func(tt testutil.T) storage.Storage {
		backend := teststorage.New(tt)
		addrs := make([]interface{}, nServers)
		for i := 0; i < nServers; i++ {
			srv, addr, stopChan := startAPIForTest(backend)
			servers = append(servers, srv)
			stopChans = append(stopChans, stopChan)
			addrs[i] = addr
		}
		ps := getProxyStorage(fmt.Sprintf(psConfig, addrs...))
		eng.NodeReplacer = ps.NodeReplacer
		return &LayeredStorage{ps, backend}
	}

	promqltest.RunTestWithStorage(t, content, eng, newStorage)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	for _, srv := range servers {
		_ = srv.Shutdown(ctx)
	}
	for _, stopChan := range stopChans {
		<-stopChan
	}
}

// Create a wrapper for the storage that will proxy reads but not writes

type LayeredStorage struct {
	proxyStorage storage.Storage
	baseStorage  storage.Storage
}

func (p *LayeredStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	return p.proxyStorage.Querier(mint, maxt)
}
func (p *LayeredStorage) StartTime() (int64, error) {
	return p.baseStorage.StartTime()
}

func (p *LayeredStorage) Appender(ctx context.Context) storage.Appender {
	return p.baseStorage.Appender(ctx)
}
func (p *LayeredStorage) Close() error {
	return p.baseStorage.Close()
}
func (p *LayeredStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return p.baseStorage.ChunkQuerier(mint, maxt)
}
