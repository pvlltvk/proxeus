package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/golang/glog"
	"github.com/grafana/regexp"
	"github.com/jessevdk/go-flags"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	_ "github.com/prometheus/prometheus/discovery/install" // Register service discovery implementations.
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/notifier"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	promlogging "github.com/prometheus/prometheus/util/logging"
	"github.com/prometheus/prometheus/util/notifications"
	"github.com/prometheus/prometheus/util/strutil"
	"github.com/prometheus/prometheus/web"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"k8s.io/klog"

	"github.com/pvlltvk/proxeus/pkg/alertbackfill"
	"github.com/pvlltvk/proxeus/pkg/auth"
	proxyconfig "github.com/pvlltvk/proxeus/pkg/config"
	"github.com/pvlltvk/proxeus/pkg/federate"
	"github.com/pvlltvk/proxeus/pkg/logging"
	"github.com/pvlltvk/proxeus/pkg/mantineui"
	"github.com/pvlltvk/proxeus/pkg/middleware"
	"github.com/pvlltvk/proxeus/pkg/proxeusui"
	"github.com/pvlltvk/proxeus/pkg/proxystorage"
	"github.com/pvlltvk/proxeus/pkg/server"

	injectionUI "github.com/pvlltvk/proxeus/cmd/proxeus/ui"
)

var (
	configSuccess = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_successful",
		Help: "Whether the last configuration reload attempt was successful.",
	})
	configSuccessTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "prometheus_config_last_reload_success_timestamp_seconds",
		Help: "Timestamp of the last successful configuration reload.",
	})

	reloadTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "process_reload_time_seconds",
		Help: "Last reload (SIGHUP) time of the process since unix epoch in seconds.",
	})
)

func init() {
	prometheus.MustRegister(versioncollector.NewCollector("proxeus"))
}

type cliOpts struct {
	Version     bool `long:"version" short:"v" description:"print out version and exit"`
	CheckConfig bool `long:"check-config" description:"check config files and exit"`

	BindAddr         string `long:"bind-addr" description:"address for proxeus to listen on" default:":8082"`
	ConfigFile       string `long:"config" description:"path to the config file" default:"config.yaml"`
	LogLevel         string `long:"log-level" description:"Log level" default:"info"`
	LogFormat        string `long:"log-format" description:"Log format(text|json)" default:"text"`
	LogMaxFormPrefix int    `long:"log-max-form-prefix" description:"Max prefix for form values in log entries" default:"256"`

	WebConfigFile      string        `long:"web.config.file" description:"[EXPERIMENTAL] Path to a Prometheus-format web config file (TLS, HTTP headers, basic auth users). See https://prometheus.io/docs/prometheus/latest/configuration/https/ for the schema."`
	WebCORSOriginRegex string        `long:"web.cors.origin" description:"Regex for CORS origin. It is fully anchored." default:".*"`
	WebReadTimeout     time.Duration `long:"web.read-timeout" description:"Maximum duration before timing out read of the request, and closing idle connections." default:"5m"`

	MetricsPath  string   `long:"metrics-path" description:"URL path for the prometheus metrics endpoint." default:"/metrics"`
	ProxyHeaders []string `long:"proxy-headers" env:"PROXY_HEADERS" description:"a list of headers to proxy to downstream servergroups."`

	ExternalURL     string `long:"web.external-url" description:"The URL under which Prometheus is externally reachable (for example, if Prometheus is served via a reverse proxy). Used for generating relative and absolute links back to Prometheus itself. If the URL has a path portion, it will be used to prefix all HTTP endpoints served by Prometheus. If omitted, relevant URL components will be derived automatically."`
	RoutePrefix     string `long:"web.route-prefix" description:"Prefix for the internal routes of web endpoints. Defaults to path of --web.external-url."`
	EnableLifecycle bool   `long:"web.enable-lifecycle" description:"Enable shutdown and reload via HTTP request."`

	MaxNotificationsSubscribers int `long:"web.max-notifications-subscribers" description:"Limits the maximum number of subscribers that can concurrently receive live notifications via the web UI." default:"16"`

	QueryTimeout        time.Duration `long:"query.timeout" description:"Maximum time a query may take before being aborted." default:"2m"`
	QueryMaxSamples     int           `long:"query.max-samples" description:"Maximum number of samples a single query can load into memory. Note that queries will fail if they would load more samples than this into memory, so this also limits the number of samples a query can return." default:"50000000"`
	QueryLookbackDelta  time.Duration `long:"query.lookback-delta" description:"The maximum lookback duration for retrieving metrics during expression evaluations." default:"5m"`
	QueryMaxConcurrency int           `long:"query.max-concurrency" default:"-1" description:"Maximum number of queries executed concurrently."`
	StoragePath         string        `long:"storage.path" description:"Base directory for proxeus's local working state (active query tracker file, remote_write WAL)."`
	LegacyStoragePath   string        `long:"storage.tsdb.path" description:"DEPRECATED: use --storage.path instead. (Proxeus has no TSDB; this flag is misnamed.)"`

	RemoteReadMaxConcurrency int `long:"remote-read.max-concurrency" description:"Maximum number of concurrent remote read calls." default:"10"`

	NotificationQueueCapacity int           `long:"alertmanager.notification-queue-capacity" description:"The capacity of the queue for pending alert manager notifications." default:"10000"`
	AccessLogDestination      string        `long:"access-log-destination" description:"where to log access logs, options (none, stderr, stdout)" default:"stdout"`
	ForOutageTolerance        time.Duration `long:"rules.alert.for-outage-tolerance" description:"Max time to tolerate prometheus outage for restoring for state of alert." default:"1h"`
	ForGracePeriod            time.Duration `long:"rules.alert.for-grace-period" description:"Minimum duration between alert and restored for state. This is maintained only for alerts with configured for time greater than grace period." default:"10m"`
	ResendDelay               time.Duration `long:"rules.alert.resend-delay" description:"Minimum amount of time to wait before resending an alert to Alertmanager." default:"1m"`
	AlertBackfill             bool          `long:"rules.alertbackfill" description:"Enable proxeus to recalculate alert state on startup when the downstream datastore doesn't have an ALERTS_FOR_STATE"`

	ShutdownDelay   time.Duration `long:"http.shutdown-delay" description:"time to wait before shutting down the http server, this allows for a grace period for upstreams (e.g. LoadBalancers) to discover the new stopping status through healthchecks" default:"10s"`
	ShutdownTimeout time.Duration `long:"http.shutdown-timeout" description:"max time to wait for a graceful shutdown of the HTTP server" default:"60s"`
}

func (c *cliOpts) ToFlags() map[string]string {
	tmp := make(map[string]string)
	// TODO: better
	b, _ := json.Marshal(c)
	json.Unmarshal(b, &tmp)
	return tmp
}

var opts cliOpts

func reloadConfig(noStepSuqueryInterval *safePromQLNoStepSubqueryInterval, notifs *notifications.Notifications, rls ...proxyconfig.Reloadable) (err error) {
	defer func() {
		if err == nil {
			configSuccess.Set(1)
			configSuccessTime.SetToCurrentTime()
			notifs.DeleteNotification(notifications.ConfigurationUnsuccessful)
		} else {
			configSuccess.Set(0)
			notifs.AddNotification(notifications.ConfigurationUnsuccessful)
		}
	}()

	cfg, err := proxyconfig.ConfigFromFile(opts.ConfigFile)
	if err != nil {
		return fmt.Errorf("error loading cfg: %v", err)
	}

	failed := false
	for _, rl := range rls {
		if err := rl.ApplyConfig(cfg); err != nil {
			logrus.Errorf("failed to apply configuration: %v", err)
			failed = true
		}
	}

	if failed {
		return fmt.Errorf("one or more errors occurred while applying new configuration")
	}
	noStepSuqueryInterval.Set(cfg.PromConfig.GlobalConfig.EvaluationInterval)
	reloadTime.Set(float64(time.Now().Unix()))
	return nil
}

// loadReactScripts reads the embedded head.js/nav.js files used to inject
// proxeus-specific behavior into the Mantine UI and wraps each in a <script>
// tag. The embedded files are part of the binary, so a read failure here
// indicates a build problem and is treated as a hard startup error by the
// caller.
func loadReactScripts() (headScript, navScript string, err error) {
	head, err := injectionUI.Scripts.ReadFile("head.js")
	if err != nil {
		return "", "", fmt.Errorf("embedded head.js missing: %w", err)
	}
	nav, err := injectionUI.Scripts.ReadFile("nav.js")
	if err != nil {
		return "", "", fmt.Errorf("embedded nav.js missing: %w", err)
	}
	return "<script>" + string(head) + "</script>", "<script>" + string(nav) + "</script>", nil
}

// reactRouteSet is the set of exact path segments that the Mantine UI handles.
// These match newUIReactRouterPaths + newUIReactRouterServerPaths in web/web.go.
var reactRouteSet = map[string]bool{
	"/config":                 true,
	"/flags":                  true,
	"/service-discovery":      true,
	"/alertmanager-discovery": true,
	"/status":                 true,
	"/targets":                true,
	"/alerts":                 true,
	"/query":                  true,
	"/rules":                  true,
	"/tsdb-status":            true,
	"/agent":                  true,
	"/backends":               true,
}

// isReactRoute reports whether urlPath (the full, possibly route-prefixed
// request path) targets one of the Mantine UI routes. It strips routePrefix
// before comparing against the unprefixed reactRouteSet keys. At the root
// (routePrefix "/" or ""), the trim is a no-op so "/query" matches as before.
func isReactRoute(routePrefix, urlPath string) bool {
	trimmed := strings.TrimRight(routePrefix, "/")
	rel := strings.TrimPrefix(urlPath, trimmed)
	// Under a non-root prefix, require that the prefix was actually present;
	// otherwise an unprefixed "/query" would be misrouted to the React app.
	if trimmed != "" && rel == urlPath {
		return false
	}
	return reactRouteSet[rel]
}

// checkedReplaceAll behaves like bytes.ReplaceAll, but if marker is not
// present in src it logs a loud warning naming the missing marker before
// returning src unchanged. This guards against a future upstream Mantine
// build renaming or removing a placeholder/injection point we depend on,
// which would otherwise cause the substitution to silently no-op.
func checkedReplaceAll(src, marker, replacement []byte, what string) []byte {
	if !bytes.Contains(src, marker) {
		logrus.Warnf("React index.html missing expected marker %q (%s); proxeus UI customization will not be applied", marker, what)
		return src
	}
	return bytes.ReplaceAll(src, marker, replacement)
}

// buildInjectedReactApp opens index.html from the embedded Mantine UI assets,
// applies the same placeholder substitutions that upstream web.Handler does,
// and injects navScript immediately before </body>. The result depends only
// on opts (and the embedded head/nav scripts), which are fixed for the
// process lifetime, so callers compute it once at startup and serve the same
// bytes for every request via serveInjectedReactApp.
func buildInjectedReactApp(opts *web.Options, headScript, navScript string) ([]byte, error) {
	const indexPath = "/index.html"
	f, err := mantineui.Assets.Open(indexPath)
	if err != nil {
		// Assets not built (stub FS). The caller falls back to an error
		// response — the upstream handler would produce its own, more
		// informative message in this case.
		return nil, fmt.Errorf("error opening React index.html: %w", err)
	}
	defer f.Close()
	idx, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("error reading React index.html: %w", err)
	}

	// Apply the same placeholder replacements as upstream serveReactApp.
	consolesPath := ""
	if opts.ExternalURL != nil {
		if _, statErr := os.Stat(opts.ConsoleTemplatesPath + "/index.html"); !os.IsNotExist(statErr) {
			consolesPath = opts.ExternalURL.Path + "/consoles/index.html"
		}
	}
	idx = checkedReplaceAll(idx, []byte("CONSOLES_LINK_PLACEHOLDER"), []byte(consolesPath), "consoles link placeholder")
	idx = checkedReplaceAll(idx, []byte("TITLE_PLACEHOLDER"), []byte(opts.PageTitle), "title placeholder")
	idx = checkedReplaceAll(idx, []byte("AGENT_MODE_PLACEHOLDER"), []byte(strconv.FormatBool(opts.IsAgent)), "agent mode placeholder")
	idx = checkedReplaceAll(idx, []byte("READY_PLACEHOLDER"), []byte("true"), "ready placeholder")
	lookbackStr := model.Duration(opts.LookbackDelta).String()
	idx = checkedReplaceAll(idx, []byte("LOOKBACKDELTA_PLACEHOLDER"), []byte(lookbackStr), "lookback delta placeholder")

	// prefix is "" at root and "/foo" under -web.route-prefix=/foo. All
	// absolute UI/asset paths below are built from it so the page works
	// whether served at the root or at a sub-path.
	prefix := strings.TrimRight(opts.RoutePrefix, "/")

	// Replace Prometheus favicon with Proxeus icon.
	idx = checkedReplaceAll(idx, []byte(`href="./favicon.svg"`), []byte(`href="`+prefix+`/proxeus/static/proxeus-icon.svg"`), "favicon link")

	// Make asset URLs absolute so they resolve correctly when the page is
	// served at a sub-path like /backends. Under a route prefix the upstream
	// web handler serves the bundled assets at <prefix>/assets/.
	idx = checkedReplaceAll(idx, []byte(`"./assets/`), []byte(`"`+prefix+`/assets/`), "double-quoted asset path prefix")
	// The single-quoted form is an optional defensive variant: Vite emits the
	// entry script/style with double quotes, so this form is normally absent.
	// Rewrite it when present, but unlike the double-quoted form its absence is
	// expected and harmless, so use a plain ReplaceAll that does not warn.
	idx = bytes.ReplaceAll(idx, []byte(`'./assets/`), []byte(`'`+prefix+`/assets/`))

	// Expose the route prefix to the injected scripts so they can build their
	// own absolute paths. Must be injected before headScript so head.js
	// can read it.
	prefixScript := fmt.Sprintf("<script>window.__PROXEUS_ROUTE_PREFIX__=%q;</script>", prefix)

	// Inject head script (blocks React Router navigation on /backends).
	idx = checkedReplaceAll(idx, []byte("<head>"), []byte("<head>"+prefixScript+headScript), "<head> injection point")

	// Inject our nav script just before </body>.
	idx = checkedReplaceAll(idx, []byte("</body>"), []byte(navScript+"</body>"), "</body> injection point")

	return idx, nil
}

// serveInjectedReactApp writes the precomputed injected Mantine UI page (from
// buildInjectedReactApp), or replays its build error as a 500 if the page
// could not be built at startup.
func serveInjectedReactApp(w http.ResponseWriter, page []byte, buildErr error) {
	if buildErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, buildErr.Error())
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(page)
}

func main() {
	// Wait for reload or termination signals. Start the handler for SIGHUP as
	// early as possible, but ignore it until we are ready to handle reloading
	// our config.
	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)

	reloadables := make([]proxyconfig.Reloadable, 0)

	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		// If the error was from the parser, then we can simply return
		// as Parse() prints the error already
		if _, ok := err.(*flags.Error); ok {
			os.Exit(1)
		}
		logrus.Fatalf("error parsing flags: %v", err)
	}

	if opts.Version {
		fmt.Println(version.Print("proxeus"))
		os.Exit(0)
	}

	if opts.LegacyStoragePath != "" {
		if opts.StoragePath != "" {
			logrus.Fatalf("--storage.tsdb.path and --storage.path are mutually exclusive; --storage.tsdb.path is deprecated, use --storage.path")
		}
		logrus.Warnf("--storage.tsdb.path is deprecated; use --storage.path instead")
		opts.StoragePath = opts.LegacyStoragePath
	}

	// CheckConfig simply will load the config, check for errors, and exit
	if opts.CheckConfig {
		if _, err := proxyconfig.ConfigFromFile(opts.ConfigFile); err != nil {
			logrus.Fatalf("Error loading cfg: %v", err)
		}
		fmt.Printf("%s if valid proxeus config file syntax\n", opts.ConfigFile)
		os.Exit(0)
	}

	logging.SetMaxFormPrefix(opts.LogMaxFormPrefix)

	// Use log level
	level, err := logrus.ParseLevel(opts.LogLevel)
	if err != nil {
		logrus.Fatalf("Unknown log level %s: %v", opts.LogLevel, err)
	}
	logrus.SetLevel(level)

	var formatter logrus.Formatter
	switch opts.LogFormat {
	case "json":
		formatter = &logrus.JSONFormatter{}
	default:
		// Set the log format to have a reasonable timestamp
		formatter = &logrus.TextFormatter{
			FullTimestamp: true,
		}
	}

	logrus.SetFormatter(formatter)

	// Above level 6, the k8s client would log bearer tokens in clear-text.
	glog.ClampLevel(6)
	glog.SetLogger(logging.NewGoKitLogger(logrus.WithField("component", "k8s_client_runtime")))

	// Above level 6, the k8s client would log bearer tokens in clear-text.
	klog.ClampLevel(6)
	klog.SetLogger(logging.NewGoKitLogger(logrus.WithField("component", "k8s_client_runtime")))

	// Create base context for this daemon
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	noStepSubqueryInterval := &safePromQLNoStepSubqueryInterval{}
	noStepSubqueryInterval.Set(config.DefaultGlobalConfig.EvaluationInterval)

	// Reload ready -- channel to close once we are ready to start reloaders
	reloadReady := make(chan struct{})

	// Create the proxy storage
	var proxyStorage storage.Storage

	ps, err := proxystorage.NewProxyStorage(noStepSubqueryInterval.Get, opts.StoragePath)
	if err != nil {
		logrus.Fatalf("Error creating proxy: %v", err)
	}
	reloadables = append(reloadables, ps)
	proxyStorage = ps

	// All prometheus libraries (notifier, scrape, web, discovery,
	// promql.NewActiveQueryTracker) take an *slog.Logger. Bridge those
	// through logrus so proxeus's user-facing logging configuration
	// (level, format, fields) governs both proxeus's own output and the
	// embedded prometheus-library output.
	logger := logging.NewLogger(logrus.StandardLogger())

	engineOpts := promql.EngineOpts{
		Reg:                      prometheus.DefaultRegisterer,
		Timeout:                  opts.QueryTimeout,
		MaxSamples:               opts.QueryMaxSamples,
		NoStepSubqueryIntervalFn: noStepSubqueryInterval.Get,
		LookbackDelta:            opts.QueryLookbackDelta,

		// EnableAtModifier and EnableNegativeOffset have to be
		// always on for regular PromQL as of Prometheus v2.33.
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	}

	if opts.QueryMaxConcurrency != -1 {
		if opts.StoragePath == "" {
			logrus.Fatalf("--storage.path must be set if you wish to enable max query concurrency limits")
		}
		engineOpts.ActiveQueryTracker = promql.NewActiveQueryTracker(opts.StoragePath, opts.QueryMaxConcurrency, logger.With("component", "activeQueryTracker"))
	}

	engine := promql.NewEngine(engineOpts)
	engine.NodeReplacer = ps.NodeReplacer

	externalUrl, err := computeExternalURL(opts.ExternalURL, opts.BindAddr)
	if err != nil {
		logrus.Fatalf("Unable to parse external URL %s", "tmp")
	}

	// Alert notifier
	notifierManager := notifier.NewManager(
		&notifier.Options{
			Registerer:    prometheus.DefaultRegisterer,
			QueueCapacity: opts.NotificationQueueCapacity,
		},
		logger.With("component", "notifier"),
	)
	reloadables = append(reloadables, proxyconfig.WrapPromReloadable(notifierManager))

	notifyDiscoverySDMetrics, err := discovery.RegisterSDMetrics(prometheus.DefaultRegisterer, discovery.NewRefreshMetrics(prometheus.DefaultRegisterer))
	if err != nil {
		logrus.Fatalf("Error registering SD metrics: %v", err)
	}
	discoveryManagerNotify := discovery.NewManager(ctx, logger.With("component", "discovery manager notify"), prometheus.DefaultRegisterer, notifyDiscoverySDMetrics)
	if discoveryManagerNotify == nil {
		logrus.Fatalf("Error creating notify discovery manager")
	}

	reloadables = append(reloadables,
		proxyconfig.WrapPromReloadable(&proxyconfig.ApplyConfigFunc{func(cfg *config.Config) error {
			c := make(map[string]discovery.Configs)
			for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
				c[k] = v.ServiceDiscoveryConfigs
			}
			return discoveryManagerNotify.ApplyConfig(c)
		}}),
	)

	go func() {
		if err := discoveryManagerNotify.Run(); err != nil {
			logrus.Errorf("Error running Notify discovery manager: %v", err)
		} else {
			logrus.Infof("Notify discovery manager stopped")
		}
	}()
	go func() {
		<-reloadReady
		notifierManager.Run(discoveryManagerNotify.SyncCh())
		logrus.Infof("Notifier manager stopped")
	}()

	var ruleQueryable storage.Queryable
	// If alertbackfill is enabled; wire it up!
	if opts.AlertBackfill {
		ruleQueryable = alertbackfill.NewAlertBackfillQueryable(engine, proxyStorage)
	} else {
		ruleQueryable = proxyStorage
	}
	ruleManager := rules.NewManager(&rules.ManagerOptions{
		Context:         ctx,         // base context for all background tasks
		ExternalURL:     externalUrl, // URL listed as URL for "who fired this alert"
		QueryFunc:       rules.EngineQueryFunc(engine, proxyStorage),
		NotifyFunc:      sendAlerts(notifierManager, externalUrl.String()),
		Appendable:      proxyStorage,
		Queryable:       ruleQueryable,
		Logger:          logger,
		Registerer:      prometheus.DefaultRegisterer,
		OutageTolerance: opts.ForOutageTolerance,
		ForGracePeriod:  opts.ForGracePeriod,
		ResendDelay:     opts.ResendDelay,
	})

	if q, ok := ruleQueryable.(*alertbackfill.AlertBackfillQueryable); ok {
		q.SetRuleGroupFetcher(ruleManager.RuleGroups)
	}

	go ruleManager.Run()

	reloadables = append(reloadables, proxyconfig.WrapPromReloadable(&proxyconfig.ApplyConfigFunc{func(cfg *config.Config) error {
		// Get all rule files matching the configuration oaths.
		var files []string
		for _, pat := range cfg.RuleFiles {
			fs, err := filepath.Glob(pat)
			if err != nil {
				// The only error can be a bad pattern.
				return fmt.Errorf("error retrieving rule files for %s: %s", pat, err)
			}
			files = append(files, fs...)
		}
		if err := ruleManager.Update(time.Duration(cfg.GlobalConfig.EvaluationInterval), files, cfg.GlobalConfig.ExternalLabels, externalUrl.String(), nil); err != nil {
			return err
		}

		if cfg.RemoteWriteConfigs == nil {
			ruleList := ruleManager.Rules()
			// check for any recording rules, if we find any lets log a fatal and stop
			for _, rule := range ruleList {
				if _, ok := rule.(*rules.RecordingRule); ok {
					return fmt.Errorf("proxeus doesn't support recording rules: %s", rule)
				}
			}

			if len(ruleList) > 0 {
				logrus.Warning("Alerting rules are configured but no remote_write endpoint is configured.")
			}
		}

		return nil
	}}))

	// PromQL query engine reloadable
	reloadables = append(reloadables, proxyconfig.WrapPromReloadable(&proxyconfig.ApplyConfigFunc{func(cfg *config.Config) error {
		if cfg.GlobalConfig.QueryLogFile == "" {
			engine.SetQueryLogger(nil)
			return nil
		}

		l, err := promlogging.NewJSONFileLogger(cfg.GlobalConfig.QueryLogFile)
		if err != nil {
			return err
		}
		engine.SetQueryLogger(l)

		return nil
	}}))

	// We need an empty scrape manager, simply to make the API not panic and error out
	scrapeManager, err := scrape.NewManager(nil, logger.With("component", "scrape manager"), nil, nil, prometheus.DefaultRegisterer)
	if err != nil {
		logrus.Fatalf("Error creating scrape manager: %v", err)
	}

	// Notifications backing /api/v1/notifications and its SSE stream. The
	// Mantine UI polls both on every page load, and web.New() passes these
	// straight through to the API handlers, so leaving them nil panics on
	// each request. proxeus has no WAL to replay, so unlike Prometheus it
	// starts with no "starting up" notification.
	notifs := notifications.NewNotifications(opts.MaxNotificationsSubscribers, prometheus.DefaultRegisterer)

	webOptions := &web.Options{
		Registerer:      prometheus.DefaultRegisterer,
		Gatherer:        prometheus.DefaultGatherer,
		Context:         ctx,
		Storage:         proxyStorage,
		LocalStorage:    ps,
		ExemplarStorage: ps,
		QueryEngine:     engine,
		ScrapeManager:   scrapeManager,
		RuleManager:     ruleManager,
		Notifier:        notifierManager,
		LookbackDelta:   opts.QueryLookbackDelta,

		RemoteReadConcurrencyLimit: opts.RemoteReadMaxConcurrency,

		EnableLifecycle: opts.EnableLifecycle,

		NotificationsGetter: notifs.Get,
		NotificationsSub:    notifs.Sub,

		// Prometheus' web.New() indexes ListenAddresses[0] unconditionally when
		// constructing GlobalURLOptions; proxeus doesn't use the embedded
		// listeners (it has its own server), but the slice still must be set
		// or startup panics.
		ListenAddresses: []string{opts.BindAddr},

		Flags:       opts.ToFlags(),
		PageTitle:   "Proxeus",
		RoutePrefix: opts.RoutePrefix,
		ExternalURL: externalUrl,
		Version: &web.PrometheusVersion{
			Version:   version.Version,
			Revision:  version.Revision,
			Branch:    version.Branch,
			BuildUser: version.BuildUser,
			BuildDate: version.BuildDate,
			GoVersion: version.GoVersion,
		},
	}

	webOptions.CORSOrigin, err = compileCORSRegexString(opts.WebCORSOriginRegex)
	if err != nil {
		logrus.Fatalf("Error parsing CORS regex: %v", err)
	}

	// Default -web.route-prefix to path of -web.external-url.
	if webOptions.RoutePrefix == "" {
		webOptions.RoutePrefix = externalUrl.Path
	}
	// RoutePrefix must always be at least '/'.
	webOptions.RoutePrefix = "/" + strings.Trim(webOptions.RoutePrefix, "/")

	webHandler := web.New(logger, webOptions)
	reloadables = append(reloadables, proxyconfig.WrapPromReloadable(webHandler))
	webHandler.SetReady(web.Ready)

	// Start the internal web handler on a loopback listener so that
	// web.Handler.Run() registers all standard Prometheus routes
	// (/api/v1/*, /-/ready, /version, /federate, etc.) without us needing
	// to reach into its unexported router or apiV1 fields.
	// proxeus reverse-proxies unhandled requests to this internal server.
	internalLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		logrus.Fatalf("Error creating internal web listener: %v", err)
	}
	internalURL, _ := url.Parse("http://" + internalLn.Addr().String())
	webProxy := httputil.NewSingleHostReverseProxy(internalURL)
	go func() {
		if runErr := webHandler.Run(ctx, []net.Listener{internalLn}, ""); runErr != nil {
			logrus.Errorf("internal web handler: %v", runErr)
		}
	}()

	// Create the proxeus inventory UI handler.
	proxeusUIHandler, err := proxeusui.NewHandler(ps, webOptions.RoutePrefix)
	if err != nil {
		logrus.Fatalf("Error creating proxeus UI handler: %v", err)
	}
	// Tie the prober loop to the main context so it stops on shutdown.
	go proxeusUIHandler.Run(ctx)

	// Precompute the injected Mantine UI page once: it depends only on
	// webOptions and the embedded head/nav scripts, both fixed for the
	// process lifetime.
	reactHeadScript, reactNavScript, err := loadReactScripts()
	if err != nil {
		logrus.Fatalf("Error loading embedded UI scripts: %v", err)
	}
	injectedReactPage, injectedReactPageErr := buildInjectedReactApp(webOptions, reactHeadScript, reactNavScript)

	// Precompute the route-prefixed paths matched in r.NotFound below;
	// webOptions.RoutePrefix never changes after startup.
	debugPathPrefix := path.Join(webOptions.RoutePrefix, "/debug")
	readyPath := path.Join(webOptions.RoutePrefix, "/-/ready")
	configPath := path.Join(webOptions.RoutePrefix, "/api/v1/status/config")
	metadataPath := path.Join(webOptions.RoutePrefix, "/api/v1/metadata")
	walReplayPath := path.Join(webOptions.RoutePrefix, "/api/v1/status/walreplay")
	flagsPath := path.Join(webOptions.RoutePrefix, "/api/v1/status/flags")
	proxeusPathPrefix := path.Join(webOptions.RoutePrefix, "/proxeus")
	debugStripPrefix := strings.Trim(webOptions.RoutePrefix, "/")
	// The injected index.html references bundled assets at <prefix>/assets/*
	// (see buildInjectedReactApp). Serve them from the proxeus-owned Mantine
	// embed, which is rooted at the mantine-ui dir so "/assets/<file>" resolves.
	// Unlike debugStripPrefix, this keeps the leading slash: after stripping the
	// route prefix the path must still start with "/" (e.g. "" at root, "/foo"
	// under -web.route-prefix=/foo).
	mantineAssetsPathPrefix := path.Join(webOptions.RoutePrefix, "/assets")
	mantineAssetsStripPrefix := strings.TrimRight(webOptions.RoutePrefix, "/")
	mantineAssetsServer := http.StripPrefix(mantineAssetsStripPrefix, http.FileServer(mantineui.Assets))

	// proxeus's own /federate handler: a faster encoder for the common
	// text/plain path (issue #784) that delegates other formats to the vendored
	// handler. Kept in sync with the configured external_labels on reload.
	federateHandler := federate.New(ps, opts.QueryLookbackDelta, webProxy)
	reloadables = append(reloadables, proxyconfig.WrapPromReloadable(&proxyconfig.ApplyConfigFunc{F: func(cfg *config.Config) error {
		federateHandler.SetExternalLabels(cfg.GlobalConfig.ExternalLabels)
		return nil
	}}))

	// Create our router
	r := httprouter.New()

	r.HandlerFunc("GET", opts.MetricsPath, promhttp.Handler().ServeHTTP)
	r.HandlerFunc("GET", path.Join(webOptions.RoutePrefix, "/federate"), federateHandler.ServeHTTP)

	stopping := false
	r.NotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Have our fallback rules
		if strings.HasPrefix(r.URL.Path, debugPathPrefix) {
			http.StripPrefix(debugStripPrefix, http.DefaultServeMux).ServeHTTP(w, r)
		} else if r.URL.Path == readyPath {
			if stopping {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprintf(w, "Proxeus is Stopping.\n")
			} else {
				webProxy.ServeHTTP(w, r)
			}
		} else if r.URL.Path == configPath {
			ps.ConfigHandler(w, r)
		} else if r.URL.Path == metadataPath {
			ps.MetadataHandler(w, r)
		} else if r.URL.Path == walReplayPath {
			ps.WalReplayHandler(w, r)
		} else if r.URL.Path == flagsPath {
			ps.FlagsHandler(w, r)
		} else if isReactRoute(webOptions.RoutePrefix, r.URL.Path) {
			// Serve Mantine index.html with our nav injection.
			// This must come before the /proxeus prefix check so that
			// /backends is served through the Mantine shell.
			serveInjectedReactApp(w, injectedReactPage, injectedReactPageErr)
		} else if strings.HasPrefix(r.URL.Path, proxeusPathPrefix) {
			proxeusUIHandler.ServeHTTP(w, r)
		} else if strings.HasPrefix(r.URL.Path, mantineAssetsPathPrefix) {
			// Mantine JS/CSS chunks referenced by the injected index.html
			// above. Served from the proxeus-owned embed since the slim
			// prometheus fork's web/ui has no built-in assets.
			mantineAssetsServer.ServeHTTP(w, r)
		} else {
			// all else we send direct to the local prometheus UI
			webProxy.ServeHTTP(w, r)
		}
	})

	if err := reloadConfig(noStepSubqueryInterval, notifs, reloadables...); err != nil {
		logrus.Fatalf("Error loading config: %s", err)
	}

	configSuccess.Set(1)
	configSuccessTime.SetToCurrentTime()

	close(reloadReady)

	// Set up access logger
	var accessLogOut io.Writer
	switch strings.ToLower(opts.AccessLogDestination) {
	case "stderr":
		accessLogOut = os.Stderr
	case "stdout":
		accessLogOut = os.Stdout
	case "none":
	default:
		logrus.Fatalf("Invalid AccessLogDestination: %s", opts.AccessLogDestination)
	}

	// Authentication is configured at startup only: auth.New does OIDC
	// discovery and compiles the provider chain, so a change to the `auth`
	// block needs a restart. Without the block the router is left unwrapped.
	// The exempt paths here are the defaults, used unless the config lists its
	// own; only we know where these routes actually ended up.
	authenticator, err := auth.New(ctx, ps.Config().Auth, []string{
		path.Join(webOptions.RoutePrefix, "/-/healthy"),
		path.Join(webOptions.RoutePrefix, "/-/ready"),
		opts.MetricsPath,
	})
	if err != nil {
		logrus.Fatalf("Error configuring authentication: %v", err)
	}
	var handler http.Handler = middleware.NewProxyHeaders(r, opts.ProxyHeaders)
	if authenticator != nil {
		handler = authenticator.Middleware(handler)
	}

	srv, err := server.CreateAndStart(opts.BindAddr, opts.LogFormat, opts.WebReadTimeout, accessLogOut, handler, opts.WebConfigFile)
	if err != nil {
		logrus.Fatalf("Error creating server: %v", err)
	}

	// wait for signals etc.
	for {
		select {
		case rc := <-webHandler.Reload():
			logrus.Infof("Reloading config")
			if err := reloadConfig(noStepSubqueryInterval, notifs, reloadables...); err != nil {
				logrus.Errorf("Error reloading config: %s", err)
				rc <- err
			} else {
				rc <- nil
			}
		case sig := <-sigs:
			switch sig {
			case syscall.SIGHUP:
				logrus.Infof("Reloading config")
				if err := reloadConfig(noStepSubqueryInterval, notifs, reloadables...); err != nil {
					logrus.Errorf("Error reloading config: %s", err)
				}
			case syscall.SIGTERM, syscall.SIGINT:
				logrus.Info("proxeus received exit signal, starting graceful shutdown")
				notifs.AddNotification(notifications.ShuttingDown)

				// Stop all services we are running
				stopping = true        // start failing healthchecks
				notifierManager.Stop() // stop alert notifier
				ruleManager.Stop()     // Stop rule manager

				if opts.ShutdownDelay > 0 {
					logrus.Infof("proxeus delaying shutdown by %v", opts.ShutdownDelay)
					time.Sleep(opts.ShutdownDelay)
				}
				logrus.Infof("proxeus exiting with timeout: %v", opts.ShutdownTimeout)
				defer cancel()
				if opts.ShutdownTimeout > 0 {
					ctx, cancel = context.WithTimeout(ctx, opts.ShutdownTimeout)
					defer cancel()
				}
				srv.Shutdown(ctx)
				return
			default:
				logrus.Errorf("Uncaught signal: %v", sig)
			}

		}
	}
}

// sendAlerts implements the rules.NotifyFunc for a Notifier.
// It filters any non-firing alerts from the input.
func sendAlerts(n *notifier.Manager, externalURL string) rules.NotifyFunc {
	return func(ctx context.Context, expr string, alerts ...*rules.Alert) {
		var res []*notifier.Alert

		for _, alert := range alerts {
			// Only send actually firing alerts.
			if alert.State == rules.StatePending {
				continue
			}
			a := &notifier.Alert{
				StartsAt:     alert.FiredAt,
				Labels:       alert.Labels,
				Annotations:  alert.Annotations,
				GeneratorURL: externalURL + strutil.TableLinkForExpression(expr),
			}
			if !alert.ResolvedAt.IsZero() {
				a.EndsAt = alert.ResolvedAt
			}
			res = append(res, a)
		}

		if len(alerts) > 0 {
			n.Send(res...)
		}
	}
}

func startsOrEndsWithQuote(s string) bool {
	return strings.HasPrefix(s, "\"") || strings.HasPrefix(s, "'") ||
		strings.HasSuffix(s, "\"") || strings.HasSuffix(s, "'")
}

// computeExternalURL computes a sanitized external URL from a raw input. It infers unset
// URL parts from the OS and the given listen address.
func computeExternalURL(u, listenAddr string) (*url.URL, error) {
	if u == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		_, port, err := net.SplitHostPort(listenAddr)
		if err != nil {
			return nil, err
		}
		u = fmt.Sprintf("http://%s:%s/", hostname, port)
	}

	if startsOrEndsWithQuote(u) {
		return nil, fmt.Errorf("URL must not begin or end with quotes")
	}

	eu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	ppref := strings.TrimRight(eu.Path, "/")
	if ppref != "" && !strings.HasPrefix(ppref, "/") {
		ppref = "/" + ppref
	}
	eu.Path = ppref

	return eu, nil
}

// compileCORSRegexString compiles given string and adds anchors
func compileCORSRegexString(s string) (*regexp.Regexp, error) {
	r, err := relabel.NewRegexp(s)
	if err != nil {
		return nil, err
	}
	return r.Regexp, nil
}

type safePromQLNoStepSubqueryInterval struct {
	value atomic.Int64
}

func durationToInt64Millis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}
func (i *safePromQLNoStepSubqueryInterval) Set(ev model.Duration) {
	i.value.Store(durationToInt64Millis(time.Duration(ev)))
}

func (i *safePromQLNoStepSubqueryInterval) Get(int64) int64 {
	return i.value.Load()
}
