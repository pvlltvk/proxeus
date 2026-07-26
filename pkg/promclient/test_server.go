package promclient

import (
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/api"
	clientv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/route"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/notifications"
	webv1 "github.com/prometheus/prometheus/web/api/v1"
)

// CreateTestServer creates a test HTTP server backed by an in-memory promql
// engine and returns an API client, a close function, and any error
// encountered during setup.
func CreateTestServer(t *testing.T, path string) (API, func(), error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	st := promqltest.LoadedStorage(t, string(content))
	eng := promqltest.NewTestEngineWithOpts(t, promql.EngineOpts{
		MaxSamples:           10000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})

	ln, err := net.Listen("tcp", "")
	if err != nil {
		st.Close()
		return nil, nil, err
	}

	cfgFunc := func() config.Config { return config.DefaultConfig }
	// Return 503 until ready (for us there isn't much startup, so this might not need to be implemented).
	readyFunc := func(f http.HandlerFunc) http.HandlerFunc { return f }
	// nil registerer: this server is created many times per test run and the
	// notification metrics would collide on a shared one.
	notifs := notifications.NewNotifications(16, nil)

	apiRouter := route.New()
	webv1.NewAPI(
		eng,
		st,
		nil, // appendable
		nil, // exemplarQueryable
		nil, // scrapePoolsRetriever
		nil, // targetRetriever
		nil, // alertmanagerRetriever
		cfgFunc,
		nil, // flagsMap
		webv1.GlobalURLOptions{
			ListenAddress: ln.Addr().String(),
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
	).Register(apiRouter.WithPrefix("/api/v1"))

	srv := &http.Server{Handler: apiRouter}
	go srv.Serve(ln) // TODO: cancel/stop ability
	closeFunc := func() {
		st.Close()
		srv.Close()
	}

	client, err := api.NewClient(api.Config{Address: "http://" + ln.Addr().String()})
	if err != nil {
		closeFunc()
		return nil, nil, err
	}

	return &PromAPIV1{API: clientv1.NewAPI(client), Client: client}, closeFunc, nil
}
