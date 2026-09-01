// Command fakeprom serves a synthetic Prometheus HTTP API (see pkg/fakeprom)
// so proxeus can be load-tested without a real Thanos / VictoriaMetrics
// backend becoming the bottleneck.
package main

import (
	"errors"
	"net/http"
	"os"
	"time"

	flags "github.com/jessevdk/go-flags"
	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/fakeprom"
)

var opts struct {
	Listen              string        `long:"listen" description:"address to listen on" default:":9090"`
	Series              int           `long:"series" description:"number of series to serve" default:"1000"`
	Instance            int           `long:"instance" description:"id of this backend; only the non-shared series differ between instances" default:"0"`
	Overlap             float64       `long:"overlap" description:"fraction of series that is identical across instances" default:"0"`
	Latency             time.Duration `long:"latency" description:"artificial delay added to every response"`
	MetricName          string        `long:"metric-name" description:"__name__ of the generated series" default:"fake_metric"`
	MaxSamplesPerSeries int           `long:"max-samples-per-series" description:"cap on the samples a range response carries per series (0: uncapped)"`
}

func main() {
	parser := flags.NewParser(&opts, flags.Default)
	if _, err := parser.Parse(); err != nil {
		// If the error was from the parser, then we can simply return
		// as Parse() prints the error already
		var flagsErr *flags.Error
		if errors.As(err, &flagsErr) {
			os.Exit(1)
		}
		logrus.Fatalf("Error parsing flags: %v", err)
	}

	handler := fakeprom.New(fakeprom.Config{
		Series:              opts.Series,
		Instance:            opts.Instance,
		OverlapFraction:     opts.Overlap,
		MetricName:          opts.MetricName,
		Latency:             opts.Latency,
		MaxSamplesPerSeries: opts.MaxSamplesPerSeries,
	})

	logrus.Infof("fakeprom listening on %s: %d series, instance %d, overlap %v", opts.Listen, opts.Series, opts.Instance, opts.Overlap)
	if err := http.ListenAndServe(opts.Listen, handler); err != nil {
		logrus.Fatalf("Error serving: %v", err)
	}
}
