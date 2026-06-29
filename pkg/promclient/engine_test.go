package promclient

import (
	"context"
	"os"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
)

func TestEngineAPI(t *testing.T) {
	content, err := os.ReadFile("testdata/metric_relabel.test")
	if err != nil {
		t.Fatal(err)
	}

	st := promqltest.LoadedStorage(t, string(content))
	defer st.Close()
	eng := promqltest.NewTestEngineWithOpts(t, promql.EngineOpts{
		MaxSamples:           10000,
		Timeout:              100 * time.Second,
		EnableAtModifier:     true,
		EnableNegativeOffset: true,
	})

	api, err := NewEngineAPI(eng, st)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.TODO()

	t.Run("QueryRange", func(t *testing.T) {
		ss := api.QueryRange(ctx, "prometheus_build_info", v1.Range{
			Start: model.Time(0).Time(),
			End:   model.Time(10).Time(),
			Step:  time.Duration(1e6),
		})

		if w := ss.Warnings(); len(w) > 0 {
			t.Fatalf("unexpected warnings: %v", w)
		}

		matrixValue, err := SeriesSetToMatrix(ss)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if len(matrixValue) != 1 {
			t.Fatalf("expecting a single series: %v", matrixValue)
		}
	})
}
