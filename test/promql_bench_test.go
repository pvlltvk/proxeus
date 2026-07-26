package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
)

func BenchmarkEvaluations(b *testing.B) {
	//this outer bench isn't real, it only spins through the others, no need to collect data
	b.StopTimer()

	files, err := filepath.Glob("benchdata/*.test")
	if err != nil {
		b.Fatal(err)
	}
	loadContent, err := os.ReadFile("benchdata/load.test")
	if err != nil {
		b.Fatalf("error reading benchdata/load.test: %s", err)
	}

	for _, fn := range files {
		if fn == "benchdata/load.test" {
			continue
		}
		evalContent, err := os.ReadFile(fn)
		if err != nil {
			b.Errorf("error reading %s: %s", fn, err)
			continue
		}
		// v3.5.0 dropped the reentrant promqltest.Test harness, so each
		// iteration re-runs the load+eval input via RunTestWithStorage; the
		// load block reseeds storage and the eval block exercises the engine.
		input := string(loadContent) + "\n" + string(evalContent)

		b.Run(fn, func(b *testing.B) {
			b.Run("direct", func(b *testing.B) {
				eng := promqltest.NewTestEngine(b, false, 0, 50000000)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					promqltest.RunTestWithStorage(b, input, eng, func(tt testutil.T) storage.Storage {
						return teststorage.New(tt)
					})
				}
			})

			b.Run("proxeus", func(b *testing.B) {
				benchProxy(b, input, rawDoublePSConfig)
			})

			b.Run("proxeus_remoteread", func(b *testing.B) {
				benchProxy(b, input, rawDoublePSConfigRR)
			})
		})
	}
}

// benchProxy stands up two backend API servers over a shared SwappableStorage,
// fronts them with a proxeus ProxyStorage, then benchmarks the load+eval input
// through the proxy engine with pushdown enabled. Each iteration repoints the
// SwappableStorage at a freshly-loaded teststorage via RunTestWithStorage's
// newStorage hook.
func benchProxy(b *testing.B, input, psConfig string) {
	backend := &SwappableStorage{}
	srv, addr, stopChan := startAPIForTest(backend)
	srv2, addr2, stopChan2 := startAPIForTest(backend)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = srv2.Shutdown(ctx)
		<-stopChan
		<-stopChan2
	}()

	ps := getProxyStorage(fmt.Sprintf(psConfig, addr, addr2))
	eng := promqltest.NewTestEngine(b, false, 0, 50000000)
	eng.NodeReplacer = ps.NodeReplacer

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		promqltest.RunTestWithStorage(b, input, eng, func(tt testutil.T) storage.Storage {
			ts := teststorage.New(tt)
			backend.s = ts
			return &LayeredStorage{ps, ts}
		})
	}
}

// Swappable storage, to make benchmark perf bearable
type SwappableStorage struct {
	s storage.Storage
}

func (p *SwappableStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	return p.s.Querier(mint, maxt)
}
func (p *SwappableStorage) StartTime() (int64, error) {
	return p.s.StartTime()
}
func (p *SwappableStorage) Appender(ctx context.Context) storage.Appender {
	return p.s.Appender(ctx)
}
func (p *SwappableStorage) Close() error {
	return p.s.Close()
}
func (p *SwappableStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return p.s.ChunkQuerier(mint, maxt)
}
