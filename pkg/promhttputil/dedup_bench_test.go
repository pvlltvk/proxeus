package promhttputil

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
)

// Scaling benchmarks for the cross-backend dedup hot path. Unlike the fixed
// n=1000 fast-vs-slow comparison in dedup_test.go, these vary cardinality and
// model the project's real data shape: mostly-disjoint series across backends
// with a small fraction of accidental overlap. They are the in-repo half of the
// scale & perf validation effort; the per-sample reducedFingerprint map-copy is
// the prime allocation suspect for long-range, high-cardinality responses.
//
//	go test -bench=BenchmarkDedup -benchmem -run=^$ ./pkg/promhttputil/

// dedupSizes are the per-backend series counts swept by the scaling benchmarks.
var dedupSizes = []int{1000, 10000, 50000}

// benchVector builds a backend's Vector of nSeries samples. The first
// sharedSeries carry a backend-independent identity so they collide on the
// reduced fingerprint (modulo "backend"); the rest are namespaced by backend so
// their reduced fingerprints never collide.
func benchVector(backend string, nSeries, sharedSeries int) model.Vector {
	v := make(model.Vector, nSeries)
	for i := 0; i < nSeries; i++ {
		v[i] = &model.Sample{
			Metric:    benchMetric(backend, i, sharedSeries),
			Value:     model.SampleValue(i),
			Timestamp: model.Time(i * 1000),
		}
	}
	return v
}

// benchMatrix is benchVector's range-query analogue: each series carries
// nSamples points, approximating a downsampled long-range query.
func benchMatrix(backend string, nSeries, sharedSeries, nSamples int) model.Matrix {
	m := make(model.Matrix, nSeries)
	for i := 0; i < nSeries; i++ {
		samples := make([]model.SamplePair, nSamples)
		for j := 0; j < nSamples; j++ {
			samples[j] = model.SamplePair{Timestamp: model.Time(j * 1000), Value: model.SampleValue(j)}
		}
		m[i] = &model.SampleStream{Metric: benchMetric(backend, i, sharedSeries), Values: samples}
	}
	return m
}

func benchMetric(backend string, i, sharedSeries int) model.Metric {
	var name model.LabelValue
	if i < sharedSeries {
		name = model.LabelValue(fmt.Sprintf("shared_metric_%d", i))
	} else {
		name = model.LabelValue(fmt.Sprintf("%s_metric_%d", backend, i))
	}
	return model.Metric{
		"__name__": name,
		"instance": "host1",
		"job":      "node",
		"backend":  model.LabelValue(backend),
	}
}

// BenchmarkDedupVectorScaling sweeps cardinality at a realistic ~5% overlap to
// expose how dedup scales with series count.
func BenchmarkDedupVectorScaling(b *testing.B) {
	ignore := ignoreSet("backend")
	for _, n := range dedupSizes {
		shared := n / 20 // 5% accidental overlap
		a := benchVector("sg0", n, shared)
		bv := benchVector("sg1", n, shared)
		b.Run(fmt.Sprintf("n=%d/overlap=5pct", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = merge2(a, bv, ignore, 0, 1)
			}
		})
	}
}

// BenchmarkDedupMatrixScaling is the range-query equivalent. Matrix dedup picks
// whole streams (no per-sample copy), so cost scales with series count, not
// samples-per-series; the points mostly drive payload size / allocations.
func BenchmarkDedupMatrixScaling(b *testing.B) {
	const nSamples = 300
	ignore := ignoreSet("backend")
	for _, n := range []int{1000, 10000} {
		shared := n / 20
		a := benchMatrix("sg0", n, shared, nSamples)
		bv := benchMatrix("sg1", n, shared, nSamples)
		b.Run(fmt.Sprintf("n=%d/samples=%d/overlap=5pct", n, nSamples), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, _ = merge2(a, bv, ignore, 0, 1)
			}
		})
	}
}

// BenchmarkDedupVectorCollisionSwap is the worst case: every series collides and
// the second input always wins, so each b sample hits the swap branch (which
// recomputes the displaced winner's full fingerprint).
func BenchmarkDedupVectorCollisionSwap(b *testing.B) {
	const n = 10000
	a := benchVector("sg0", n, n)  // 100% shared
	bv := benchVector("sg1", n, n) // 100% shared
	// a is the higher ordinal, so every b sample displaces a's winner.
	ignore := ignoreSet("backend")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = merge2(a, bv, ignore, 1, 0)
	}
}
