package promclient

import (
	"fmt"

	"github.com/prometheus/common/model"

	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// The pre-SeriesSet dedup implementation, kept here as a differential oracle
// for the native one that replaced it (PR #13). It shipped until then, so
// matching it series-for-series is the strongest statement the new code can
// make. Test-only: nothing in the binary has merged model.Values since.

// reducedFingerprintInto returns the fingerprint of m with all keys in ignore
// removed, reusing scratch as working storage. scratch is cleared on entry, so
// a single map can be shared across many calls to avoid a per-call allocation.
// FastFingerprint is order-independent and allocation-free, so repopulating
// scratch is the only cost.
func reducedFingerprintInto(scratch model.LabelSet, m model.Metric, ignore map[model.LabelName]struct{}) model.Fingerprint {
	for k := range scratch {
		delete(scratch, k)
	}
	for k, v := range m {
		if _, skip := ignore[k]; !skip {
			scratch[k] = v
		}
	}
	return scratch.FastFingerprint()
}

// ordinalValue pairs one backend's value result with its source ordinal (its
// index within the apis slice / YAML server_group order).
type ordinalValue struct {
	Ordinal int
	Value   model.Value
}

// mergeValuesDeterministic merges N per-backend values in a single pass, each
// tagged with its source ordinal, resolving cross-backend collisions (series
// that match modulo ignore) by lowest ordinal. The winning sample/stream retains
// its full Metric (including its own backend label) so the response is honest
// about its origin.
//
// Callers should pass the inputs in ascending-ordinal order for a deterministic
// result ordering, but correctness (which series wins, and collision attribution)
// does not depend on the order.
//
// This is intended only for cross-group merges where each group has distinct
// external labels. Within-group HA dedup must continue to use MergeValues.
//
// Matrix exact-duplicates keep the first stream (no value interleaving) while
// vector exact-duplicates apply first-non-zero-wins; the asymmetry is
// intentional because this is cross-backend, not HA, dedup.
func mergeValuesDeterministic(inputs []ordinalValue, ignore map[model.LabelName]struct{}) (model.Value, *promhttputil.DedupStats, error) {
	stats := &promhttputil.DedupStats{}

	nonNil := make([]ordinalValue, 0, len(inputs))
	for _, in := range inputs {
		if in.Value != nil {
			nonNil = append(nonNil, in)
		}
	}

	switch len(nonNil) {
	case 0:
		return nil, stats, nil
	case 1:
		return nonNil[0].Value, stats, nil
	}

	typ := nonNil[0].Value.Type()
	for _, x := range nonNil[1:] {
		if x.Value.Type() != typ {
			return nil, stats, fmt.Errorf("mismatch type %v!=%v", typ, x.Value.Type())
		}
	}

	switch typ {
	case model.ValVector:
		vectors := make([]model.Vector, len(nonNil))
		ords := make([]int, len(nonNil))
		for i, x := range nonNil {
			vectors[i] = x.Value.(model.Vector)
			ords[i] = x.Ordinal
		}
		return mergeVectorsDeterministic(vectors, ords, ignore, stats), stats, nil

	case model.ValMatrix:
		matrices := make([]model.Matrix, len(nonNil))
		ords := make([]int, len(nonNil))
		for i, x := range nonNil {
			matrices[i] = x.Value.(model.Matrix)
			ords[i] = x.Ordinal
		}
		return mergeMatricesDeterministic(matrices, ords, ignore, stats), stats, nil

	case model.ValScalar, model.ValString:
		// Scalar/String have no series identity to dedup; fold pairwise with the
		// existing first-non-zero / first-wins semantics, lowest ordinal first.
		result := nonNil[0].Value
		for _, x := range nonNil[1:] {
			merged, err := promhttputil.MergeValues(0, false, result, x.Value, false)
			if err != nil {
				return nil, stats, err
			}
			result = merged
		}
		return result, stats, nil
	}

	return nil, stats, fmt.Errorf("unknown type! %v", nonNil[0].Value.Type().String())
}

// mergeVectorsDeterministic merges N Vectors using reduced-fingerprint collision
// detection: every sample is bucketed under its origin ordinal, so the
// lowest-ordinal source wins each reduced-fingerprint bucket and stats record the
// true {winner, loser} pair.
//
// Only the reduced fingerprint is indexed. Because each input's ignore labels are
// constant within that input, an exact full-labelset duplicate always lands in
// the same reduced bucket as its twin — so a direct labelset compare (run only on
// a bucket hit) tells exact-dups from genuine cross-group collisions. This keeps
// the common, disjoint path at one fingerprint per sample with no per-sample full
// Fingerprint() call (the dominant allocator at high cardinality).
func mergeVectorsDeterministic(vectors []model.Vector, ordinals []int, ignore map[model.LabelName]struct{}, stats *promhttputil.DedupStats) model.Vector {
	type entry struct {
		sample  *model.Sample
		ordinal int
		idx     int
	}

	total := 0
	for _, v := range vectors {
		total += len(v)
	}
	buckets := make(map[model.Fingerprint]*entry, total)
	result := make(model.Vector, 0, total)
	scratch := make(model.LabelSet, 16)

	for vi, vec := range vectors {
		ord := ordinals[vi]
		for _, s := range vec {
			redFP := reducedFingerprintInto(scratch, s.Metric, ignore)

			existing, ok := buckets[redFP]
			if !ok {
				result = append(result, s)
				buckets[redFP] = &entry{sample: s, ordinal: ord, idx: len(result) - 1}
				continue
			}

			// Exact duplicate of this bucket's winner: first non-zero wins.
			if existing.sample.Metric.Equal(s.Metric) {
				if result[existing.idx].Value == model.SampleValue(0) {
					result[existing.idx].Value = s.Value
				}
				continue
			}

			// Genuine cross-group collision: lower ordinal wins.
			if ord < existing.ordinal {
				stats.Record(ord, existing.ordinal)
				result[existing.idx] = s
				existing.sample = s
				existing.ordinal = ord
			} else {
				stats.Record(existing.ordinal, ord)
			}
		}
	}

	return result
}

// mergeMatricesDeterministic is mergeVectorsDeterministic's range-query analog.
// Streams are not interleaved (this is cross-backend, not HA dedup); the
// lowest-ordinal source's stream wins each bucket.
func mergeMatricesDeterministic(matrices []model.Matrix, ordinals []int, ignore map[model.LabelName]struct{}, stats *promhttputil.DedupStats) model.Matrix {
	type entry struct {
		stream  *model.SampleStream
		ordinal int
		idx     int
	}

	total := 0
	for _, m := range matrices {
		total += len(m)
	}
	buckets := make(map[model.Fingerprint]*entry, total)
	result := make(model.Matrix, 0, total)
	scratch := make(model.LabelSet, 16)

	for mi, mat := range matrices {
		ord := ordinals[mi]
		for _, stream := range mat {
			redFP := reducedFingerprintInto(scratch, stream.Metric, ignore)

			existing, ok := buckets[redFP]
			if !ok {
				result = append(result, stream)
				buckets[redFP] = &entry{stream: stream, ordinal: ord, idx: len(result) - 1}
				continue
			}

			// Exact duplicate of this bucket's winner: keep first.
			if existing.stream.Metric.Equal(stream.Metric) {
				continue
			}

			// Genuine cross-group collision: lower ordinal wins.
			if ord < existing.ordinal {
				stats.Record(ord, existing.ordinal)
				result[existing.idx] = stream
				existing.stream = stream
				existing.ordinal = ord
			} else {
				stats.Record(existing.ordinal, ord)
			}
		}
	}

	return result
}
