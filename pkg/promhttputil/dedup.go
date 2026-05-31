package promhttputil

import (
	"fmt"
	"reflect"

	"github.com/prometheus/common/model"
)

// DedupOpts controls the behaviour of MergeValuesDeterministic.
type DedupOpts struct {
	// IgnoreLabels are the label names stripped when computing the reduced
	// fingerprint used for cross-backend collision detection.
	IgnoreLabels map[model.LabelName]struct{}

	// OrdinalA and OrdinalB are the server_group ordinals (YAML order) for
	// the `a` and `b` inputs respectively. Lower ordinal wins on collision.
	OrdinalA, OrdinalB int

	// NameA and NameB are the human-readable group names for the two inputs,
	// used in DedupStats.
	NameA, NameB string
}

// DedupStats reports what MergeValuesDeterministic resolved.
type DedupStats struct {
	// Collisions counts series-level collisions resolved by tie-break.
	// One increment per overlapping reduced fingerprint, not per sample.
	Collisions int
}

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

// MergeValuesDeterministic merges `a` and `b` like MergeValues, but detects
// series-level collisions modulo opts.IgnoreLabels and resolves them by lowest
// ordinal. The winning sample/stream retains its full Metric (including its own
// backend label) so the response is honest about its origin.
//
// This is intended only for cross-group merges where each group has distinct
// external labels. Within-group HA dedup must continue to use MergeValues.
func MergeValuesDeterministic(a, b model.Value, opts DedupOpts) (model.Value, *DedupStats, error) {
	stats := &DedupStats{}

	if a == nil {
		return b, stats, nil
	}
	if b == nil {
		return a, stats, nil
	}
	if a.Type() != b.Type() {
		return nil, stats, fmt.Errorf("mismatch type %v!=%v", a.Type(), b.Type())
	}

	// Fast-path: when no labels are ignored the reduced fingerprint is identical
	// to the full fingerprint, so cross-backend collision detection collapses to
	// plain exact-FP dedup.  MergeValues already does that efficiently without
	// any per-sample map copies, so we can skip the entire slow path.
	// Scalar/String are handled identically via MergeValues further below, so
	// this fast-path is safe for all types including Vector and Matrix.
	if len(opts.IgnoreLabels) == 0 {
		v, err := MergeValues(0, a, b, false)
		return v, stats, err
	}

	switch aTyped := a.(type) {
	case *model.Scalar:
		// Delegate scalar tie-break to existing semantics (first non-zero wins).
		v, err := MergeValues(0, a, b, false)
		return v, stats, err

	case *model.String:
		v, err := MergeValues(0, a, b, false)
		return v, stats, err

	case model.Vector:
		bTyped := b.(model.Vector)
		merged, collisions := mergeVectorDeterministic(aTyped, bTyped, opts)
		stats.Collisions = collisions
		return merged, stats, nil

	case model.Matrix:
		bTyped := b.(model.Matrix)
		merged, collisions := mergeMatrixDeterministic(aTyped, bTyped, opts)
		stats.Collisions = collisions
		return merged, stats, nil
	}

	return nil, stats, fmt.Errorf("unknown type! %v", reflect.TypeOf(a))
}

// mergeVectorDeterministic merges two Vectors using reduced-fingerprint
// collision detection. Lower-ordinal source wins per bucket.
//
// Only the reduced fingerprint is indexed. Because each input's ignore labels
// are constant within that input, an exact full-labelset duplicate always lands
// in the same reduced bucket as its twin — so a direct labelset compare (run
// only on a bucket hit) tells exact-dups from genuine cross-group collisions.
// This keeps the common, disjoint path at one fingerprint per sample with no
// per-sample full Fingerprint() call (the dominant allocator at high cardinality).
func mergeVectorDeterministic(a, b model.Vector, opts DedupOpts) (model.Vector, int) {
	type entry struct {
		sample  *model.Sample
		ordinal int
		idx     int // position of this bucket's winner in result
	}

	buckets := make(map[model.Fingerprint]*entry, len(a)+len(b))
	result := make(model.Vector, 0, len(a)+len(b))
	scratch := make(model.LabelSet, 16)
	collisions := 0

	add := func(s *model.Sample, ordinal int) {
		redFP := reducedFingerprintInto(scratch, s.Metric, opts.IgnoreLabels)

		existing, ok := buckets[redFP]
		if !ok {
			result = append(result, s)
			buckets[redFP] = &entry{sample: s, ordinal: ordinal, idx: len(result) - 1}
			return
		}

		// Exact duplicate of this bucket's winner: within-group preferMax=false
		// semantics (first non-zero wins).
		if existing.sample.Metric.Equal(s.Metric) {
			if result[existing.idx].Value == model.SampleValue(0) {
				result[existing.idx].Value = s.Value
			}
			return
		}

		// Genuine cross-group collision: lower ordinal wins.
		collisions++
		if ordinal < existing.ordinal {
			result[existing.idx] = s
			existing.sample = s
			existing.ordinal = ordinal
		}
	}

	for _, s := range a {
		add(s, opts.OrdinalA)
	}
	for _, s := range b {
		add(s, opts.OrdinalB)
	}

	return result, collisions
}

// mergeMatrixDeterministic merges two Matrices using reduced-fingerprint
// collision detection. Lower-ordinal source's stream wins per bucket;
// samples are NOT interleaved (this is cross-backend, not HA dedup).
func mergeMatrixDeterministic(a, b model.Matrix, opts DedupOpts) (model.Matrix, int) {
	type entry struct {
		stream  *model.SampleStream
		ordinal int
		idx     int // position of this bucket's winner in result
	}

	buckets := make(map[model.Fingerprint]*entry, len(a)+len(b))
	result := make(model.Matrix, 0, len(a)+len(b))
	scratch := make(model.LabelSet, 16)
	collisions := 0

	add := func(stream *model.SampleStream, ordinal int) {
		redFP := reducedFingerprintInto(scratch, stream.Metric, opts.IgnoreLabels)

		existing, ok := buckets[redFP]
		if !ok {
			result = append(result, stream)
			buckets[redFP] = &entry{stream: stream, ordinal: ordinal, idx: len(result) - 1}
			return
		}

		// Exact duplicate of this bucket's winner: within-group HA already
		// merged; keep first.
		if existing.stream.Metric.Equal(stream.Metric) {
			return
		}

		// Genuine cross-group collision: lower ordinal wins.
		collisions++
		if ordinal < existing.ordinal {
			result[existing.idx] = stream
			existing.stream = stream
			existing.ordinal = ordinal
		}
	}

	for _, s := range a {
		add(s, opts.OrdinalA)
	}
	for _, s := range b {
		add(s, opts.OrdinalB)
	}

	return result, collisions
}
