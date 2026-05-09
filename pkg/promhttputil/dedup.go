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

// reducedFingerprint returns the fingerprint of m with all keys in ignore
// removed. It copies the metric, so callers may reuse m safely.
func reducedFingerprint(m model.Metric, ignore map[model.LabelName]struct{}) model.Fingerprint {
	reduced := make(model.LabelSet, len(m))
	for k, v := range m {
		if _, skip := ignore[k]; !skip {
			reduced[k] = v
		}
	}
	return reduced.FastFingerprint()
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
func mergeVectorDeterministic(a, b model.Vector, opts DedupOpts) (model.Vector, int) {
	type entry struct {
		sample  *model.Sample
		ordinal int
	}

	// Key: full fingerprint → index in result (for fast same-FP dedup)
	// Key: reduced fingerprint → entry (for cross-group dedup)
	fullFPIndex := make(map[model.Fingerprint]int, len(a)+len(b))
	reducedFPEntry := make(map[model.Fingerprint]*entry, len(a)+len(b))
	result := make(model.Vector, 0, len(a)+len(b))
	collisions := 0

	add := func(s *model.Sample, ordinal int) {
		fullFP := s.Metric.Fingerprint()

		// Exact duplicate: apply within-group preferMax=false semantics (first wins).
		if idx, ok := fullFPIndex[fullFP]; ok {
			if result[idx].Value == model.SampleValue(0) {
				result[idx].Value = s.Value
			}
			return
		}

		redFP := reducedFingerprint(model.Metric(s.Metric), opts.IgnoreLabels)
		if existing, ok := reducedFPEntry[redFP]; ok {
			collisions++
			// Lower ordinal wins.
			if ordinal < existing.ordinal {
				// Swap out the losing sample in result. We locate it via the
				// full-FP index of the current winner before removing it.
				oldFullFP := existing.sample.Metric.Fingerprint()
				oldIdx := fullFPIndex[oldFullFP]
				delete(fullFPIndex, oldFullFP)
				result[oldIdx] = s
				fullFPIndex[fullFP] = oldIdx
				existing.sample = s
				existing.ordinal = ordinal
			}
			return
		}

		idx := len(result)
		result = append(result, s)
		fullFPIndex[fullFP] = idx
		reducedFPEntry[redFP] = &entry{sample: s, ordinal: ordinal}
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
	}

	fullFPIndex := make(map[model.Fingerprint]int, len(a)+len(b))
	reducedFPEntry := make(map[model.Fingerprint]*entry, len(a)+len(b))
	result := make(model.Matrix, 0, len(a)+len(b))
	collisions := 0

	add := func(stream *model.SampleStream, ordinal int) {
		fullFP := model.Metric(stream.Metric).Fingerprint()

		// Exact-FP duplicate: within-group HA already merged; keep first.
		if _, ok := fullFPIndex[fullFP]; ok {
			return
		}

		redFP := reducedFingerprint(model.Metric(stream.Metric), opts.IgnoreLabels)
		if existing, ok := reducedFPEntry[redFP]; ok {
			collisions++
			if ordinal < existing.ordinal {
				// Swap out the losing stream. Locate it via the full-FP index
				// of the current winner before removing that entry.
				oldFullFP := model.Metric(existing.stream.Metric).Fingerprint()
				oldIdx := fullFPIndex[oldFullFP]
				delete(fullFPIndex, oldFullFP)
				result[oldIdx] = stream
				fullFPIndex[fullFP] = oldIdx
				existing.stream = stream
				existing.ordinal = ordinal
			}
			return
		}

		idx := len(result)
		result = append(result, stream)
		fullFPIndex[fullFP] = idx
		reducedFPEntry[redFP] = &entry{stream: stream, ordinal: ordinal}
	}

	for _, s := range a {
		add(s, opts.OrdinalA)
	}
	for _, s := range b {
		add(s, opts.OrdinalB)
	}

	return result, collisions
}
