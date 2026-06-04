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

// DedupNStats reports collisions resolved by MergeValuesDeterministicN. Total is
// the number of series-level collisions (one per losing series). Pairs breaks
// that down by the {winnerOrdinal, loserOrdinal} that actually collided, so a
// caller can attribute each collision to the exact contributing server_groups.
//
// This is the property the binary MergeValuesDeterministic cannot give when
// chained: there, every accumulated series is presented to the next merge under
// a single OrdinalA (the running minimum), so a collision against a series that
// truly came from a middle backend is misattributed to the lowest ordinal. The
// n-way merge buckets every series under its own origin ordinal, so attribution
// is exact regardless of input order.
type DedupNStats struct {
	Total int
	Pairs map[[2]int]int
}

func (s *DedupNStats) record(winner, loser int) {
	s.Total++
	if s.Pairs == nil {
		s.Pairs = make(map[[2]int]int)
	}
	s.Pairs[[2]int{winner, loser}]++
}

// MergeValuesDeterministicN merges N per-backend values in a single pass, each
// tagged with its source ordinal, resolving cross-backend collisions (modulo
// ignore) by lowest ordinal. values[i] is the result from the backend at
// ordinals[i]; the two slices must have equal length.
//
// Callers should pass the inputs in ascending-ordinal order for a deterministic
// result ordering, but correctness (which series wins, and collision
// attribution) does not depend on the order.
//
// Unlike chaining MergeValuesDeterministic pairwise, every series is bucketed
// with its true origin ordinal, so the returned DedupNStats attributes each
// collision to the exact winner/loser server_group.
func MergeValuesDeterministicN(values []model.Value, ordinals []int, ignore map[model.LabelName]struct{}) (model.Value, *DedupNStats, error) {
	stats := &DedupNStats{}

	type ordinalValue struct {
		v   model.Value
		ord int
	}
	nonNil := make([]ordinalValue, 0, len(values))
	for i, v := range values {
		if v != nil {
			nonNil = append(nonNil, ordinalValue{v: v, ord: ordinals[i]})
		}
	}

	switch len(nonNil) {
	case 0:
		return nil, stats, nil
	case 1:
		return nonNil[0].v, stats, nil
	}

	typ := nonNil[0].v.Type()
	for _, x := range nonNil[1:] {
		if x.v.Type() != typ {
			return nil, stats, fmt.Errorf("mismatch type %v!=%v", typ, x.v.Type())
		}
	}

	switch typ {
	case model.ValVector:
		vectors := make([]model.Vector, len(nonNil))
		ords := make([]int, len(nonNil))
		for i, x := range nonNil {
			vectors[i] = x.v.(model.Vector)
			ords[i] = x.ord
		}
		return mergeNVectorsDeterministic(vectors, ords, ignore, stats), stats, nil

	case model.ValMatrix:
		matrices := make([]model.Matrix, len(nonNil))
		ords := make([]int, len(nonNil))
		for i, x := range nonNil {
			matrices[i] = x.v.(model.Matrix)
			ords[i] = x.ord
		}
		return mergeNMatricesDeterministic(matrices, ords, ignore, stats), stats, nil

	case model.ValScalar, model.ValString:
		// Scalar/String have no series identity to dedup; fold pairwise with the
		// existing first-non-zero / first-wins semantics, lowest ordinal first.
		result := nonNil[0].v
		for _, x := range nonNil[1:] {
			merged, err := MergeValues(0, result, x.v, false)
			if err != nil {
				return nil, stats, err
			}
			result = merged
		}
		return result, stats, nil
	}

	return nil, stats, fmt.Errorf("unknown type! %v", reflect.TypeOf(nonNil[0].v))
}

// mergeNVectorsDeterministic is the n-way generalization of
// mergeVectorDeterministic: every sample is bucketed under its origin ordinal,
// so the lowest-ordinal source wins each reduced-fingerprint bucket and stats
// record the true {winner, loser} pair.
func mergeNVectorsDeterministic(vectors []model.Vector, ordinals []int, ignore map[model.LabelName]struct{}, stats *DedupNStats) model.Vector {
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
				stats.record(ord, existing.ordinal)
				result[existing.idx] = s
				existing.sample = s
				existing.ordinal = ord
			} else {
				stats.record(existing.ordinal, ord)
			}
		}
	}

	return result
}

// mergeNMatricesDeterministic is the n-way generalization of
// mergeMatrixDeterministic. Streams are not interleaved; the lowest-ordinal
// source's stream wins each bucket.
func mergeNMatricesDeterministic(matrices []model.Matrix, ordinals []int, ignore map[model.LabelName]struct{}, stats *DedupNStats) model.Matrix {
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
				stats.record(ord, existing.ordinal)
				result[existing.idx] = stream
				existing.stream = stream
				existing.ordinal = ord
			} else {
				stats.record(existing.ordinal, ord)
			}
		}
	}

	return result
}
