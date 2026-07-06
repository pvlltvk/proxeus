package promclient

import "context"

// aggregatePushdownKey marks a context as carrying a pushed-down aggregation
// query (e.g. `sum(up)` sent verbatim to each server_group by the
// proxystorage NodeReplacer).
type aggregatePushdownKey struct{}

// WithAggregatePushdown marks ctx as a pushed-down aggregation fan-out. Each
// backend's result is a per-group aggregate PARTIAL that the promql engine
// re-combines upstream — partials from different groups are never duplicates
// of each other, so cross-group dedup must not collapse them (the aggregation
// may have removed exactly the labels dedup uses to tell series apart,
// e.g. bare `sum(up)` reduces every group's partial to the same empty
// labelset modulo external labels).
func WithAggregatePushdown(ctx context.Context) context.Context {
	return context.WithValue(ctx, aggregatePushdownKey{}, true)
}

// IsAggregatePushdown reports whether ctx was marked by WithAggregatePushdown.
func IsAggregatePushdown(ctx context.Context) bool {
	v, _ := ctx.Value(aggregatePushdownKey{}).(bool)
	return v
}
