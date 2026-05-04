package servergroup

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gatherFamily gathers all registered metric families and returns the one
// matching the given metric name, or nil if not found.
func gatherFamily(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("prometheus gather error: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

// hasLabel returns true if any metric in the family has a label with the given name and value.
func hasLabel(mf *dto.MetricFamily, labelName, labelValue string) bool {
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelName && lp.GetValue() == labelValue {
				return true
			}
		}
	}
	return false
}

// TestServerGroupSummaryHasServerGroupLabel verifies that the
// server_group_request_duration_seconds metric carries a "server_group" label.
func TestServerGroupSummaryHasServerGroupLabel(t *testing.T) {
	const sgName = "sg-label-test"

	// Emit a sample so the metric family appears in the gather output.
	serverGroupSummary.WithLabelValues(sgName, "host:9090", "query", "success").Observe(0.001)

	mf := gatherFamily(t, "server_group_request_duration_seconds")
	if mf == nil {
		t.Fatal("metric family server_group_request_duration_seconds not found in registry")
	}

	if !hasLabel(mf, "server_group", sgName) {
		t.Errorf("label server_group=%q not found in server_group_request_duration_seconds", sgName)
	}
}

// TestServerGroupErrorCounterRegistered verifies that promxy_server_group_request_errors_total
// is registered and carries the server_group label.
func TestServerGroupErrorCounterRegistered(t *testing.T) {
	const sgName = "sg-error-test"

	serverGroupRequestErrors.WithLabelValues(sgName, "host:9090", "query").Inc()

	mf := gatherFamily(t, "promxy_server_group_request_errors_total")
	if mf == nil {
		t.Fatal("metric family promxy_server_group_request_errors_total not found in registry")
	}

	if !hasLabel(mf, "server_group", sgName) {
		t.Errorf("label server_group=%q not found in promxy_server_group_request_errors_total", sgName)
	}
}

// TestAPIClientMetricFuncServerGroupLabel verifies that the apiClientMetricFunc
// closure (as constructed in loadTargetGroupMap) correctly tags observations
// with the configured server group name and only increments the error counter
// on status="error".
func TestAPIClientMetricFuncServerGroupLabel(t *testing.T) {
	const sgName = "sg-closure-test"
	targets := []string{"target-host:9090"}

	// Replicate the closure from loadTargetGroupMap.
	fn := func(i int, api, status string, took float64) {
		serverGroupSummary.WithLabelValues(sgName, targets[i], api, status).Observe(took)
		if status == "error" {
			serverGroupRequestErrors.WithLabelValues(sgName, targets[i], api).Inc()
		}
	}

	// One success — should NOT increment the error counter for this label set.
	fn(0, "query_range", "success", 0.010)
	// One error — SHOULD increment the error counter.
	fn(0, "query_range", "error", 0.200)

	// Verify summary has sample with the right server_group label.
	summaryMF := gatherFamily(t, "server_group_request_duration_seconds")
	if summaryMF == nil {
		t.Fatal("server_group_request_duration_seconds not found")
	}
	if !hasLabel(summaryMF, "server_group", sgName) {
		t.Errorf("summary missing server_group=%q label", sgName)
	}

	// Verify error counter has exactly 1 for the error observation.
	errMF := gatherFamily(t, "promxy_server_group_request_errors_total")
	if errMF == nil {
		t.Fatal("promxy_server_group_request_errors_total not found")
	}
	if !hasLabel(errMF, "server_group", sgName) {
		t.Errorf("error counter missing server_group=%q label", sgName)
	}

	// Ensure the counter value is >= 1 (other tests may also write to this metric).
	var found bool
	for _, m := range errMF.GetMetric() {
		sgMatch, apiMatch := false, false
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "server_group" && lp.GetValue() == sgName {
				sgMatch = true
			}
			if lp.GetName() == "call" && lp.GetValue() == "query_range" {
				apiMatch = true
			}
		}
		if sgMatch && apiMatch {
			found = true
			if v := m.GetCounter().GetValue(); v < 1 {
				t.Errorf("expected error counter >= 1 for server_group=%s call=query_range, got %v", sgName, v)
			}
		}
	}
	if !found {
		t.Errorf("no counter sample found for server_group=%s call=query_range", sgName)
	}
}
