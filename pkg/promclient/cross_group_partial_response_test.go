package promclient

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// keyedStub is an API whose Key() returns a distinct labelset, so each instance
// occupies its own fingerprint bucket — exactly how real server_groups behave
// (and the condition under which the fail-hard / partial-response distinction
// matters; shared-bucket stubs would mask it). It returns a fixed value or err.
type keyedStub struct {
	API
	key model.LabelSet
	val model.Value
	err error
}

func (s *keyedStub) Key() model.LabelSet { return s.key }

func (s *keyedStub) Query(_ context.Context, _ string, _ time.Time) (model.Value, v1.Warnings, error) {
	return s.val, nil, s.err
}

func crossGroupPartial(t *testing.T, apis []API, partial bool) *MultiAPI {
	t.Helper()
	names := []string{"sg0", "sg1"}
	labels := []model.LabelSet{{"server_group": "sg0"}, {"server_group": "sg1"}}
	m, err := NewCrossGroupMultiAPI(apis, names, labels, nil, false, nil, partial)
	if err != nil {
		t.Fatalf("NewCrossGroupMultiAPI: %v", err)
	}
	return m
}

func vec(name, sg string) model.Vector {
	return model.Vector{{Metric: model.Metric{"__name__": model.LabelValue(name), "server_group": model.LabelValue(sg)}, Value: 1, Timestamp: 100}}
}

// With partial_response=false, a single backend error fails the whole query —
// this is the fail-hard behavior the SRE review flagged (H1).
func TestCrossGroupPartialResponse_DisabledFailsHard(t *testing.T) {
	apis := []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, val: vec("cpu", "sg0")},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: errors.New("backend down")},
	}
	m := crossGroupPartial(t, apis, false)

	_, _, err := m.Query(context.Background(), "cpu", time.Now())
	if err == nil {
		t.Fatal("expected error when a backend fails and partial_response is disabled")
	}
}

// With partial_response=true, the healthy backend's data is returned and a
// warning is attached for the failed one.
func TestCrossGroupPartialResponse_EnabledReturnsPartial(t *testing.T) {
	apis := []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, val: vec("cpu", "sg0")},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: errors.New("backend down")},
	}
	m := crossGroupPartial(t, apis, true)

	v, warnings, err := m.Query(context.Background(), "cpu", time.Now())
	if err != nil {
		t.Fatalf("expected success with partial_response, got error: %v", err)
	}
	res, ok := v.(model.Vector)
	if !ok || len(res) != 1 || res[0].Metric["server_group"] != "sg0" {
		t.Fatalf("expected the healthy sg0 series, got %v", v)
	}
	if len(warnings) == 0 {
		t.Fatal("expected a degradation warning, got none")
	}
	if !strings.Contains(warnings[0], "partial_response") {
		t.Fatalf("expected partial_response warning, got %q", warnings[0])
	}
}

// Partial response still fails when every backend is down — a total outage must
// not masquerade as an empty (successful) result.
func TestCrossGroupPartialResponse_AllBackendsFail(t *testing.T) {
	apis := []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, err: errors.New("sg0 down")},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: errors.New("sg1 down")},
	}
	m := crossGroupPartial(t, apis, true)

	if _, _, err := m.Query(context.Background(), "cpu", time.Now()); err == nil {
		t.Fatal("expected error when all backends fail, even with partial_response")
	}
}
