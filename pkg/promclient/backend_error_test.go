package promclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/pvlltvk/proxeus/pkg/promapi"
)

// failAPI answers every call with err.
type failAPI struct {
	API
	err error
}

func (f *failAPI) LabelNames(ctx context.Context, matchers []string, start, end time.Time) ([]string, v1.Warnings, error) {
	return nil, nil, f.err
}

func (f *failAPI) LabelValues(ctx context.Context, label string, matchers []string, start, end time.Time) (model.LabelValues, v1.Warnings, error) {
	return nil, nil, f.err
}

func (f *failAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return storage.ErrSeriesSet(f.err)
}

func (f *failAPI) QueryRange(ctx context.Context, query string, r v1.Range) storage.SeriesSet {
	return storage.ErrSeriesSet(f.err)
}

func (f *failAPI) Series(ctx context.Context, matches []string, start, end time.Time) ([]model.LabelSet, v1.Warnings, error) {
	return nil, nil, f.err
}

func (f *failAPI) GetValue(ctx context.Context, start, end time.Time, matchers []*labels.Matcher) storage.SeriesSet {
	return storage.ErrSeriesSet(f.err)
}

func (f *failAPI) Metadata(ctx context.Context, metric, limit string) (map[string][]v1.Metadata, error) {
	return nil, f.err
}

func (f *failAPI) QueryExemplars(ctx context.Context, query string, start, end time.Time) ([]v1.ExemplarQueryResult, error) {
	return nil, f.err
}

// apiCalls invokes one API method and returns the error it surfaced, so a test
// can make the same assertion for every entry point ErrorWrap covers.
var apiCalls = map[string]func(API) error{
	"LabelNames": func(a API) error {
		_, _, err := a.LabelNames(context.Background(), nil, time.Time{}, time.Time{})
		return err
	},
	"LabelValues": func(a API) error {
		_, _, err := a.LabelValues(context.Background(), "job", nil, time.Time{}, time.Time{})
		return err
	},
	"Query": func(a API) error {
		return a.Query(context.Background(), "up", time.Now()).Err()
	},
	"QueryRange": func(a API) error {
		return a.QueryRange(context.Background(), "up", v1.Range{}).Err()
	},
	"Series": func(a API) error {
		_, _, err := a.Series(context.Background(), []string{"up"}, time.Time{}, time.Time{})
		return err
	},
	"GetValue": func(a API) error {
		return a.GetValue(context.Background(), time.Time{}, time.Time{}, nil).Err()
	},
	"Metadata": func(a API) error {
		_, err := a.Metadata(context.Background(), "up", "")
		return err
	},
	"QueryExemplars": func(a API) error {
		_, err := a.QueryExemplars(context.Background(), "up", time.Time{}, time.Time{})
		return err
	},
}

func TestIsBackendQueryError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", errors.New("connection refused"), false},
		{"bad_data", &promapi.ResponseError{Type: "bad_data", Msg: `invalid parameter "query"`}, true},
		{"execution", &promapi.ResponseError{Type: "execution", Msg: `invalid label name "a\xc5z"`}, true},
		{"timeout", &promapi.ResponseError{Type: "timeout", Msg: "query timed out"}, false},
		{"internal", &promapi.ResponseError{Type: "internal", Msg: "boom"}, false},
		{"bad_response", &promapi.ResponseError{Type: "bad_response", Msg: "unexpected EOF"}, false},
		{"untyped response error", &promapi.ResponseError{Msg: "boom"}, false},
		{"client_golang bad_data", &v1.Error{Type: v1.ErrBadData, Msg: "bad query"}, true},
		{"client_golang server error", &v1.Error{Type: v1.ErrServer, Msg: "boom"}, false},
		// The predicate is consulted after other layers have already added
		// their own context, so it has to see through a wrap chain.
		{"wrapped", fmt.Errorf("expanding series: %w", &promapi.ResponseError{Type: "execution", Msg: "boom"}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isBackendQueryError(tc.err); got != tc.want {
				t.Fatalf("isBackendQueryError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// wrapLikeServerGroup builds the pair of wraps a real request passes through,
// the same way ApplyConfig does: the target first, then the server group.
func wrapLikeServerGroup(api API) *ErrorWrap {
	target := &ErrorWrap{A: api, Msg: "error in target=" + testTargetURL, OmitOnQueryError: true}
	return &ErrorWrap{A: target, Msg: testServerGroupMsg}
}

const (
	testTargetURL      = "http://127.0.0.1:9090"
	testServerGroupMsg = "error in servergroup ord=0"
)

// A rejected query keeps the server group that rejected it -- with mixed
// backends that is the useful half -- and loses the target address.
func TestErrorWrapDropsTheTargetFromQueryErrors(t *testing.T) {
	const msg = `invalid label name "a\xc5z"`
	queryErr := &promapi.ResponseError{Type: "execution", Msg: msg}
	want := testServerGroupMsg + ": " + queryErr.Error()
	wrapped := wrapLikeServerGroup(&failAPI{err: queryErr})

	t.Run("SeriesSet path", func(t *testing.T) {
		err := wrapped.Query(context.Background(), "up", time.Now()).Err()
		if err == nil || err.Error() != want {
			t.Fatalf("got %v, want %v", err, want)
		}
		if !errors.Is(err, queryErr) {
			t.Fatalf("error identity lost: %v", err)
		}
	})

	t.Run("returned error path", func(t *testing.T) {
		_, _, err := wrapped.LabelNames(context.Background(), nil, time.Time{}, time.Time{})
		if err == nil || err.Error() != want {
			t.Fatalf("got %v, want %v", err, want)
		}
	})
}

func TestErrorWrapStillFramesOtherFailures(t *testing.T) {
	boom := errors.New("connection refused")
	wrapped := wrapLikeServerGroup(&failAPI{err: boom})

	err := wrapped.Query(context.Background(), "up", time.Now()).Err()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{testServerGroupMsg, "error in target=" + testTargetURL, boom.Error()} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q is missing %q", err, want)
		}
	}
}

// Every ErrorWrap entry point has to make the same call, whether it frames a
// returned error or one carried by a SeriesSet.
func TestErrorWrapEveryMethod(t *testing.T) {
	queryErr := &promapi.ResponseError{Type: "execution", Msg: "boom"}
	boom := errors.New("connection refused")

	for name, call := range apiCalls {
		t.Run(name, func(t *testing.T) {
			wrapped := wrapLikeServerGroup(&failAPI{err: queryErr})
			want := testServerGroupMsg + ": " + queryErr.Error()
			if err := call(wrapped); err == nil || err.Error() != want {
				t.Fatalf("got %v, want %v", err, want)
			}

			wrapped = wrapLikeServerGroup(&failAPI{err: boom})
			err := call(wrapped)
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range []string{testServerGroupMsg, "error in target=" + testTargetURL, boom.Error()} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q is missing %q", err, want)
				}
			}
		})
	}
}

func TestFetchError(t *testing.T) {
	queryErr := &promapi.ResponseError{Type: "bad_data", Msg: "bad query"}
	if got := fetchError(queryErr); got.Error() != queryErr.Error() {
		t.Fatalf("got %q, want the backend error verbatim", got)
	}

	boom := errors.New("connection refused")
	if got := fetchError(boom); !strings.Contains(got.Error(), "Unable to fetch from downstream servers") {
		t.Fatalf("got %q, want it framed as a fan-out failure", got)
	}
}

// The fan-out has to leave alone what ErrorWrap deliberately let through.
// fetchError is only reached in partial-response mode -- everywhere else the
// fan-out aborts on the first error that puts requiredCount out of reach and
// returns it as-is -- so these drive it through a cross-group MultiAPI with
// every backend failing.
func TestMultiAPIAllBackendsRejectTheQuery(t *testing.T) {
	queryErr := &promapi.ResponseError{Type: "bad_data", Msg: "bad query"}
	m := crossGroupPartial(t, []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, err: queryErr},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: queryErr},
	}, true)

	err := m.Query(context.Background(), "cpu", time.Now()).Err()
	if err == nil || err.Error() != queryErr.Error() {
		t.Fatalf("got %v, want %v", err, queryErr)
	}
	if !errors.Is(err, queryErr) {
		t.Fatalf("error identity lost: %v", err)
	}
}

func TestMultiAPIAllBackendsDown(t *testing.T) {
	boom := errors.New("connection refused")
	m := crossGroupPartial(t, []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, err: boom},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: boom},
	}, true)

	err := m.Query(context.Background(), "cpu", time.Now()).Err()
	if err == nil || !strings.Contains(err.Error(), "Unable to fetch from downstream servers") {
		t.Fatalf("got %v, want it framed as a fan-out failure", err)
	}
}

func TestPartialResponseErr(t *testing.T) {
	queryErr := &promapi.ResponseError{Type: "execution", Msg: "boom"}
	if got, want := partialResponseErr(1, queryErr).Error(), "partial_response: backend[1] rejected the query: execution: boom"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	boom := errors.New("connection refused")
	if got, want := partialResponseErr(1, boom).Error(), "partial_response: backend[1] unavailable: connection refused"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A backend that rejects the query while another answers is a degradation the
// user is told about, but it is not an outage: the warning has to say which
// backend disagreed and that it disagreed, not that it was unreachable.
func TestPartialResponseWarningNamesTheRejectingBackend(t *testing.T) {
	queryErr := &promapi.ResponseError{Type: "execution", Msg: `invalid label name "a\xc5z"`}
	m := crossGroupPartial(t, []API{
		&keyedStub{key: model.LabelSet{"server_group": "sg0"}, val: vec("cpu", "sg0")},
		&keyedStub{key: model.LabelSet{"server_group": "sg1"}, err: queryErr},
	}, true)

	ss := m.Query(context.Background(), "cpu", time.Now())
	if err := ss.Err(); err != nil {
		t.Fatalf("expected the healthy backend's data, got error: %v", err)
	}
	warnings := warningStrings(ss.Warnings())
	want := "partial_response: backend[1] rejected the query: " + queryErr.Error()
	if len(warnings) != 1 || warnings[0] != want {
		t.Fatalf("warnings = %v, want [%q]", warnings, want)
	}
}
