package promclient

import (
	"errors"
	"strings"
	"testing"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/promql"

	"github.com/pvlltvk/proxeus/pkg/promapi"
)

// Errors reach NormalizePromError already wrapped by the target and
// server-group ErrorWraps, so the classification has to see through the chain
// and the message has to survive it.
func TestNormalizePromErrorThroughWraps(t *testing.T) {
	const context = "error in servergroup ord=0"

	for _, tc := range []struct {
		name    string
		inner   error
		wantAs  any
		wantErr bool
	}{
		{
			name:   "response error timeout",
			inner:  &promapi.ResponseError{Type: "timeout", Msg: "query timed out in expression evaluation"},
			wantAs: new(promql.ErrQueryTimeout),
		},
		{
			name:   "response error canceled",
			inner:  &promapi.ResponseError{Type: "canceled", Msg: "query was canceled"},
			wantAs: new(promql.ErrQueryCanceled),
		},
		{
			name:  "client_golang timeout",
			inner: &v1.Error{Type: v1.ErrServer, Detail: `{"status":"error","errorType":"timeout","error":"query timed out in expression evaluation"}`},
			// client_golang reports a 503 as ErrServer; the real type is in the body.
			wantAs: new(promql.ErrQueryTimeout),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := errors.New(context + ": " + tc.inner.Error())
			got := NormalizePromError(&wrapElem{msg: context, err: tc.inner})

			switch want := tc.wantAs.(type) {
			case *promql.ErrQueryTimeout:
				if !errors.As(got, want) {
					t.Fatalf("got %T (%v), want it to carry ErrQueryTimeout", got, got)
				}
			case *promql.ErrQueryCanceled:
				if !errors.As(got, want) {
					t.Fatalf("got %T (%v), want it to carry ErrQueryCanceled", got, got)
				}
			}

			// The API renders the message it is handed, so the context has to
			// survive: otherwise nobody can tell which backend timed out.
			if !strings.Contains(got.Error(), context) {
				t.Fatalf("error %q lost the fan-out context %q", got, context)
			}
			if got.Error() != wrapped.Error() {
				t.Fatalf("message changed: got %q, want %q", got, wrapped)
			}
			// The original is still reachable for anything matching on it.
			if !errors.Is(got, tc.inner) {
				t.Fatalf("original error no longer in the chain: %v", got)
			}
		})
	}
}

func TestNormalizePromErrorLeavesOthersAlone(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"plain", errors.New("connection refused")},
		{"bad_data", &promapi.ResponseError{Type: "bad_data", Msg: "bad query"}},
		{"execution", &promapi.ResponseError{Type: "execution", Msg: `invalid label name "a\xc5z"`}},
		{"internal", &promapi.ResponseError{Type: "internal", Msg: "boom"}},
		{"unparseable detail", &v1.Error{Type: v1.ErrServer, Detail: "<html>502 Bad Gateway</html>"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := &wrapElem{msg: "error in servergroup ord=0", err: tc.err}
			got := NormalizePromError(in)
			if got.Error() != in.Error() {
				t.Fatalf("message changed: got %q, want %q", got, in)
			}
			var timeout promql.ErrQueryTimeout
			var canceled promql.ErrQueryCanceled
			if errors.As(got, &timeout) || errors.As(got, &canceled) {
				t.Fatalf("%q was reclassified as a timeout or cancellation", got)
			}
		})
	}
}

// wrapElem stands in for the ErrorWrap chain without pulling the whole API
// surface into the test.
type wrapElem struct {
	msg string
	err error
}

func (w *wrapElem) Error() string { return w.msg + ": " + w.err.Error() }
func (w *wrapElem) Unwrap() error { return w.err }
