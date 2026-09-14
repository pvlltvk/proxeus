package promclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/prometheus/storage"

	"github.com/pvlltvk/proxeus/pkg/promapi"
)

// failAPI answers every call it implements with err. The embedded interface
// supplies the rest of API, which these tests never reach.
type failAPI struct {
	API
	err error
}

func (f *failAPI) LabelNames(ctx context.Context, matchers []string, start, end time.Time) ([]string, v1.Warnings, error) {
	return nil, nil, f.err
}

func (f *failAPI) Query(ctx context.Context, query string, ts time.Time) storage.SeriesSet {
	return storage.ErrSeriesSet(f.err)
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

func TestErrorWrapPassesBackendQueryErrorsThrough(t *testing.T) {
	const msg = `invalid label name "a\xc5z"`
	queryErr := &promapi.ResponseError{Type: "execution", Msg: msg}
	// Both wraps a real request passes through: the target and then the
	// server group it belongs to.
	wrapped := &ErrorWrap{&ErrorWrap{&failAPI{err: queryErr}, "error in target=http://127.0.0.1:9090"}, "error in servergroup ord=0"}

	t.Run("SeriesSet path", func(t *testing.T) {
		err := wrapped.Query(context.Background(), "up", time.Now()).Err()
		if err == nil || err.Error() != queryErr.Error() {
			t.Fatalf("got %v, want %v", err, queryErr)
		}
		if !errors.Is(err, queryErr) {
			t.Fatalf("error identity lost: %v", err)
		}
	})

	t.Run("returned error path", func(t *testing.T) {
		_, _, err := wrapped.LabelNames(context.Background(), nil, time.Time{}, time.Time{})
		if err == nil || err.Error() != queryErr.Error() {
			t.Fatalf("got %v, want %v", err, queryErr)
		}
	})
}

func TestErrorWrapStillFramesOtherFailures(t *testing.T) {
	boom := errors.New("connection refused")
	wrapped := &ErrorWrap{&ErrorWrap{&failAPI{err: boom}, "error in target=http://127.0.0.1:9090"}, "error in servergroup ord=0"}

	err := wrapped.Query(context.Background(), "up", time.Now()).Err()
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"error in servergroup ord=0", "error in target=http://127.0.0.1:9090", boom.Error()} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q is missing %q", err, want)
		}
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
