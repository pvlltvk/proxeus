package promclient

import (
	"errors"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// isBackendQueryError reports whether err is a downstream API rejecting the
// query rather than failing to answer it: the bad_data and execution types,
// which Prometheus returns for a query the caller has to fix. Those reach the
// client unwrapped -- naming the target or server group that reported one adds
// nothing (every backend rejects the same query for the same reason) while
// putting an internal address in a message the user is meant to act on.
func isBackendQueryError(err error) bool {
	var respErr *promapi.ResponseError
	if errors.As(err, &respErr) {
		switch promhttputil.ErrorType(respErr.Type) {
		case promhttputil.ErrorBadData, promhttputil.ErrorExec:
			return true
		}
		return false
	}

	var apiErr *v1.Error
	if errors.As(err, &apiErr) {
		return apiErr.Type == v1.ErrBadData || apiErr.Type == v1.ErrExec
	}

	return false
}
