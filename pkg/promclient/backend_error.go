package promclient

import (
	"errors"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"

	"github.com/pvlltvk/proxeus/pkg/promapi"
	"github.com/pvlltvk/proxeus/pkg/promhttputil"
)

// isBackendQueryError reports whether a downstream API rejected the query
// rather than failed to answer it: the bad_data and execution types. Callers
// use it to keep an internal target address out of a message the user is meant
// to act on. It is not proof that the query is at fault -- "execution" is the
// API's catch-all errorType (web/api/v1/api.go, returnAPIError), so a
// downstream that is itself a proxy reports its own plumbing failures under it
// too; that is why only the target-level wrap drops its message.
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
