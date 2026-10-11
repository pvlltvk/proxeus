package main

import (
	"context"
	"net/http"
	"path"

	"github.com/prometheus/prometheus/util/stats"
	api_v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/pvlltvk/proxeus/pkg/proxystorage"
	"github.com/pvlltvk/proxeus/pkg/querystats"
)

// backendQueryStats is Prometheus' stats with the per-group backend calls
// added. BuiltinStats is embedded so its JSON stays exactly as Prometheus
// renders it.
type backendQueryStats struct {
	stats.BuiltinStats
	Backends querystats.Stats `json:"backends"`
}

func (s *backendQueryStats) Builtin() stats.BuiltinStats { return s.BuiltinStats }

// renderQueryStats is the API's stats renderer: the default one, plus
// `backends` when withQueryStats put a recorder into the request context.
func renderQueryStats(ctx context.Context, s *stats.Statistics, param string) stats.QueryStats {
	qs := api_v1.DefaultStatsRenderer(ctx, s, param)
	rec := querystats.FromContext(ctx)
	if qs == nil || rec == nil {
		return qs
	}
	return &backendQueryStats{BuiltinStats: qs.Builtin(), Backends: rec.Stats()}
}

// withQueryStats gives /api/v1/query and /api/v1/query_range requests that ask
// for stats a recorder for the server group calls they cause. Like the API
// handler, it reads `stats` with FormValue, which parses the form once for
// both of them.
func withQueryStats(next http.Handler, routePrefix string, groups func() []querystats.Group) http.Handler {
	queryPath := path.Join(routePrefix, "/api/v1/query")
	queryRangePath := path.Join(routePrefix, "/api/v1/query_range")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.URL.Path == queryPath || r.URL.Path == queryRangePath) && r.FormValue("stats") != "" {
			rec := querystats.NewRecorder(groups())
			r = r.WithContext(querystats.NewContext(r.Context(), rec))
		}
		next.ServeHTTP(w, r)
	})
}

// queryStatsGroups snapshots the configured server groups for a recorder.
func queryStatsGroups(ps *proxystorage.ProxyStorage) func() []querystats.Group {
	return func() []querystats.Group {
		cfg := ps.Config()
		if cfg == nil {
			return nil
		}
		groups := make([]querystats.Group, 0, len(cfg.ServerGroups))
		for _, sg := range cfg.ServerGroups {
			lbls := make(map[string]string, len(sg.Labels))
			for k, v := range sg.Labels {
				lbls[string(k)] = string(v)
			}
			groups = append(groups, querystats.Group{Name: sg.Name, Ordinal: sg.Ordinal, Labels: lbls})
		}
		return groups
	}
}
