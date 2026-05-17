package proxystorage

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// crossGroupDedupCollisions counts series collisions resolved by ordinal
// tie-break during cross-server_group dedup (B1). Label values are the winning
// and losing server_group names as defined in the promxy configuration.
var crossGroupDedupCollisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "promxy_cross_group_dedup_collisions_total",
		Help: "Number of cross-server_group series collisions resolved by ordinal tie-break.",
	},
	[]string{"winner", "loser"},
)

// crossGroupDedupMetadataCollisions counts collisions resolved while deduping
// metadata-style endpoints (currently only /api/v1/series) when
// cross_group_dedup_metadata is enabled (F2). The `endpoint` label allows
// future expansion to other endpoints without breaking dashboards.
var crossGroupDedupMetadataCollisions = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "promxy_cross_group_dedup_metadata_collisions_total",
		Help: "Number of cross-server_group metadata collisions resolved by ordinal tie-break, by endpoint.",
	},
	[]string{"winner", "loser", "endpoint"},
)
