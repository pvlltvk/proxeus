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
