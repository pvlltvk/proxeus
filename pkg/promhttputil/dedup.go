package promhttputil

// DedupStats reports collisions resolved by cross-group dedup. Collisions
// is the number of series-level collisions (one per losing series). Pairs breaks
// that down by the {winnerOrdinal, loserOrdinal} that actually collided, so a
// caller can attribute each collision to the exact contributing server_groups.
//
// Attribution is exact regardless of input order: the caller defers Record
// until a bucket's final winner is known, so a collision is never charged to a
// backend that a later, lower ordinal goes on to beat.
type DedupStats struct {
	Collisions int
	Pairs      map[[2]int]int
}

// Record marks one collision resolved in favor of winner over loser
// (identified by their source ordinals).
func (s *DedupStats) Record(winner, loser int) {
	s.Collisions++
	if s.Pairs == nil {
		s.Pairs = make(map[[2]int]int)
	}
	s.Pairs[[2]int{winner, loser}]++
}
