// Package querystats records, per query, what each server group was asked
// for and what it returned, for the `backends` section of the API's `stats`
// output. A Recorder lives in the request context only when the client asked
// for stats; everything else in proxeus checks FromContext and does nothing
// when it is nil.
package querystats

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MaxRequests is how many request entries a Recorder keeps, by start order.
// Later requests still count towards the group totals.
const MaxRequests = 100

// Fetch paths, as set by the proxystorage wrappers.
const (
	PathPushdown = "pushdown"
	PathRaw      = "raw"
)

// Statuses of a recorded request.
const (
	StatusSuccess = "success"
	StatusError   = "error"
)

// Group identifies a configured server group in the snapshot a Recorder is
// created with.
type Group struct {
	Name    string
	Ordinal int
	Labels  map[string]string
}

// Request describes one call to a server group, as handed to it.
type Request struct {
	// GroupOrdinal is the Group.Ordinal of the group being called.
	GroupOrdinal int
	// Op is the API method: "query", "query_range" or "get_value".
	Op string
	// Path is PathPushdown or PathRaw, or "" when unknown.
	Path string
	// Query is the PromQL sent (pushdown) or the selector built from the
	// matchers (raw).
	Query string
	// Start and End bound the request; an instant query has Start == End.
	Start, End time.Time
	// Step is the query_range resolution; zero otherwise.
	Step time.Duration
}

// Recorder collects the requests of one query. It is safe for concurrent use.
type Recorder struct {
	mu        sync.Mutex
	groups    []GroupStats
	byOrdinal map[int]int // ordinal -> index in groups
	begun     int
	requests  []*RequestStats // indexed by begin order, only the first MaxRequests
	truncated int
}

// NewRecorder returns a Recorder whose output lists groups, in ordinal order,
// even when they receive no requests.
func NewRecorder(groups []Group) *Recorder {
	r := &Recorder{byOrdinal: make(map[int]int, len(groups))}
	for _, g := range groups {
		lbls := g.Labels
		if lbls == nil {
			lbls = map[string]string{}
		}
		r.groups = append(r.groups, GroupStats{Name: g.Name, Ordinal: g.Ordinal, Labels: lbls})
	}
	sort.SliceStable(r.groups, func(i, j int) bool { return r.groups[i].Ordinal < r.groups[j].Ordinal })
	for i, g := range r.groups {
		r.byOrdinal[g.Ordinal] = i
	}
	return r
}

// Begin starts timing req. Its place in the request list is decided here, by
// call order, not by when it finishes.
func (r *Recorder) Begin(req Request) *Entry {
	r.mu.Lock()
	seq := r.begun
	r.begun++
	r.mu.Unlock()
	return &Entry{r: r, seq: seq, req: req, start: time.Now()}
}

// group returns the index of the group with the given ordinal, adding a row
// for an ordinal missing from the snapshot. r.mu must be held.
func (r *Recorder) group(ordinal int) int {
	if i, ok := r.byOrdinal[ordinal]; ok {
		return i
	}
	r.groups = append(r.groups, GroupStats{Ordinal: ordinal, Labels: map[string]string{}})
	r.byOrdinal[ordinal] = len(r.groups) - 1
	return len(r.groups) - 1
}

// Stats returns what has been recorded so far.
func (r *Recorder) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Stats{
		Groups:            make([]GroupStats, len(r.groups)),
		Requests:          make([]RequestStats, 0, len(r.requests)),
		RequestsTruncated: r.truncated,
	}
	copy(s.Groups, r.groups)
	sort.SliceStable(s.Groups, func(i, j int) bool { return s.Groups[i].Ordinal < s.Groups[j].Ordinal })
	for _, rs := range r.requests {
		if rs != nil {
			s.Requests = append(s.Requests, *rs)
		}
	}
	return s
}

// Entry is one in-flight request returned by Begin.
type Entry struct {
	r     *Recorder
	seq   int
	req   Request
	start time.Time
	once  sync.Once
}

// Finish stops timing and records what the group returned. A non-nil err
// marks the request as failed. Calling Finish more than once has no effect.
func (e *Entry) Finish(series, samples int64, err error) {
	e.once.Do(func() { e.finish(series, samples, err) })
}

func (e *Entry) finish(series, samples int64, err error) {
	took := time.Since(e.start).Seconds()
	r := e.r
	r.mu.Lock()
	defer r.mu.Unlock()

	g := &r.groups[r.group(e.req.GroupOrdinal)]
	g.Requests++
	g.Series += series
	g.Samples += samples
	g.Duration += took
	if err != nil {
		g.Errors++
	}

	if e.seq >= MaxRequests {
		r.truncated++
		return
	}
	rs := &RequestStats{
		Group:    g.Name,
		Op:       e.req.Op,
		Path:     e.req.Path,
		Query:    e.req.Query,
		Start:    unixSeconds(e.req.Start),
		End:      unixSeconds(e.req.End),
		Step:     e.req.Step.Seconds(),
		Duration: took,
		Series:   series,
		Samples:  samples,
		Status:   StatusSuccess,
	}
	if err != nil {
		rs.Status = StatusError
		rs.Error = err.Error()
	}
	if len(r.requests) <= e.seq {
		r.requests = append(r.requests, make([]*RequestStats, e.seq+1-len(r.requests))...)
	}
	r.requests[e.seq] = rs
}

func unixSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / 1e9
}

// Stats is the `backends` section of the API stats.
type Stats struct {
	Groups            []GroupStats   `json:"groups"`
	Requests          []RequestStats `json:"requests"`
	RequestsTruncated int            `json:"requestsTruncated"`
}

// GroupStats sums one group's requests. Duration is the sum of their
// durations, in seconds.
type GroupStats struct {
	Name     string            `json:"name"`
	Ordinal  int               `json:"ordinal"`
	Labels   map[string]string `json:"labels"`
	Requests int               `json:"requests"`
	Errors   int               `json:"errors"`
	Series   int64             `json:"series"`
	Samples  int64             `json:"samples"`
	Duration float64           `json:"duration"`
}

// RequestStats is one request. Start and End are Unix seconds, Step and
// Duration are seconds.
type RequestStats struct {
	Group    string  `json:"group"`
	Op       string  `json:"op"`
	Path     string  `json:"path,omitempty"`
	Query    string  `json:"query"`
	Start    float64 `json:"start"`
	End      float64 `json:"end"`
	Step     float64 `json:"step,omitempty"`
	Duration float64 `json:"duration"`
	Series   int64   `json:"series"`
	Samples  int64   `json:"samples"`
	Status   string  `json:"status"`
	Error    string  `json:"error,omitempty"`
}

type recorderKey struct{}

// NewContext returns ctx carrying r.
func NewContext(ctx context.Context, r *Recorder) context.Context {
	return context.WithValue(ctx, recorderKey{}, r)
}

// FromContext returns the Recorder in ctx, or nil.
func FromContext(ctx context.Context) *Recorder {
	r, _ := ctx.Value(recorderKey{}).(*Recorder)
	return r
}

type pathKey struct{}

// WithPath marks ctx with the fetch path of the calls made under it.
func WithPath(ctx context.Context, path string) context.Context {
	return context.WithValue(ctx, pathKey{}, path)
}

// PathFromContext returns the path set by WithPath, or "".
func PathFromContext(ctx context.Context) string {
	p, _ := ctx.Value(pathKey{}).(string)
	return p
}
