package proxeusui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

// maxTSDBStatusBody caps a backend's /api/v1/status/tsdb answer; with topN at
// most 1000 per list a real one stays far below it.
const maxTSDBStatusBody = 16 << 20

// cardinalityQuery is what the Cardinality page asks every group for.
type cardinalityQuery struct {
	TopN       int
	Match      []string
	FocusLabel string
	// Date is a VictoriaMetrics day (YYYY-MM-DD); empty means today.
	Date string
}

// cardinalityBackend is one server_group as the cardinality fan-out sees it.
// BaseURLs are its targets' scheme://host{path_prefix}, which are replicas, so
// the first one that answers is enough.
type cardinalityBackend struct {
	Name        string
	BackendType servergroup.BackendType
	Client      *http.Client
	BaseURLs    []string
}

// CardinalityStat is one row of a top-N list: a metric name, label name or
// label=value pair, and its count (bytes for memoryInBytesByLabelName).
type CardinalityStat struct {
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

// CardinalityCoverage says what a group's numbers cover. Prometheus and Thanos
// report their head block ("head", MinTime/MaxTime in ms); VictoriaMetrics
// reports one day's index ("day", Date).
type CardinalityCoverage struct {
	Kind    string `json:"kind"`
	MinTime int64  `json:"minTime,omitempty"`
	MaxTime int64  `json:"maxTime,omitempty"`
	Date    string `json:"date,omitempty"`
}

// CardinalityGroup is one server_group's entry in cardinality.json. Lists the
// backend doesn't report are omitted. Groups are never summed: series kept by
// more than one group would be counted twice.
type CardinalityGroup struct {
	Name        string                  `json:"name"`
	BackendType servergroup.BackendType `json:"backendType"`
	// Target is the base URL that answered.
	Target string `json:"target,omitempty"`
	Error  string `json:"error,omitempty"`
	// NotApplied lists the requested filters this backend type doesn't
	// support, so they were not sent.
	NotApplied []string             `json:"notApplied,omitempty"`
	Partial    bool                 `json:"partial,omitempty"`
	Coverage   *CardinalityCoverage `json:"coverage,omitempty"`

	TotalSeries                  uint64            `json:"totalSeries"`
	TotalLabelValuePairs         uint64            `json:"totalLabelValuePairs"`
	SeriesCountByMetricName      []CardinalityStat `json:"seriesCountByMetricName,omitempty"`
	LabelValueCountByLabelName   []CardinalityStat `json:"labelValueCountByLabelName,omitempty"`
	SeriesCountByLabelValuePair  []CardinalityStat `json:"seriesCountByLabelValuePair,omitempty"`
	MemoryInBytesByLabelName     []CardinalityStat `json:"memoryInBytesByLabelName,omitempty"`
	SeriesCountByLabelName       []CardinalityStat `json:"seriesCountByLabelName,omitempty"`
	SeriesCountByFocusLabelValue []CardinalityStat `json:"seriesCountByFocusLabelValue,omitempty"`
}

// tsdbStatusResponse decodes both answers to /api/v1/status/tsdb: headStats
// from Prometheus and Thanos, totalSeries and the extra lists from
// VictoriaMetrics.
type tsdbStatusResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	IsPartial bool   `json:"isPartial"`
	Data      *struct {
		HeadStats *struct {
			NumSeries     uint64 `json:"numSeries"`
			NumLabelPairs uint64 `json:"numLabelPairs"`
			MinTime       int64  `json:"minTime"`
			MaxTime       int64  `json:"maxTime"`
		} `json:"headStats"`
		TotalSeries                  *uint64           `json:"totalSeries"`
		TotalLabelValuePairs         uint64            `json:"totalLabelValuePairs"`
		SeriesCountByMetricName      []CardinalityStat `json:"seriesCountByMetricName"`
		LabelValueCountByLabelName   []CardinalityStat `json:"labelValueCountByLabelName"`
		SeriesCountByLabelValuePair  []CardinalityStat `json:"seriesCountByLabelValuePair"`
		MemoryInBytesByLabelName     []CardinalityStat `json:"memoryInBytesByLabelName"`
		SeriesCountByLabelName       []CardinalityStat `json:"seriesCountByLabelName"`
		SeriesCountByFocusLabelValue []CardinalityStat `json:"seriesCountByFocusLabelValue"`
	} `json:"data"`
}

// collectCardinality asks every backend in parallel and returns one entry per
// backend, in the order given.
func collectCardinality(ctx context.Context, backends []cardinalityBackend, q cardinalityQuery) []CardinalityGroup {
	groups := make([]CardinalityGroup, len(backends))
	var wg sync.WaitGroup
	for i, b := range backends {
		wg.Go(func() { groups[i] = groupCardinality(ctx, b, q) })
	}
	wg.Wait()
	return groups
}

func groupCardinality(ctx context.Context, b cardinalityBackend, q cardinalityQuery) CardinalityGroup {
	g := CardinalityGroup{Name: b.Name, BackendType: b.BackendType}
	if g.BackendType == "" {
		g.BackendType = BackendUnknown
	}
	params, notApplied := tsdbStatusParams(b.BackendType, q)
	g.NotApplied = notApplied

	if len(b.BaseURLs) == 0 {
		g.Error = "no targets"
		return g
	}
	var errs []string
	for _, base := range b.BaseURLs {
		resp, err := fetchTSDBStatus(ctx, b.Client, base, params)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{"group": b.Name, "target": base}).Debug("proxeusui: cardinality request failed")
			errs = append(errs, base+": "+err.Error())
			continue
		}
		g.Target = base
		fillCardinality(&g, resp, q)
		return g
	}
	g.Error = strings.Join(errs, "; ")
	return g
}

// tsdbStatusParams builds the query for one backend type. Prometheus and
// Thanos take limit; VictoriaMetrics takes topN plus the filters. Thanos
// answers all zeros to any match[], so filters go to VictoriaMetrics only and
// the rest are reported back instead of silently dropped. A group that
// declares no backend_type gets both limit and topN.
func tsdbStatusParams(bt servergroup.BackendType, q cardinalityQuery) (url.Values, []string) {
	n := strconv.Itoa(q.TopN)
	v := url.Values{}
	vm := bt == servergroup.BackendVictoriaMetrics
	switch bt {
	case servergroup.BackendVictoriaMetrics:
		v.Set("topN", n)
	case "", BackendUnknown:
		v.Set("limit", n)
		v.Set("topN", n)
	default:
		v.Set("limit", n)
	}

	var notApplied []string
	filter := func(name string, set bool, apply func()) {
		switch {
		case !set:
		case vm:
			apply()
		default:
			notApplied = append(notApplied, name)
		}
	}
	filter("match[]", len(q.Match) > 0, func() { v["match[]"] = q.Match })
	filter("focusLabel", q.FocusLabel != "", func() { v.Set("focusLabel", q.FocusLabel) })
	filter("date", q.Date != "", func() { v.Set("date", q.Date) })
	return v, notApplied
}

func fetchTSDBStatus(ctx context.Context, client *http.Client, base string, params url.Values) (*tsdbStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/v1/status/tsdb?"+params.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTSDBStatusBody))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	var r tsdbStatusResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}
	if r.Status != "success" {
		return nil, fmt.Errorf("backend error: %s", r.Error)
	}
	if r.Data == nil || (r.Data.HeadStats == nil && r.Data.TotalSeries == nil) {
		return nil, errors.New("unrecognized response: neither headStats nor totalSeries")
	}
	return &r, nil
}

func fillCardinality(g *CardinalityGroup, r *tsdbStatusResponse, q cardinalityQuery) {
	d := r.Data
	g.Partial = r.IsPartial
	if d.HeadStats != nil {
		g.TotalSeries = d.HeadStats.NumSeries
		g.TotalLabelValuePairs = d.HeadStats.NumLabelPairs
		g.Coverage = &CardinalityCoverage{Kind: "head", MinTime: d.HeadStats.MinTime, MaxTime: d.HeadStats.MaxTime}
	} else {
		g.TotalSeries = *d.TotalSeries
		g.TotalLabelValuePairs = d.TotalLabelValuePairs
		date := q.Date
		if date == "" {
			date = time.Now().UTC().Format(time.DateOnly)
		}
		g.Coverage = &CardinalityCoverage{Kind: "day", Date: date}
	}
	g.SeriesCountByMetricName = nonEmpty(d.SeriesCountByMetricName)
	g.LabelValueCountByLabelName = nonEmpty(d.LabelValueCountByLabelName)
	g.SeriesCountByLabelValuePair = nonEmpty(d.SeriesCountByLabelValuePair)
	g.MemoryInBytesByLabelName = nonEmpty(d.MemoryInBytesByLabelName)
	g.SeriesCountByLabelName = nonEmpty(d.SeriesCountByLabelName)
	g.SeriesCountByFocusLabelValue = nonEmpty(d.SeriesCountByFocusLabelValue)
}

func nonEmpty(s []CardinalityStat) []CardinalityStat {
	if len(s) == 0 {
		return nil
	}
	return s
}
