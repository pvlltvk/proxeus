<p align="center">
  <img src="logo.svg" alt="proxeus" width="500">
</p>

# Proxeus

[![Go](https://github.com/pvlltvk/proxeus/workflows/Go/badge.svg)](https://github.com/pvlltvk/proxeus/actions)
[![build](https://github.com/pvlltvk/proxeus/workflows/build/badge.svg)](https://github.com/pvlltvk/proxeus/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/pvlltvk/proxeus)](https://goreportcard.com/report/github.com/pvlltvk/proxeus)

**Proxeus** (*proxy* + *Prometheus*) presents many Prometheus-compatible backends as a **single PromQL endpoint**.

Point Grafana at one datasource and query across all of them — including backends of *different kinds*. Proxeus was
built to unify **Thanos** and **VictoriaMetrics** in one place, and works with anything that speaks the Prometheus
`/api/v1` HTTP API: Prometheus itself, Thanos, VictoriaMetrics, Cortex/Mimir.

It is a stateless read path. No sidecars, no agents, no changes to the backends it federates.

## Why

Prometheus has no clustering, and no query federation across heterogeneous stores. In practice you end up with several
datasources in Grafana — which is confusing for users, and makes aggregation *across* them impossible.

Proxeus solves two problems at once:

- **HA merge.** Backends holding the same data (`server_group` replicas) are merged, so a gap in one is filled by
  another.
- **Cross-backend federation.** Backends holding *different* data are unioned into one series set, so
  `sum by (job) (rate(http_requests_total[5m]))` spans every store you have.

## How it works

Queries scatter to every configured `server_group` in parallel and gather into one result.

**Aggregation pushdown.** Reentrant aggregations (`sum`, `min`, `max`, `topk`, `bottomk`, `group`, plus `count` and
`avg` via rewrite) are sent verbatim to each backend, and only the per-group partials come back over the network — not
raw series. This is why proxeus depends on a patched PromQL engine (see [Prometheus fork](#prometheus-fork)).

**Cross-group dedup.** With `cross_group_dedup: true`, series that are identical modulo each group's external `labels`
collapse to one. Ties break on `server_groups[]` order — lowest index wins — so results are deterministic rather than
racy. Collisions are counted in `proxeus_cross_group_dedup_collisions_total`.

> **Identity rule:** two series from different `server_groups` are the same series if their labels match once you
> remove the union of every group's own `labels:` keys; `__name__` is part of the identity. Real backends stamp labels of their own on top
> of that — Thanos Receive adds `receive_replica` and `tenant_id` to everything it stores, vmagent's
> `-remoteWrite.label` and Prometheus's own `external_labels` commonly add things like `prometheus_replica` — and
> those are *not* covered by a group's `labels:` key unless you list them explicitly. `cross_group_dedup_ignore_labels`
> does that:
>
> ```yaml
> proxeus:
>   cross_group_dedup: true
>   cross_group_dedup_ignore_labels: [receive_replica, tenant_id]
> ```
>
> It's a single global list, unioned into the same ignore set every group's `labels:` already contributes — so
> `cluster` does *not* need to go here if every group declares it as a `labels:` key (the common case, and how the
> example above does it). List it here only if some backend stamps `cluster` on its series without proxeus having
> declared it as that group's label.
>
> The ignored labels only affect identity, not output: the winning series keeps its full label set, extras included.
> Overlapping series carry whichever backend won the collision's extra labels; a series only one backend has doesn't
> gain any. The effective ignore set is logged once at startup and on every reload.
>
> This only works if both stacks' scrape configs agree on `job` and `instance` for the same target — dedup can't
> reconcile `instance="10.0.0.5:9100"` on one backend with `instance="node-a"` on the other, that's a scrape-config
> problem, not a labels problem. And never list a label that's actually part of a series' identity: putting `instance`
> or `job` in `cross_group_dedup_ignore_labels` silently merges unrelated targets into one series.

> **Scope:** dedup applies to raw selector results. Pushed-down aggregations fan out per-group *partials* that the
> engine re-combines, so those are unioned, never deduped. A series present in two groups appears once in `up`, but
> contributes to both partials in `count(up)`. Pushed-down selectors and per-series functions such as `rate()` are
> evaluated in each group and their results deduped, so a series split across groups at a migration seam is evaluated
> per piece: `rate()` over a window that straddles the seam sees only one side, and a group that stopped receiving
> the series can still win with its last sample for the 5m lookback.
>
> **Exact mode:** `cross_group_exact: true` (requires `cross_group_dedup`) fixes both. With more
> than one `server_group` proxeus pushes nothing down, so the engine evaluates the whole query over the deduplicated,
> gap-filled raw series: `count(up)` matches `up`, and `rate()` across a seam matches a single store holding the full
> history. The decision is visible as `proxeus_pushdown_nodes_total{result="fallback",reason="exact"}`.
> The cost is the point of the trade: raw samples cross the network instead of per-group partials and step-aligned
> results, which hurts most on wide ranges and high cardinality, and a query that used to fit under
> `--query.max-samples` can now exceed it and fail outright. A single-`server_group` deployment is unaffected — one backend sees the whole series set, so there is
> nothing to dedup. The gate counts *configured* groups, not overlapping ones, so a deployment whose groups hold
> disjoint data pays the cost for no correctness gain.
>
> "Exact" means exact modulo dedup's fingerprint: overlapping series have to be identical apart from each group's
> declared `labels`, so if a backend stamps anything else of its own (a Thanos `prometheus_replica`, say) dedup won't
> collapse them and the aggregate still double-counts.
>
> Raw samples fetched over the HTTP API (groups without `remote_read: true`) arrive without Prometheus staleness
> markers: the raw fetch is a `foo[Ns]` range query, and range selectors drop them. In exact mode a series that
> disappeared can therefore keep answering instant queries for up to the lookback delta (5m by default).
> `remote_read: true` groups keep the markers.

> **Known overlap.** When you know which series more than one group holds (endpoints scraped both by a Thanos
> and by a VictoriaMetrics), exclude them from all but one group with `inject_matchers`. The groups are then disjoint,
> so pushed-down aggregations are exact without paying for exact mode, and dedup stays on as the safety net for overlap
> you didn't list:
>
> ```yaml
> proxeus:
>   cross_group_dedup: true
>   server_groups:
>     - labels: {backend: thanos}          # first: wins dedup, has the longest history
>     - labels: {backend: vm-a}
>       inject_matchers: ['job!~"node-exporter|kube-state-metrics"']   # endpoints Thanos already has
> ```
>
> The excluded copy can no longer fill gaps in the one you kept.
>
> Keep a classic histogram whole: `histogram_quantile` and `histogram_fraction` are pushed down per group, so all
> `_bucket` series of one histogram must live in the same group. Don't split buckets across groups (by `le` in
> `inject_matchers`, say). Native histograms are unaffected.

> **Joins across groups.** Every series carries its group's `labels`, so vector matching between series from
> different groups never matches on its own: with `foo` in `backend="thanos"` and `bar` in `backend="vm-a"`,
> `foo + bar` is empty and `foo or bar` returns both instead of dropping `bar`. Exclude the group label from the
> matching, as in `foo + ignoring(backend) bar`, or match with `on(...)`.
>
> The same labels change `limit_ratio`: it picks series by a hash of their labels, so it selects a different subset
> than the same query against one store holding all the series.

> **Gap filling.** A hole in the dedup winner's series is filled from the other groups in priority order — typical
> during a migration, when the new backend lacks history or the old one stops first. The winner's own samples are
> never replaced. A gap is the span before the winner's first sample or after its last, an interval of at least
> `cross_group_dedup_gap`, or the span after a `StaleNaN`. It is on by default with `cross_group_dedup`:
>
> ```yaml
> proxeus:
>   cross_group_dedup: true
>   cross_group_dedup_fill_gaps: true   # default; false returns the winner whole
>   cross_group_dedup_gap: 0            # 0 = auto: twice the winner's median sample interval
> ```
>
> Values can differ slightly at a filled seam (downsampled Thanos next to raw VictoriaMetrics, or different scrape
> moments), as with Thanos Query's own replica dedup. Pushed-down aggregations are not deduped and so not filled; with
> `cross_group_exact: true` they are. Every series found in more than one group has all its copies read,
> which costs memory when most series overlap.

**Backend dialects.** Declaring `backend_type` on a `server_group` (`prometheus`, `thanos`, `victoriametrics`,
`cortex`, `mimir`) unlocks a typed block of that backend's own query options — `thanos:` (`dedup`,
`partial_response`, `max_source_resolution`, `replica_labels`), `victoriametrics:` (`nocache`, `extra_filters`,
`max_lookback`, `deny_partial_response`, `raw_fetch`) and `mimir:` (`tenant`, sent as `X-Scope-OrgID`). Proxeus translates them into
the params and headers that backend expects, and rejects a mistyped duration or matcher at config load rather than on
the wire. The thanos and victoriametrics knobs are HTTP-API query params, so they do not apply to `remote_read: true`
requests; the mimir tenant is a header and applies to both. The generic `query_params` / `http_headers` maps still work and override the dialect on key conflict.
`backend_type` is also what the inventory UI displays per target.

**VictoriaMetrics raw fetches.** VictoriaMetrics implements no remote_read, so raw samples (what a query that can't be
pushed down needs) normally come back as matrix JSON from `/api/v1/query` — quoted strings, one array per sample, which
is what dominates a wide 1y range. `victoriametrics: {raw_fetch: export}` fetches them from VictoriaMetrics'
`/api/v1/export` NDJSON endpoint instead: bare numbers, millisecond timestamps, roughly half the decode cost.
Opt-in, and only the raw path changes — instant and range queries, series and labels stay on the v1 API. Two caveats:
export applies neither the lookback-delta nor `-search.latencyOffset`, so the freshest ~30s can include samples
VictoriaMetrics' own `/api/v1/query` would hide; and it is mutually exclusive with `remote_read: true` (rejected at
config load). `export_max_rows_per_line` caps the samples one exported line carries.

**Partial response.** `cross_group_partial_response: true` lets a query succeed when only some groups answer, attaching
a warning per failed backend. Correct for federating disjoint data, where a Thanos outage should not blank out
VictoriaMetrics-sourced series. Off by default, since for HA replicas a silent partial answer is worse than an error.

## Quickstart

```sh
docker run -p 8082:8082 -v $PWD/config.yaml:/etc/proxeus/config.yaml:ro \
  ghcr.io/pvlltvk/proxeus:latest --config=/etc/proxeus/config.yaml
```

Or build from source (Go 1.27+):

```sh
git clone git@github.com:pvlltvk/proxeus.git
cd proxeus/cmd/proxeus && go build -tags netgo
./proxeus --config=config.yaml --bind-addr=:8082
```

A commented example config lives at [`cmd/proxeus/config.yaml`](cmd/proxeus/config.yaml).

### Minimal config

```yaml
global:
  evaluation_interval: 5s

proxeus:
  cross_group_dedup: true

  server_groups:
    - static_configs:
        - targets: ['thanos-query:9090']
      labels:
        backend: thanos

    - static_configs:
        - targets: ['victoriametrics:8428']
      labels:
        backend: vm
```

Proxeus then serves the Prometheus API at `:8082`, plus a backend inventory UI at `/proxeus/backends`.

## Authentication

Without an `auth` block every request is anonymous, as before. Adding one turns on a chain of providers, tried in a
fixed order — **trusted_header, basic, oidc** — each of which either finds no credentials it recognises (the next one
gets a turn), authenticates the caller, or rejects the request outright. Credentials one provider claims are never
retried by another: a wrong password is a 401, not a fall-through to the bearer token in the same request.

```yaml
proxeus:
  auth:
    # Paths that skip authentication, matched verbatim on whole segments —
    # include --web.route-prefix yourself if you set one. Setting this replaces
    # the defaults (/-/healthy, /-/ready and --metrics-path, prefixed for you).
    exempt_paths: [/-/healthy, /-/ready, /metrics]

    # Username -> bcrypt hash, the same shape as exporter-toolkit's basic_auth_users.
    # Generate with: htpasswd -nBC 10 "" | tr -d ':\n'
    basic:
      users:
        alice: $2a$10$nRYmVvmznzCXqV9O7Bq/beEBbTBlv7GVEt9gyhqiGt.lZdBYcojHK

    # Bearer tokens verified against an OIDC issuer. Discovery runs at startup.
    oidc:
      issuer_url: https://issuer.example/realms/main
      client_id: proxeus              # expected `aud`, or `azp` for Keycloak-style tokens
      username_claim: preferred_username   # default: sub
      groups_claim: groups            # optional

    # Identity from an authenticating proxy in front of proxeus.
    trusted_header:
      user_header: X-Forwarded-User
      groups_header: X-Forwarded-Groups   # optional, comma-separated
      trusted_proxies: [127.0.0.1/32]     # required: CIDRs the header is honoured from
```

A request that reaches the end of the chain without an identity gets a 401 with `WWW-Authenticate` listing the enabled
schemes — except when trusted_header is the only provider, where there is no auth scheme to advertise and the 401 is
sent bare. CORS preflights (`OPTIONS` with `Access-Control-Request-Method`) skip the chain, since a browser will not
attach credentials to one. The authenticated name appears in the access log's user field.

`trusted_proxies` is matched against the connection's remote address, never `X-Forwarded-For`, so the header cannot be
spoofed by whoever is talking to proxeus — from an address outside the list the header is ignored entirely and the
remaining providers still run.

> Keep `trusted_proxies` to the narrowest range that covers the proxy itself, ideally a `/32`. Anything that can
> connect to proxeus from inside that range can name itself any user it likes.

> The `auth` block is read at **startup only**: OIDC discovery and the provider chain are built once. A SIGHUP reload
> picks up every other change but not this one — restart proxeus after editing it.

### Authorization

Authentication says who the caller is; the optional `authorization` block says which of those callers are served.
Without it every authenticated identity is, as before.

```yaml
proxeus:
  auth:
    authorization:
      # An identity passes when its name is listed here or one of its groups is
      # listed below. Leave both lists out to let every authenticated identity
      # through and restrict with routes alone.
      allowed_users:  [alice, admin@example.com]
      allowed_groups: [admins]

      # Per-path rules, applied on top of the lists above — a caller must pass
      # the lists and every rule whose path_prefix covers the path, so order
      # does not matter. Paths no rule covers need only the top-level policy.
      # Each rule needs at least one non-empty list.
      routes:
        - path_prefix: /mcp
          allowed_users: []
          allowed_groups: [mcp-users]
```

The common "everyone may query, one account may use MCP" setup is therefore just a route rule:

```yaml
proxeus:
  auth:
    authorization:
      routes:
        - path_prefix: /mcp
          allowed_users: [mcp-bot]
```

A denied caller gets a 403 with no `WWW-Authenticate`: the credentials were fine, sending them again changes nothing.
`path_prefix` matches on whole segments the way `exempt_paths` does — `/mcp` covers `/mcp/messages` but not `/mcpx` —
and, like `exempt_paths`, it must include `--web.route-prefix` if you set one — also when the prefix is only implied
by the path of `--web.external-url`. A rule whose prefix never matches restricts nothing, so proxeus logs a warning at
startup for rules outside the route prefix. Exempt paths stay exempt: they skip authentication and authorization alike.
Like the rest of the `auth` block, the policy is read at startup only; a SIGHUP reload does not change it.

Groups are whatever the provider hands over: the OIDC `groups_claim`, the trusted-header `groups_header`, and nothing
at all for basic auth. A policy that only lists `allowed_groups` therefore locks out every basic-auth user — name them
in `allowed_users` if they should get through. Names and groups are compared exactly, case included: list them the way
the provider spells them.

### Interaction with `--web.config.file`

exporter-toolkit's `basic_auth_users` (in the `--web.config.file`) runs *outside* proxeus and rejects anything without
a `Basic` header, bearer tokens included. Use one or the other: `--web.config.file` for TLS only, `proxeus.auth` for
identity.

### Browser SSO with oauth2-proxy

proxeus has no login flow, so put oauth2-proxy in front of the UI and let it pass the identity through:

```
oauth2-proxy --upstream=http://proxeus:8082 \
  --set-xauthrequest --pass-user-headers \
  --provider=oidc --oidc-issuer-url=https://issuer.example/realms/main
```

```yaml
proxeus:
  auth:
    trusted_header:
      user_header: X-Forwarded-User
      groups_header: X-Forwarded-Groups
      # the oauth2-proxy address only — everything in this range can claim to
      # be any user
      trusted_proxies: [10.42.7.13/32]
```

### Grafana

Basic auth:

```yaml
datasources:
  - name: Proxeus
    type: prometheus
    url: http://proxeus:8082
    basicAuth: true
    basicAuthUser: grafana
    secureJsonData:
      basicAuthPassword: ...
```

Forwarding the logged-in user's OIDC token instead, with Grafana configured against the same issuer:

```yaml
datasources:
  - name: Proxeus
    type: prometheus
    url: http://proxeus:8082
    jsonData:
      oauthPassThru: true
```

The forwarded token is issued to *Grafana*, so the issuer has to be told to name proxeus in it: add proxeus'
`client_id` to the token's `aud` (in Keycloak, an "Audience" mapper on the Grafana client's dedicated scope adding the
proxeus client). Without that step proxeus rejects every forwarded token as the wrong audience.

## High availability

proxeus is stateless: run as many replicas as you like behind one Service, each answers queries on its own, and
nothing is shared between them.

**Rules and alerts** are not evaluated by proxeus; a config with `rule_files`, `alerting` or `remote_write` is
rejected at load. Point a ruler at proxeus as its query endpoint instead, and its rules span every backend:

- **Thanos Ruler**: `thanos rule --query=http://proxeus:8082`, writing to its own object store or `remote_write`.
- **vmalert**: `-datasource.url=http://proxeus:8082`.

Proxeus does not serve `/api/v1/read` (it answers 501); rulers query it over the HTTP API.

## Prometheus fork

Aggregation pushdown needs a hook inside the PromQL engine that upstream Prometheus does not expose. Proxeus therefore
depends on a patched fork, pinned in `go.mod`:

```
replace github.com/prometheus/prometheus => github.com/pvlltvk/proxeus-prometheus v0.305.0-proxeus.4
```

The patch and its rebase procedure are documented in
[proxeus-prometheus/FORK.md](https://github.com/pvlltvk/proxeus-prometheus/blob/main/FORK.md). This is the same approach
Grafana Mimir takes with `grafana/mimir-prometheus`.

> Go only honours `replace` directives in the **main** module. If you import proxeus as a *library*, copy the directive
> above into your own `go.mod`.

## Notes

**Query performance** targets the slowest backend in the fan-out. Pushdown keeps aggregate queries from dragging raw
series across the network.

**Layering** works — proxeus in front of proxeus is fine, since it is itself a Prometheus-compatible API endpoint.

**Monitoring proxeus itself:** scrape its `/metrics` and import
[`deploy/grafana/proxeus-dashboard.json`](deploy/grafana/proxeus-dashboard.json) — per-backend request rate, latency
percentiles and errors, `proxeus_server_group_targets` (alert on `== 0`), and cross-group dedup collisions.

**Pushdown metrics** show how much of a query the backends answer, and how much proxeus drags across the network:

- `proxeus_pushdown_nodes_total{node,result,reason}` — one PromQL AST node decision per increment. `result` is
  `pushed` or `fallback`; `reason` names the branch that gave up (`multi_vector_selector`, `nested_aggregate`,
  `histogram`, `lossy_histogram`, `non_reentrant_agg`, ...) and is empty for `pushed`.
- `proxeus_raw_series_fetches_total` — fetches of raw series for local evaluation, i.e. the queries pushdown could not
  help with. Rising while the `pushed` rate stays flat means a dashboard query has left the fast path.
- `proxeus_backend_series_total{path}` and `proxeus_backend_samples_total{path}` — volume read from the backends,
  split into `pushdown` and `raw`.

> A `fallback` is not a failure — plenty of queries (`stddev`, vector-to-vector binaries, native histograms) can only
> be evaluated centrally. The number to watch is the *sample volume* on the `raw` path, since that is what a long
> range query actually costs.

## MCP endpoint

Proxeus can serve a [Model Context Protocol](https://modelcontextprotocol.io) endpoint, so an agent can query the
federated view over the same path Grafana uses. It is off by default:

```sh
./proxeus --config=config.yaml --mcp.enable
```

The endpoint is streamable HTTP at `<route-prefix>/mcp`, stateless (no session state, so replicas behind a load
balancer are interchangeable). Tools call proxeus's own `/api/v1`, so dedup, pushdown and the backend metrics apply
exactly as they do for any other client.

| flag | meaning | default |
| --- | --- | --- |
| `--mcp.enable` | serve the MCP endpoint | off |
| `--mcp.max-series` | max series (or label names/values) a tool call returns; `0` disables the cap | `100` |
| `--mcp.max-samples` | max samples a tool call returns; `0` disables the cap | `5000` |
| `--mcp.query-timeout` | max time for one tool call; `0` disables the timeout | `60s` |

Tool names, argument names and descriptions mirror
[prometheus/prometheus-mcp](https://github.com/prometheus/prometheus-mcp), so skills written for that server work here
unchanged. All tools are read-only; there are no admin or TSDB tools.

| tool | arguments | returns |
| --- | --- | --- |
| `query` | `query`, `timestamp`, `truncation_limit` | instant query: one value per series |
| `range_query` | `query`, `start_time`, `end_time`, `step`, `truncation_limit` | range query; `step` defaults to ~250 points |
| `series` | `matches` (required), `start_time`, `end_time`, `truncation_limit` | the label sets that match |
| `label_names` | `matches`, `start_time`, `end_time`, `truncation_limit` | label names |
| `label_values` | `label` (required), `matches`, `start_time`, `end_time`, `truncation_limit` | values of one label |
| `metric_metadata` | `metric`, `limit` | type/help/unit per metric name |
| `build_info` | — | proxeus build information |
| `list_server_groups` | — | the `server_groups` behind the fan-out: backend type, per-target health, labels, configured time range, `remote_read` |

`list_server_groups` is the proxeus-specific one, and the reason a model can explain its own results: a metric may be
missing or a range partial because the backend holding it is unhealthy or configured for a different time range.

Results are capped by the flags above; `truncation_limit` may lower a cap for one call but never raise it. A truncated
result says so (`truncated`, `returned`, `total_before_truncation`, `note`), so the model can narrow the query instead
of guessing that the data does not exist. For `range_query` the sample cap is usually the one that bites first: with
the default step (~250 points per series) 5000 samples is about 20 series, well below `--mcp.max-series`. Tool calls are counted in `proxeus_mcp_tool_calls_total{tool,result}` and timed
in `proxeus_mcp_tool_call_duration_seconds{tool}`.

> **Do not expose this endpoint unauthenticated.** The MCP package carries no auth of its own: anything that can POST
> to `/mcp` can read every metric in every backend. It is served by the same router as everything else, so an
> [`auth` block](#authentication) covers it — do not put `/mcp` in `exempt_paths`, and use an
> [`authorization` route rule](#authorization) if only some of your users should reach it. Basic auth via `--web.config.file`
> (the [Prometheus HTTPS/auth schema](https://prometheus.io/docs/prometheus/latest/configuration/https/)) or auth
> terminated in your ingress work too. MCP clients pass credentials in the `Authorization` header:

```json
{
  "mcpServers": {
    "proxeus": {
      "type": "http",
      "url": "https://proxeus.example.com/mcp",
      "headers": {
        "Authorization": "Basic <base64 user:password>"
      }
    }
  }
}
```

Pointing [prometheus-mcp](https://github.com/prometheus/prometheus-mcp) at proxeus works too, and is the better choice
if you want its documentation and runbook tools — proxeus is a Prometheus-compatible API endpoint like any other. The
embedded endpoint exists so that a proxeus deployment needs no second process, and so the agent can ask about the
fan-out itself.

## Load testing with fakeprom

Proxeus's own throughput is hard to measure against a real backend: a laptop-sized Thanos or VictoriaMetrics saturates
first, and the numbers end up describing the backend. `pkg/fakeprom` is a synthetic Prometheus HTTP API
(`/api/v1/query`, `/query_range`, `/series`, `/labels`, `/label/<name>/values`, `/status/buildinfo`, `/-/healthy`,
`/-/ready`) that generates every answer from a deterministic function of the series index and timestamp and streams the
JSON straight to the client, so it costs almost nothing per sample and never materializes a response in memory.

```sh
make fakeprom
./build/fakeprom --bind-addr=:9090 --series=100000 --instance=0 --overlap=0.5
./build/fakeprom --bind-addr=:9091 --series=100000 --instance=1 --overlap=0.5
```

| flag | meaning | default |
| --- | --- | --- |
| `--bind-addr` | address to listen on | `:9090` |
| `--series` | cardinality: how many series every query returns | `1000` |
| `--instance` | id of this backend, see overlap below | `0` |
| `--overlap` | fraction of the series shared with the other instances | `0` |
| `--latency` | delay added to every response | none |
| `--metric-name` | `__name__` of the generated series | `fake_metric` |
| `--max-samples-per-series` | cap on samples per series in a range response | uncapped |

**Overlap semantics.** Two fakeproms with the same `--series` and `--overlap` but different `--instance` serve exactly
`round(overlap * series)` series that are byte-identical in both labels *and* values; the remaining ones carry the
instance id in their `instance` label and are therefore disjoint. That is precisely what proxeus's cross-group dedup
keys on, so `--overlap=0.5` across two server_groups must collapse to `1.5 * series` with `cross_group_dedup: true`.

**Query handling** is a heuristic, not a PromQL implementation: if the query starts with an aggregation operator
(`sum`, `count`, `avg`, `min`, `max`, `topk`, ...) the backend answers with a single series, modelling what a real
backend returns for a pushed-down aggregation; anything else returns the full series set. Matchers are ignored.

### Benchmarks

`test/fakeprom_bench_test.go` drives HTTP `query_range` requests through a real `ProxyStorage` and Prometheus v1 API in
front of fakeprom backends, sweeping cardinality × server_groups × `cross_group_dedup` × query shape:

```sh
make bench-e2e                                   # default sweep, -benchmem
make bench-e2e BENCHTIME=10x BENCH_PROFILE_DIR=/tmp/prof   # plus cpu/mem profiles
```

The default query is a 1 hour range at a 15s step. Override with `PROXEUS_BENCH_RANGE`, `PROXEUS_BENCH_STEP`,
`PROXEUS_BENCH_OVERLAP` and `PROXEUS_BENCH_MAX_SAMPLES`. The last one is a ceiling on the worst-case response size
(default 8M samples): raw-selector cases above it are skipped rather than risking an OOM, which is why 100k series over
an hour at 15s (24M samples across two groups) does not run by default. To measure that tier, shorten the range:

```sh
PROXEUS_BENCH_RANGE=5m PROXEUS_BENCH_STEP=60s make bench-e2e
```

## Relationship to promxy

Proxeus is a hard fork of [promxy](https://github.com/jacksontj/promxy) by Thomas Jackson, and owes it the core
scatter-gather and HA-merge design. It diverges in aiming squarely at **heterogeneous multi-backend federation** —
deterministic cross-group dedup, per-backend partial response, and a backend inventory UI — rather than HA over
identical Prometheus replicas.

The fork is final; there is no upstream coordination. Original copyright is retained in [LICENSE](LICENSE).

## Contributing

Issues and pull requests welcome.

## License

MIT — see [LICENSE](LICENSE).
