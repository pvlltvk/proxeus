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

> **Scope:** dedup applies to raw selector results. Pushed-down aggregations fan out per-group *partials* that the
> engine re-combines, so those are unioned, never deduped. A series present in two groups appears once in `up`, but
> contributes to both partials in `count(up)`. If exact aggregates over overlapping groups matter, keep the overlap out
> of the groups.

**Backend dialects.** Declaring `backend_type` on a `server_group` (`prometheus`, `thanos`, `victoriametrics`,
`cortex`, `mimir`) unlocks a typed block of that backend's own query options — `thanos:` (`dedup`,
`partial_response`, `max_source_resolution`, `replica_labels`), `victoriametrics:` (`nocache`, `extra_filters`,
`max_lookback`, `deny_partial_response`) and `mimir:` (`tenant`, sent as `X-Scope-OrgID`). Proxeus translates them into
the params and headers that backend expects, and rejects a mistyped duration or matcher at config load rather than on
the wire. The thanos and victoriametrics knobs are HTTP-API query params, so they do not apply to `remote_read: true`
requests; the mimir tenant is a header and applies to both. The generic `query_params` / `http_headers` maps still work and override the dialect on key conflict.
`backend_type` is also what the inventory UI displays per target.

**Partial response.** `cross_group_partial_response: true` lets a query succeed when only some groups answer, attaching
a warning per failed backend. Correct for federating disjoint data, where a Thanos outage should not blank out
VictoriaMetrics-sourced series. Off by default, since for HA replicas a silent partial answer is worse than an error.

## Quickstart

```sh
docker run -p 8082:8082 -v $PWD/config.yaml:/etc/proxeus/config.yaml:ro \
  ghcr.io/pvlltvk/proxeus:latest --config=/etc/proxeus/config.yaml
```

Or build from source (Go 1.25+):

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
    # Paths that skip authentication, matched as prefixes below --web.route-prefix.
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
schemes. The authenticated name appears in the access log's user field.

`trusted_proxies` is matched against the connection's remote address, never `X-Forwarded-For`, so the header cannot be
spoofed by whoever is talking to proxeus — from an address outside the list the header is ignored entirely and the
remaining providers still run.

> The `auth` block is read at **startup only**: OIDC discovery and the provider chain are built once. A SIGHUP reload
> picks up every other change but not this one — restart proxeus after editing it.

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
      trusted_proxies: [10.0.0.0/8]   # the oauth2-proxy pods
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

Forwarding the logged-in user's OIDC token instead (Grafana must be configured with the same issuer):

```yaml
datasources:
  - name: Proxeus
    type: prometheus
    url: http://proxeus:8082
    jsonData:
      oauthPassThru: true
```

## Prometheus fork

Aggregation pushdown needs a hook inside the PromQL engine that upstream Prometheus does not expose. Proxeus therefore
depends on a patched fork, pinned in `go.mod`:

```
replace github.com/prometheus/prometheus => github.com/pvlltvk/proxeus-prometheus v0.305.0-proxeus.2
```

The patch and its rebase procedure are documented in
[proxeus-prometheus/FORK.md](https://github.com/pvlltvk/proxeus-prometheus/blob/main/FORK.md). This is the same approach
Grafana Mimir takes with `grafana/mimir-prometheus`.

> Go only honours `replace` directives in the **main** module. If you import proxeus as a *library*, copy the directive
> above into your own `go.mod`.

## Notes

**Recording and alerting rules** work, and execute across your entire federated view — a global error-rate alert that no
single backend could evaluate. Proxeus has no local TSDB, so rule output needs a `remote_write` target defined in the
config; that is where the resulting series are written.

> **In containers:** `remote_write` needs a writable directory for its WAL. The published image is built `FROM scratch`
> and runs as `nobody`, so there is nothing writable by default — pass `--storage.path` pointed at a mounted volume:
> ```sh
> docker run -p 8082:8082 --tmpfs /data:rw,mode=1777 \
>   -v $PWD/config.yaml:/etc/proxeus/config.yaml:ro \
>   ghcr.io/pvlltvk/proxeus:latest --config=/etc/proxeus/config.yaml --storage.path=/data
> ```
> Without `remote_write` configured, no storage path is needed.

**Query performance** targets the slowest backend in the fan-out. Pushdown keeps aggregate queries from dragging raw
series across the network.

**Layering** works — proxeus in front of proxeus is fine, since it is itself a Prometheus-compatible API endpoint.

**Monitoring proxeus itself:** scrape its `/metrics` and import
[`deploy/grafana/proxeus-dashboard.json`](deploy/grafana/proxeus-dashboard.json) — per-backend request rate, latency and
errors, `proxeus_server_group_targets` (alert on `== 0`), and cross-group dedup collisions.

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
