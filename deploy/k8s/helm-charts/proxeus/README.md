# proxeus Helm chart

Deploys [proxeus](https://github.com/pvlltvk/proxeus) — a single PromQL endpoint over many
Prometheus-compatible backends.

## Install

```sh
helm install proxeus ./deploy/k8s/helm-charts/proxeus -f my-values.yaml
```

## Configuration

The proxeus config itself goes under the `config:` key in values, and is rendered into a ConfigMap mounted at
`/etc/proxeus/config.yaml`:

```yaml
config:
  global:
    evaluation_interval: 5s
  proxeus:
    cross_group_dedup: true
    server_groups:
      - static_configs:
          - targets: ['thanos-query:9090']
        labels:
          backend: thanos
```

Set `configMap: <name>` instead to point at a ConfigMap you manage yourself; `config:` is then ignored.

`config.proxeus.server_groups` has **no default** and the chart refuses to render while it is empty — there is
no sensible guess, and the obvious one is a trap. An unscoped

```yaml
    server_groups:
      - kubernetes_sd_configs:
          - role: pod            # do not do this
```

makes every pod in the cluster a PromQL backend, proxeus included, on every port each pod declares. One
`/api/v1/query?query=up` then fans out across all of them: the pod grew past 1.8Gi in under a minute and was
OOMKilled while the query was still returning `context deadline exceeded`. Discovery-based groups need a
namespace scope and a port filter, e.g.

```yaml
    server_groups:
      - kubernetes_sd_configs:
          - role: service
            namespaces:
              names: [monitoring]
        relabel_configs:
          - source_labels: [__meta_kubernetes_service_name, __meta_kubernetes_service_port_name]
            regex: thanos-query;http
            action: keep
```

| key | default | notes |
|---|---|---|
| `replicaCount` | `1` | proxeus is stateless unless `storage.persistence` is on, but see [Rule evaluation and replicas](#rule-evaluation-and-replicas) before scaling out |
| `image.repository` | `ghcr.io/pvlltvk/proxeus` | `image.tag` empty means the chart's `appVersion`; `image.digest` pins by content |
| `service` | ClusterIP on 8082 | |
| `ingress` | disabled | authenticate proxeus (see below) before exposing it |
| `networkPolicy` | disabled | ingress to 8082 only, from the peers you list |
| `config.proxeus.server_groups` | none | required: the chart refuses to render without backends, see above |
| `serviceMonitor` / `podMonitor` | disabled | need the Prometheus Operator CRDs; skipped, with a warning in NOTES, when absent |
| `probes` | plain HTTP, no headers | scheme and headers for the liveness/readiness probes, see [Secrets](#secrets) |
| `hpa` / `verticalAutoscaler` | disabled | an actuating VPA `updateMode` together with the HPA is refused; the VPA needs its own CRDs and is skipped, with a warning, when absent |
| `rules.alertingOnly` | `false` | states that `config.rule_files` records nothing, which is what makes more than one replica safe — see [Rule evaluation and replicas](#rule-evaluation-and-replicas) |
| `podDisruptionBudget` | disabled | `maxUnavailable: 1` unless you set a field yourself |
| `serviceAccount` | created | |
| `rbac.create` | `true` | ClusterRole reading pods/services/endpoints, for `kubernetes_sd_configs` |
| `podSecurityContext` / `securityContext` | locked down | non-root 65534, read-only root filesystem, all capabilities dropped, `RuntimeDefault` seccomp |
| `storage.path` | `/var/lib/proxeus` on an `emptyDir` | see [Storage](#storage) |
| `resources` | 500m/1Gi requested, 2Gi limit | a floor for one quiet replica, not a recommendation |
| `configCheck.enabled` | `true` | init container running `--check-config` |
| `webLifecycle` | `false` | see [Reloading config](#reloading-config) |
| `mcp` | disabled | see [MCP](#mcp) |
| `extraArgs` | `log-level: info` | extra CLI flags, e.g. `log-level: debug`; wins over the flags the chart sets itself |
| `configmapReloader` | disabled | sidecar that hits `/-/reload` when the ConfigMap changes |

See [`values.yaml`](values.yaml) for the full set, and [`ci/`](ci/) for worked combinations that CI renders on
every change.

## Storage

proxeus has no TSDB, but it does need a writable directory: the remote_write WAL behind recording rules and the
active query tracker behind `--query.max-concurrency` both live under `--storage.path`. The image is built
`FROM scratch`, so there is no writable `/tmp` to fall back on, and `readOnlyRootFilesystem` removes the last
one. The chart therefore always sets `--storage.path` and mounts something there — an `emptyDir` by default:

```yaml
storage:
  path: /var/lib/proxeus
  persistence:
    enabled: true      # only to keep the WAL across restarts
    size: 10Gi
```

Every replica of the Deployment mounts the same claim, so `storage.persistence` with `replicaCount > 1` is
refused unless the access mode is `ReadWriteMany`.

## Rule evaluation and replicas

proxeus evaluates `config.rule_files` itself, in-process, and has **no leader election**. Every replica therefore
evaluates every rule:

- alerting rules are fine — Alertmanager deduplicates by label set, the same as a pair of HA Prometheus servers;
- recording rules are not — each replica `remote_write`s the same series, so the write target receives one copy per
  replica.

So `config.rule_files` together with more than one possible replica (`replicaCount > 1`, or `hpa.enabled` with
`maxReplicas > 1`) is refused. Three ways out:

```yaml
# 1. one replica, rules evaluated in proxeus -- see ci/rules-values.yaml
replicaCount: 1
hpa:
  enabled: false

# 2. rule files that only alert, never record
replicaCount: 3
rules:
  alertingOnly: true

# 3. no rules here at all: an external ruler (Thanos Ruler, a rules-only
#    Prometheus) queries proxeus and writes its own output
replicaCount: 3
```

Option 3 is the one that scales, and it keeps the federated view: the ruler's queries go through proxeus, so its rules
still span every backend. See [High availability](../../../../README.md#high-availability).

With `configMap: <name>` the chart cannot read the config, so it cannot tell whether your ConfigMap has `rule_files`
and the check does not run. Multiple replicas with an external ConfigMap are on you.

## Shutdown

proxeus stops reporting itself ready, waits `shutdownDelay` (`--http.shutdown-delay`, 10s) so that upstream
load balancers notice, and only then drains in-flight requests for up to `shutdownTimeout`
(`--http.shutdown-timeout`, 60s). Kubernetes' default 30s grace period would SIGKILL it partway through, so the
chart derives `terminationGracePeriodSeconds` from those two values (plus a small margin) — including when they
are overridden through `extraArgs`:

```yaml
extraArgs:
  http.shutdown-delay: 30s
  http.shutdown-timeout: 2m
# -> terminationGracePeriodSeconds: 155
```

Set `terminationGracePeriodSeconds` explicitly to take it over.

## Secrets

proxeus does **not** expand environment variables in its config file, so credentials cannot be injected as
`${VARS}`. Everything secret has to arrive as a file, or as a Secret-rendered config:

- Per-backend credentials: use the `_file` forms of the server groups' `http_client`
  (`bearer_token_file`, `basic_auth.password_file`, `tls_config.{ca,cert,key}_file`) and mount the Secret with
  `extraSecretMounts`. See [`ci/secrets-values.yaml`](ci/secrets-values.yaml).
- TLS for proxeus' own listener, or `basic_auth_users` for `--web.config.file`: put the file in a Secret and set
  `webConfig.existingSecret`; the chart mounts it at `/etc/proxeus-web/` and passes the flag. Unlike
  `config.proxeus.auth`, that file covers **every** path — `/-/healthy`, `/-/ready` and `/metrics` included —
  so the probes and any ServiceMonitor have to be told about it, see below.
- `config.proxeus.auth.basic.users` holds bcrypt hashes inline. Those go through the ConfigMap like the rest of
  the config; if you would rather not have them in a ConfigMap at all, render the whole config into a Secret
  yourself — but note that `configMap: <name>` expects a ConfigMap, so this needs `extraVolumes`/
  `extraVolumeMounts` over `/etc/proxeus` today.
- Environment variables the Prometheus SD libraries read themselves (cloud credentials, `HTTP_PROXY`) go in
  `env` / `envFrom`.

### Which authentication to use

`config.proxeus.auth` exempts `/metrics`, `/-/healthy` and `/-/ready` by default, so probes and ServiceMonitors
keep working with nothing extra. Override `auth.exempt_paths` and you own that.

`--web.config.file` has no exemptions. Everything the kubelet or Prometheus sends to a `webConfig` release needs
credentials of its own, so the chart cannot infer what that Secret contains and asks:

```yaml
webConfig:
  existingSecret: proxeus-web-config
  basicAuthUsers: true          # the file sets basic_auth_users
  tls: true                     # the file sets tls_server_config

probes:
  scheme: HTTPS                 # required by webConfig.tls
  httpHeaders:                  # required by webConfig.basicAuthUsers
    - name: Authorization
      value: Basic YWxpY2U6czNjcmV0

serviceMonitor:
  enabled: true
  scheme: https
  tlsConfig:
    insecureSkipVerify: true
  basicAuth:
    username:
      name: proxeus-scrape-credentials
      key: username
    password:
      name: proxeus-scrape-credentials
      key: password
```

Leaving any of those out is refused at render time rather than shipped: without `probes.httpHeaders` the kubelet
gets a 401 from `/-/healthy`, liveness fails, and the pod restarts forever. `probes.httpHeaders` ends up in the
pod template in clear text, which is the other reason to prefer `config.proxeus.auth`.

## Reloading config

`--web.enable-lifecycle` serves `/-/reload` and `/-/quit`, which proxeus does not authenticate separately —
anything that can reach the pod can reload or stop it. So the chart leaves it off, and rolls the pods instead:
the pod template carries a `checksum/config` annotation, so `helm upgrade` with a changed `config:` restarts
them.

The reload sidecar is for the other case, a ConfigMap edited outside Helm. It needs the endpoint it posts to, so
`configmapReloader.enabled: true` without `webLifecycle: true` is refused rather than silently reloading
nothing. With `config.proxeus.auth` configured, also add `/-/reload` to `auth.exempt_paths` or the sidecar's
POST gets a 401.

## MCP

`--mcp.enable` is read-only but unauthenticated on its own, which is why its own flag help says it must not be
exposed without authentication. The chart enforces that: `mcp.enabled: true` renders only when
`config.proxeus.auth` or `webConfig.existingSecret` is set, or when you state that something in front of proxeus
authenticates:

```yaml
mcp:
  enabled: true
  authenticatedByProxy: true   # oauth2-proxy, an authenticating ingress, ...
```

## Values `helm upgrade` cannot change

Some values map onto Kubernetes fields that are immutable after creation. The chart still renders the new value,
the API server rejects or ignores it, and the release looks fine:

| value | what happens |
|---|---|
| `service.clusterIP` | immutable on an existing Service; the upgrade fails. Delete and recreate the Service, or the release |
| `storage.persistence.size` | only grows, and only on a StorageClass with `allowVolumeExpansion: true`; ignored otherwise |
| `storage.persistence.storageClassName` | immutable on an existing claim. A claim that never bound (a class that does not exist) cannot be fixed by an upgrade either — delete the PVC, then upgrade |

Pick the storage values before the first install, and check `kubectl get storageclass` for a class that actually
exists: naming one that does not leaves the claim `Pending` and the pod unschedulable, with no way out through
`helm upgrade`.

## Upgrading to chart 0.3.0

- `config.proxeus.server_groups` no longer defaults to `kubernetes_sd_configs: - role: pod`. That default made
  every pod in the cluster a backend and OOMKilled proxeus on the first query; it is now empty and required. An
  upgrade that relied on it is refused until the backends are spelled out.
- `webConfig.existingSecret` now needs `webConfig.tls` and/or `webConfig.basicAuthUsers`, and the matching
  `probes` (and monitor) settings. The combination was accepted before and crash-looped.
- `mcp.enabled` is no longer satisfied by `webConfig.existingSecret` alone — `webConfig.basicAuthUsers` (or
  `config.proxeus.auth`, or `mcp.authenticatedByProxy`) is what counts. TLS without basic auth encrypts the MCP
  endpoint without authenticating it.
- `verticalAutoscaler` renders only when the VPA CRDs are registered, matching `serviceMonitor`/`podMonitor`,
  instead of failing the install.

## Upgrading from chart 0.0.1

- `appVersion` is a release (`v0.3.3`) and `image.tag` defaults to it, instead of tracking `master`.
- The pod runs as 65534 with a read-only root filesystem. A sidecar you add through `extraContainers` has to
  cope with that, or override `securityContext`.
- `--web.enable-lifecycle` and the reloader sidecar are now off by default (`webLifecycle`,
  `configmapReloader.enabled`).
- `resources` defaults are higher; the old 100m/128Mi could not merge much.
- A ClusterRole/ClusterRoleBinding is created (`rbac.create`), which any `kubernetes_sd_configs` config needs and
  the chart never shipped. Turn it off when the config only uses `static_configs`.
- `hpa`, `podDisruptionBudget` and `verticalAutoscaler` now render current API versions
  (`autoscaling/v2`, `policy/v1`, `autoscaling.k8s.io/v1`); the old ones were removed in Kubernetes 1.25/1.26.
