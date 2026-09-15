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

| key | default | notes |
|---|---|---|
| `replicaCount` | `1` | proxeus is stateless unless `storage.persistence` is on, so scale freely |
| `image.repository` | `ghcr.io/pvlltvk/proxeus` | `image.tag` empty means the chart's `appVersion`; `image.digest` pins by content |
| `service` | ClusterIP on 8082 | |
| `ingress` | disabled | authenticate proxeus (see below) before exposing it |
| `networkPolicy` | disabled | ingress to 8082 only, from the peers you list |
| `serviceMonitor` / `podMonitor` | disabled | need the Prometheus Operator CRDs; skipped silently when absent |
| `hpa` / `verticalAutoscaler` | disabled | an actuating VPA `updateMode` together with the HPA is refused |
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
  `webConfig.existingSecret`; the chart mounts it at `/etc/proxeus-web/` and passes the flag.
- `config.proxeus.auth.basic.users` holds bcrypt hashes inline. Those go through the ConfigMap like the rest of
  the config; if you would rather not have them in a ConfigMap at all, render the whole config into a Secret
  yourself — but note that `configMap: <name>` expects a ConfigMap, so this needs `extraVolumes`/
  `extraVolumeMounts` over `/etc/proxeus` today.
- Environment variables the Prometheus SD libraries read themselves (cloud credentials, `HTTP_PROXY`) go in
  `env` / `envFrom`.

Note that `/metrics`, `/-/healthy` and `/-/ready` are exempt from authentication by default, so probes and
ServiceMonitors keep working with `config.proxeus.auth` on. Override `auth.exempt_paths` and you own that.

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

## Upgrading from chart 0.0.1

- `appVersion` is a release (`v0.2.0`) and `image.tag` defaults to it, instead of tracking `master`.
- The pod runs as 65534 with a read-only root filesystem. A sidecar you add through `extraContainers` has to
  cope with that, or override `securityContext`.
- `--web.enable-lifecycle` and the reloader sidecar are now off by default (`webLifecycle`,
  `configmapReloader.enabled`).
- `resources` defaults are higher; the old 100m/128Mi could not merge much.
- A ClusterRole/ClusterRoleBinding is created (`rbac.create`), which the default `kubernetes_sd_configs` config
  always needed and the chart never shipped.
- `hpa`, `podDisruptionBudget` and `verticalAutoscaler` now render current API versions
  (`autoscaling/v2`, `policy/v1`, `autoscaling.k8s.io/v1`); the old ones were removed in Kubernetes 1.25/1.26.
