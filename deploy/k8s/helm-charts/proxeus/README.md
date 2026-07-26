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
| `replicaCount` | `1` | proxeus is stateless, so scale freely |
| `image.repository` | `ghcr.io/pvlltvk/proxeus` | |
| `service` | ClusterIP on 8082 | |
| `ingress` | disabled | proxeus has **no built-in auth** — see [../../README.md](../../README.md) |
| `hpa` / `verticalAutoscaler` | disabled | |
| `podDisruptionBudget` | | |
| `serviceAccount` | created | |
| `extraArgs` | `[]` | extra CLI flags, e.g. `--log-level=debug` |
| `configmapReloader` | enabled | sidecar that hits `/-/reload` when the ConfigMap changes |

See [`values.yaml`](values.yaml) for the full set.

## Notes

Recording rules need a `remote_write` target in the config (proxeus has no local TSDB) plus a writable
`--storage.path` for the WAL — add the latter via `extraArgs` and mount a volume for it.
