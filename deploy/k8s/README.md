# k8s

Proxeus authenticates incoming requests itself when the config carries a `proxeus.auth` block (basic, OIDC or a
trusted header from an authenticating proxy). Without that block every request is anonymous, so either configure
it or put something in front:

1. k8s' ingress -- https://kubernetes.github.io/ingress-nginx/examples/auth/basic/
2. setting up nginx (or another proxy) as the auth endpoint -- https://prometheus.io/docs/guides/basic-auth/

[`proxeus.yaml`](proxeus.yaml) is a minimal manifest set; [`helm-charts/proxeus/`](helm-charts/proxeus/) is the
chart to deploy from.
