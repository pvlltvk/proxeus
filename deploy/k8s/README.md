# k8s

Proxeus doesn't provide any auth mechanisms today. If you require auth in your setup I recommend using either:

1. k8s' ingress -- https://kubernetes.github.io/ingress-nginx/examples/auth/basic/
2. setting up nginx (or another proxy) as the auth endpoint -- https://prometheus.io/docs/guides/basic-auth/
