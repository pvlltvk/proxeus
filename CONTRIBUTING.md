# Contributing

## Setup

```
git clone git@github.com:pvlltvk/proxeus.git
cd proxeus
go build -tags netgo ./cmd/proxeus
```

## Before opening a PR

```
make fmt && make imports   # must leave no diff
make lint                  # golangci-lint, config in .golangci.yml
make test                  # go test -race ./...
```

These are the same checks CI runs (`.github/workflows/go.yml`). A PR won't merge until they're green.

## PR process

- Branch off `main`, open a PR against `main`.
- Keep commit subjects short (subject line, or subject + one terse line — no multi-bullet bodies).
- `main` requires a PR; direct pushes aren't accepted.

## Releasing

Maintainer-only: pushing a `v*` tag triggers `.github/workflows/release.yml`, which runs `make release` (cross-platform binaries + `SHA256SUMS`) and publishes a GitHub Release. `.github/workflows/build.yml` builds and pushes the multi-arch Docker image to `ghcr.io/pvlltvk/proxeus` for the same tag.
