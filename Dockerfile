# syntax=docker/dockerfile:1

# ── Stage 1: Build Go binaries ───────────────────────────────────────────────
# The Mantine UI assets are pre-built and committed under pkg/mantineui/static
# (see pkg/mantineui/embed.go); there is no node stage here.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine3.21 AS builder

ARG BUILDPLATFORM
ARG TARGETARCH
ARG TARGETOS
ENV GOARCH=${TARGETARCH} GOOS=${TARGETOS}

WORKDIR /go/src/github.com/pvlltvk/proxeus

COPY go.mod go.sum ./
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build \
    cd cmd/proxeus && \
    CGO_ENABLED=0 go build -tags netgo \
      -ldflags="-s -w"

RUN --mount=type=cache,target=/root/.cache/go-build \
    cd cmd/remote_write_exporter && \
    CGO_ENABLED=0 go build \
      -ldflags="-s -w"

# ── Final image ───────────────────────────────────────────────────────────────
# Binaries are fully static (CGO_ENABLED=0, netgo); scratch is viable.
# /etc/passwd is inlined so USER nobody resolves at runtime.
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /go/src/github.com/pvlltvk/proxeus/cmd/proxeus/proxeus               /bin/proxeus
COPY --from=builder /go/src/github.com/pvlltvk/proxeus/cmd/remote_write_exporter/remote_write_exporter /bin/remote_write_exporter

# Minimal /etc/passwd so nobody (uid 65534) resolves inside scratch.
COPY --from=builder /etc/passwd /etc/passwd

LABEL org.opencontainers.image.authors="Pavel Litvyak <pvlltvk@gmail.com>"
LABEL org.opencontainers.image.source="https://github.com/pvlltvk/proxeus"
LABEL org.opencontainers.image.licenses="MIT"
EXPOSE 8082

USER nobody

ENTRYPOINT ["/bin/proxeus"]
