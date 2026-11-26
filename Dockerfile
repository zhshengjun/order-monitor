# syntax=docker/dockerfile:1.7
# Cross-compile both target architectures on the native builder instead of
# running the Go compiler for arm64 under QEMU.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG BUILDOS
ARG BUILDARCH
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-${BUILDOS:-linux}} GOARCH=${TARGETARCH:-${BUILDARCH:-amd64}} \
    go build -trimpath -ldflags='-s -w' -o /out/order-monitor ./cmd/order-monitor

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl util-linux && rm -rf /var/lib/apt/lists/* && groupadd --gid 10001 monitor && useradd --uid 10001 --gid 10001 --create-home monitor && mkdir /data && chown monitor:monitor /data
ENV HOST=0.0.0.0 PORT=8080 DATABASE_PATH=/data/monitor.db
WORKDIR /app
COPY --from=build /out/order-monitor /app/order-monitor
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod 0755 /app/docker-entrypoint.sh
ENTRYPOINT ["/app/docker-entrypoint.sh"]
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1
CMD ["/app/order-monitor"]
