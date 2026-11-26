# syntax=docker/dockerfile:1.7
# Local/compose build from source. CI publishes with Dockerfile.prebuilt.
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

FROM alpine:3.20
RUN addgroup -g 10001 monitor && adduser -D -u 10001 -G monitor monitor && mkdir -p /data /etc/ssl/certs && chown monitor:monitor /data
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENV HOST=0.0.0.0 PORT=8080 DATABASE_PATH=/data/monitor.db DROP_PRIVILEGES=1
WORKDIR /app
COPY --from=build /out/order-monitor /app/order-monitor
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod 0755 /app/order-monitor /app/docker-entrypoint.sh
ENTRYPOINT ["/app/docker-entrypoint.sh"]
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
CMD ["/app/order-monitor"]
