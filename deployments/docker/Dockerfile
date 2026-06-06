# ---- Build Stage ----
FROM golang:1.23-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w -X main.version=$(cat VERSION 2>/dev/null || echo dev) -X main.commit=$(git rev-parse --short HEAD 2>/dev/null || echo none) -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" -o /build/iye ./cmd/iye/

# ---- Runtime Stage ----
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="İYE"
LABEL org.opencontainers.image.description="Edge-Based Log Squeezer & Anomaly Detector"
LABEL org.opencontainers.image.version="1.0.0"

COPY --from=builder /build/iye /usr/local/bin/iye

EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/iye"]
CMD ["-config", "/etc/iye/config.yaml"]
