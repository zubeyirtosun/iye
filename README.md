# IYE

[WIP] -- This project is under active development. APIs and configuration are subject to change before the 1.0 stable release.

Edge log squeezer, PII masker, and anomaly detector. Reduces cloud log ingestion costs by filtering, masking, and selectively sampling log data before it leaves the host.

## The Log Tax Problem

Cloud logging costs scale linearly with volume. Most log data is noise -- repeated info lines, debug messages, routine health checks. Yet every line is shipped, stored, and indexed at the same price as critical errors.

Traditional approaches either ship everything (wasteful) or pre-filter at collection time with fragile grep rules (lossy). IYE sits between your application logs and your observability backend. It reads log files, strips sensitive data (PII, secrets, tokens) before they leave the host, applies dynamic sampling that increases fidelity during anomalies and drops routine noise during steady state, buffers to local disk for crash recovery, and transmits compressed batches over standard HTTP(S).

The result: 40-80% reduction in log volume without losing signal.

## Vendor-Agnostic Design

IYE transmits logs as standard HTTP POST requests with a JSON payload, optionally compressed with zstd or gzip. There is no proprietary protocol, no vendor SDK, no agent plugin required. Any HTTP-capable log receiver works:

- Grafana Loki (with push endpoint)
- Elasticsearch / OpenSearch bulk API
- Vector / Fluentd via HTTP source
- Logstash with http input plugin
- Custom HTTP endpoints

The batch payload format is straightforward:

```json
{
  "entries": [
    {
      "ts": "2026-06-07T10:00:00.123Z",
      "src": "/var/log/app/api.log",
      "msg": "user login successful",
      "lvl": "info"
    }
  ],
  "count": 1,
  "compressed": true,
  "algorithm": "zstd"
}
```

Drop IYE in front of your existing pipeline. No infrastructure changes needed.

## Architecture

```
                  +-----------+
                  | Log Files |
                  +-----+-----+
                        |
                  +-----v------+
                  |   Tailer    |  File discovery, inode tracking,
                  |             |  rotation detection, glob/regex
                  +-----+------+
                        |
                  +-----v------+
                  |   Masker    |  18 built-in PII regex patterns,
                  |             |  custom patterns, preserve-length
                  +-----+------+
                        |
                  +-----v------+
                  |   Metrics   |  Prometheus counters/gauges,
                  |             |  severity inference, HTTP /metrics
                  +-----+------+
                        |
                  +-----v------+
                  |  Sampling   |  Anomaly detection via error ratio,
                  |             |  dynamic rate, cooldown periods
                  +-----+------+
                        |
                   +----v----+
                   |  Buffer  |  Badger LSM-tree persistent FIFO,
                   |          |  crash-safe, at-least-once delivery
                   +----+----+
                        |
                  +-----v------+
                  |  Transport  |  HTTP(S) with zstd/gzip, batching,
                  |             |  exponential backoff, circuit breaker
                  +-----+------+
                        |
                  +-----v------+
                  |  Log Backend |  Loki, ES, Logstash, Vector, ...
                  +-------------+
```

## Quick Start

### Local Binary

```bash
# Build from source (Go 1.23+ required)
go build -o iye ./cmd/iye

# Run with default configuration (reads /var/log/pods/**/*.log)
./iye

# Run with custom config file
./iye -c /etc/iye/config.yaml

# Show version information
./iye -version

# Run with different log level
./iye -log-level debug
```

### Docker

```bash
docker build -t iye:latest -f deployments/docker/Dockerfile .
docker run -v /var/log:/var/log:ro iye:latest
```

The Docker image is multi-stage, based on `gcr.io/distroless/static-debian12`, approximately 10MB.

### Kubernetes (DaemonSet)

```bash
kubectl apply -f deployments/k8s/namespace.yaml
kubectl apply -f deployments/k8s/configmap.yaml
kubectl create configmap iye-config --from-file=config.yaml=/etc/iye/config.yaml
kubectl apply -f deployments/k8s/rbac.yaml
kubectl apply -f deployments/k8s/daemonset.yaml
kubectl apply -f deployments/k8s/service.yaml
```

The DaemonSet runs IYE on every node, mounts the host filesystem log directories read-only, and exposes a Prometheus metrics endpoint on port 9090.

## Configuration

See [config.example.yaml](config.example.yaml) for all available options with defaults.

### Minimum Production Configuration

```yaml
tailer:
  paths:
    - /var/log/app/*.log

masker:
  enabled: true

buffer:
  enabled: true
  path: /data/iye/buffer
  max_size_bytes: 536870912

transport:
  enabled: true
  endpoint: https://loki.example.com/loki/api/v1/push
  compression: zstd
  batch_size: 2000
  batch_timeout: 5s
```

## Metrics and Observability

### HTTP Endpoints

| Endpoint | Description |
|----------|-------------|
| `/metrics` | Prometheus metrics in plaintext |
| `/healthz` | Always returns 200 OK |
| `/readyz` | Returns 200 when server is initialized, 503 otherwise |

### Prometheus Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `iye_log_lines_total` | Counter | `source`, `level` | Total log lines processed through the pipeline |
| `iye_log_bytes_total` | Counter | `source` | Total bytes processed |
| `iye_log_lines_masked_total` | Counter | `source` | Lines where at least one PII pattern was matched and replaced |
| `iye_log_lines_sampled_total` | Counter | `source`, `mode` | Lines selected for output (mode: normal or anomaly) |
| `iye_log_lines_dropped_total` | Counter | `source` | Lines dropped by sampling |
| `iye_log_processing_duration_seconds` | Histogram | `stage` | Processing latency per pipeline stage |
| `iye_anomaly_events_total` | Counter | `type`, `source` | Anomaly state transitions (anomaly_started, anomaly_ended) |
| `iye_current_sampling_mode` | Gauge | `source` | Current sampling state: 0 = normal, 1 = anomaly |
| `iye_buffer_size_bytes` | Gauge | `buffer` | Current disk buffer utilization |
| `iye_buffer_dropped_total` | Counter | `buffer` | Lines dropped from buffer (circuit breaker) |
| `iye_custom_pattern_matches_total` | Counter | `pattern`, `source` | Matches for user-defined custom metric extraction patterns |

### Key Queries

Rate of log processing:
```
rate(iye_log_lines_total[1m])
```

Log reduction rate (sampling effectiveness):
```
1 - rate(iye_log_lines_sampled_total[1m]) / rate(iye_log_lines_total[1m])
```

Anomaly active:
```
iye_current_sampling_mode > 0
```

## Features

- **Log Tailing**: Glob pattern file discovery, inode-based rotation detection (no fsnotify dependency), regex include/exclude filters, buffered reading with configurable max line size.
- **PII Masking**: 18 built-in patterns covering AWS keys, GCP service account keys, Azure connection strings, JWT tokens, email addresses, credit card numbers, US SSNs, IPv4/IPv6 addresses, private keys (PEM), database connection strings, GitHub tokens, Slack tokens, npm/Gem tokens, SSH keys, and more. Supports custom regex patterns and optional preserve-length masking.
- **Dynamic Sampling**: Event-driven anomaly detection based on error-to-total ratio within a sliding window. During normal operation, the sample rate adjusts to the observed error rate. When the error ratio exceeds the configured threshold, IYE enters anomaly mode and samples at 100% until the window expires or the error ratio drops.
- **Disk Buffer**: Badger-backed persistent FIFO queue. Provides at-least-once delivery semantics. Writes are acknowledged immediately; reads are committed only after successful transport. Crash-safe -- buffered data survives process restarts.
- **HTTP Transport**: Batched POST requests with configurable batch size and timeout. Compression via zstd or gzip. Exponential backoff with configurable maximum retries. Circuit breaker stops retrying after 10 consecutive failures to prevent buffer poisoning.
- **TLS/mTLS**: Optional client certificate authentication with configurable CA pool and `InsecureSkipVerify`.

## Performance

See [PERFORMANCE.md](PERFORMANCE.md) for detailed benchmark methodology, lock analysis, and architectural decisions.

| Component | Per-Operation Latency | Description |
|-----------|----------------------|-------------|
| Masker | ~72 microseconds | 18 regex patterns on log line with PII content |
| Metrics | ~25 microseconds | Prometheus counter increments, field extraction, label assignment |
| Sampling | ~9 microseconds | Event queue push, cleanup, anomaly evaluation |

## Requirements

- Go 1.23 or later
- Linux kernel (inode-based file rotation detection)
- Approximately 15MB RAM idle, 30MB under typical load
- Approximately 100MB disk for buffer (configurable up to available space)
- Outbound HTTP connectivity to log backend (if transport enabled)

## Development

```bash
# Run all tests
go test ./... -count=1 -timeout 60s

# Run with race detector
go test ./... -race -count=1 -timeout 120s

# Coverage
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out

# Static analysis
go vet ./...
```

### Test Coverage

| Package | Coverage |
|---------|----------|
| Sampling controller | 92.4% |
| Masker | 90.7% |
| Buffer | 89.3% |
| Metrics collector | 83.6% |
| Config | 77.4% |
| Models | 78.4% |
| Transport | 69.4% |
| Tailer | 65.5% |

### Project Structure

```
cmd/iye/main.go                     -- Entry point, pipeline wiring, signal handling
internal/
  tailer/   tailer.go, inode_linux.go
  masker/   masker.go
  metrics/  collector.go
  sampling/ controller.go
  buffer/   buffer.go
  transport/ transport.go
  config/   config.go
pkg/models/config.go
deployments/
  docker/Dockerfile
  k8s/  daemonset.yaml, configmap.yaml, rbac.yaml, service.yaml, namespace.yaml
config.example.yaml
```

## License

MIT -- see [LICENSE](LICENSE).
