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
                  | Log Files |  <- glob patterns, multiple paths
                  +-----+-----+
                        |                    +-------------------+
                  +-----v------+             | Pipeline Config   |
                  |  Tailer(s) |  per-pipeline tailer with      |
                  |            |  independent paths, filter rules|
                  +-----+------+             +-------------------+
                        |
                  +-----v------+
                  |   Masker   |  18 built-in PII regex patterns,
                  |            |  RE2-optimized, literal pre-filter,
                  |            |  lock-free hot path, custom patterns
                  +-----+------+
                        |
                  +-----v------+
                  |  Metrics   |  Prometheus counters/gauges,
                  |            |  custom metric extractors,
                  |            |  severity inference, HTTP /metrics
                  +-----+------+
                        |
                  +-----v------+
                  |  Sampling  |  Bucket-based sliding window,
                  |            |  O(1) insert/cleanup, anomaly
                  |            |  hysteresis, per-pipeline policy
                  +-----+------+
                        |
                   +----v----+
                   |  Buffer  |  Badger LSM-tree persistent FIFO,
                   |          |  crash-safe, at-least-once delivery
                   +----+----+
                        |
                  +-----v------+
                  |  Transport |  HTTP(S) with zstd/gzip, batching,
                  |            |  exponential backoff, circuit breaker
                  +-----+------+
                        |
                  +-----v------+
                  | Log Backend|  Loki, ES, Logstash, Vector, ...
                  +------------+

             Compliance pipelines skip sampling entirely,
             ensuring audit and regulatory logs never drop.
             Each pipeline runs in its own goroutine;
             a slow compliance pipeline cannot block
             high-throughput application logs.
```

## Quick Start

### Local Binary

```bash
# Build from source (Go 1.23+ required)
go build -o iye ./cmd/iye

# Generate a configuration interactively
./iye init
./iye init /etc/iye/config.yaml

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

### Interactive Wizard

```bash
./iye init                    # writes to iye.yaml
./iye init /path/to/file.yaml # writes to specific path
```

The wizard prompts for 13 configuration values with sensible defaults shown in brackets. Press Enter to accept. Stdlib-only -- no external dependencies.

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

### Multi-Pipeline Configuration

Different log sources can flow through independent pipelines with separate masking and sampling policies:

```yaml
pipelines:
  - name: auth-audit
    paths:
      - /var/log/auth/*.log
    compliance: true          # never drops, never samples
    masker:
      enabled: true

  - name: app-microservices
    paths:
      - /var/log/app/*.log
    sampling:
      min_sample_rate: 0.01
      error_threshold: 0.03
```

Compliance pipelines are exempt from sampling -- every audit log line is delivered. Application pipelines use adaptive sampling to reduce volume without losing signal during incidents.

### Custom Metric Extraction

Extract structured metrics from unstructured log lines using named regex patterns:

```yaml
metrics:
  enabled: true
  listen_address: ":9090"
  custom_metrics:
    - name: http_status
      pattern: "status=(\\d+)"
    - name: response_time
      pattern: "duration=([\\d.]+)ms"
```

Each match increments a dedicated Prometheus counter (`iye_custom_http_status_matches_total`) with the pattern name and source file as labels.

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

Custom metric rate:
```
rate(iye_custom_pattern_matches_total{pattern="http_status"}[1m])
```

## Features

- **Log Tailing**: Glob pattern file discovery, inode-based rotation detection (no fsnotify dependency), regex include/exclude filters, buffered reading with configurable max line size. Lines exceeding the read buffer are read fully with `ReadBytes` fallback -- no silent truncation.
- **PII Masking**: 18 built-in patterns covering AWS keys, GCP service account keys, JWT tokens, email addresses, credit card numbers, US SSNs, IPv4/IPv6 addresses, private keys (PEM), database connection strings, and more. RE2-optimized with `strings.Contains` literal pre-filtering that skips 12 of 18 patterns per line when no match is possible. Lock-free hot path via `atomic.Uint64` counters. Supports custom regex patterns and optional preserve-length masking.
- **Dynamic Sampling**: Bucket-based sliding window anomaly detection with O(1) insert and cleanup. During normal operation, the sample rate adjusts to the observed error rate via a probabilistic Bernoulli gate. When the error ratio exceeds the configured threshold, IYE enters anomaly mode and samples at 100% until the window expires or the error ratio drops below 70% of the threshold (hysteresis prevents flapping). Configurable bucket count and duration for fine-grained window control.
- **Tagged Pipelines**: Route different log sources through independent processing pipelines. Each pipeline has its own tailer, masker, and sampling controller, running in its own goroutine. Compliance-tagged pipelines skip sampling entirely -- every audit log line reaches the backend regardless of volume.
- **Custom Metric Extractors**: Define named regex patterns in the config to extract structured counters from unstructured log text. Each match auto-registers a Prometheus counter. No code changes needed to track application-specific events.
- **Configuration Wizard**: `iye init` generates a production-ready config interactively. Pure stdlib -- no survey or cobra dependencies.
- **Disk Buffer**: Badger-backed persistent FIFO queue. Provides at-least-once delivery semantics. Writes are acknowledged immediately; reads are committed only after successful transport. Crash-safe -- buffered data survives process restarts.
- **HTTP Transport**: Batched POST requests with configurable batch size and timeout. Compression via zstd or gzip. Exponential backoff with configurable maximum retries. Circuit breaker stops retrying after 10 consecutive failures to prevent buffer poisoning.
- **TLS/mTLS**: Optional client certificate authentication with configurable CA pool and `InsecureSkipVerify`.

## Performance

See [PERFORMANCE.md](PERFORMANCE.md) for detailed benchmark methodology, lock analysis, and architectural decisions.

| Component | Per-Operation Latency | Allocs | Description |
|-----------|----------------------|--------|-------------|
| Masker (dense PII) | ~46 microseconds | 39 | Single line with 4 PII types (AWS key, JWT, email, password) |
| Masker (mixed) | ~21 microseconds | 16 | 7 realistic lines, 1-2 PII hits on average, <30µs target achieved |
| Metrics | ~0.3 microseconds | 0 | Counter increments, label resolution, pattern matching |
| Sampling | ~0.07 microseconds | 0 | Bucket insert, window rotation, anomaly evaluation |
| Tailer | pool-reuse | 0 | 64KB pooled read buffer, dynamic growth for oversized lines |

Benchmarks run on 11th Gen Intel Core i5-1135G7 @ 2.40GHz, Go 1.23. All packages pass with `-race`, 0 data races.

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

Total: ~89 tests across 9 packages, all pass with `-race`, 0 data races.

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
  demo/     log-generator, receiver, prometheus, grafana (docker compose)
  docker/Dockerfile
  k8s/  daemonset.yaml, configmap.yaml, rbac.yaml, service.yaml, namespace.yaml
config.example.yaml
```

### Demo Environment

A full demo stack is available in `deployments/demo/`:

```bash
cd deployments/demo
docker compose up -d
```

Spins up 5 containers:
- **log-generator** -- produces synthetic log lines with PII, error bursts, and routine noise
- **iye** -- the squeezer itself, configured to tail the generator output
- **receiver** -- minimal HTTP endpoint that accepts and counts batches
- **prometheus** -- scrapes iye metrics
- **grafana** -- pre-provisioned dashboard showing sampling rate, masking ratio, and anomaly state

## License

MIT -- see [LICENSE](LICENSE).
