# İYE MASTER DOCUMENTATION & REFERENCE MANUAL

**Version:** 0.1.0-dev  
**Last Updated:** 2026-06-08  
**Maintainer:** İYE Core Team  
**Repository:** https://github.com/zubeyirtosun/iye

---

## Table of Contents

1. [Architecture & Pipeline Model](#1--architecture--pipeline-model)
   - [1.1 The Six-Stage Data Flow](#11-the-six-stage-data-flow)
   - [1.2 Non-Destructive Compliance Gateway](#12-non-destructive-compliance-gateway)
   - [1.3 High-Performance Lifecycle](#13-high-performance-lifecycle)
2. [Setup & Quickstart Guides](#2--setup--quickstart-guides)
   - [2.1 Interactive CLI Setup Wizard](#21-interactive-cli-setup-wizard)
   - [2.2 Local Binary Operations](#22-local-binary-operations)
   - [2.3 Containerized & Orchestrated Deployments](#23-containerized--orchestrated-deployments)
3. [Complete Configuration Reference](#3--complete-configuration-reference)
   - [3.1 Global Configuration](#31-global-configuration)
   - [3.2 Tag-Based Multi-Pipeline Specs](#32-tag-based-multi-pipeline-specs)
   - [3.3 Tailer Configuration](#33-tailer-configuration)
   - [3.4 Masker Configuration](#34-masker-configuration)
   - [3.5 Sampling Configuration](#35-sampling-configuration)
   - [3.6 Metrics Configuration](#36-metrics-configuration)
   - [3.7 Buffer Configuration](#37-buffer-configuration)
   - [3.8 Secure Ingestion & Transport Blueprint](#38-secure-ingestion--transport-blueprint)
   - [3.9 Custom Telemetry Extractors](#39-custom-telemetry-extractors)
4. [Integrations & Protocol Bindings](#4--integrations--protocol-bindings)
   - [4.1 Vendor-Agnostic Transport Layer](#41-vendor-agnostic-transport-layer)
   - [4.2 Grafana Loki](#42-grafana-loki)
   - [4.3 Elasticsearch / OpenSearch](#43-elasticsearch--opensearch)
   - [4.4 Logstash / Apache Kafka / Vector](#44-logstash--apache-kafka--vector)
   - [4.5 Custom Webhooks / Internal SIEM](#45-custom-webhooks--internal-siem)
5. [FAQ & Real-World Troubleshooting](#5--faq--real-world-troubleshooting)
   - [5.1 Data Integrity](#51-data-integrity)
   - [5.2 Resilience](#52-resilience)
   - [5.3 Masking Accuracy](#53-masking-accuracy)
   - [5.4 Performance Characteristics](#54-performance-characteristics)
   - [5.5 Operational Concerns](#55-operational-concerns)
6. [Appendix: Prometheus Metrics Reference](#6-appendix-prometheus-metrics-reference)
7. [Appendix: Performance Benchmarks](#7-appendix-performance-benchmarks)

---

## 1. ARCHITECTURE & PIPELINE MODEL

### 1.1 The Six-Stage Data Flow

İYE implements a strictly linear, six-stage processing pipeline. Every log line that enters the system passes through each stage in order. There are no branching paths, no concurrent mutations, and no out-of-order processing within a single pipeline. Each stage is independently goroutine-safe and communicates with the next via buffered channels.

```
                              +--------------------------------------------------+
                              |                CONFIGURATION LAYER               |
                              |  YAML -> models.Config -> per-component configs  |
                              +--------------------------------------------------+
                                         |          |          |
                           +-------------v----------v----------v-------------+
                           |                  PIPELINE STAGES                |
                           |                                                  |
     +----------------+    |  +----------+    +---------+    +----------+    |
     |   LOG FILES    |----+->| TAILER   |--->| MASKER  |--->| METRICS  |    |
     |  (glob paths)  |    |  |          |    |         |    |          |    |
     +----------------+    |  | - inode  |    | - 18    |    | - Prom   |    |
                           |  |   track  |    |   RE2   |    |   count  |    |
                           |  | - rotat  |    |   patts |    | - custom |    |
                           |  |   detect |    | - cust  |    |   extra  |    |
                           |  | - regex  |    |   patts |    | - sever  |    |
                           |  |   filter |    | - lk-fr |    |   infer  |    |
                           |  +----------+    +---------+    +----------+    |
                           |       |               |               |         |
                           |       v               v               v         |
                           |  +----------+    +---------+    +----------+    |
                           |  | SAMPLING |    | BUFFER  |    |TRANSPORT |    |
                           |  |          |    | (Badger |    |          |    |
                           |  | - bucket |    |  LSM)   |    | - HTTP   |    |
                           |  |   window |    | - crash |    |   batch  |    |
                           |  | - anomal |    |   safe  |    | - compr  |    |
                           |  |   detect |    | - FIFO  |    | - backof |    |
                           |  | - hyster |    |   queue |    | - circ   |    |
                           |  +----------+    +---------+    +----------+    |
                           |                                                  |
                           +--------------------------------------------------+
                                         |
                                         v
                              +-------------------------+
                              |   LOG BACKEND          |
                              | (Loki / ES / Vector /  |
                              |  Custom HTTP endpoint) |
                              +-------------------------+

  COMPLIANCE PIPELINES (parallel, isolated goroutines):
       +----------+    +---------+    +----------+    +----------+
       | TAILER   |--->| MASKER  |--->| METRICS  |--->| BUFFER  |---> TRANSPORT
       | (audit   |    | (on)    |    | (count)  |    | (always |    (always)
       |  paths)  |    |         |    |          |    |  write) |
       +----------+    +---------+    +----------+    +----------+
       SAMPLING: DISABLED (compliance: true -> Enabled: false -> 100% pass)
```

Each pipeline instance contains its own Tailer, Masker, and SamplingController instances. Metrics, Buffer, and Transport are shared across pipelines. This isolation means a compliance pipeline processing large volume audit logs cannot starve or delay a real-time application pipeline.

**Stage Responsibilities:**

| Stage | Input | Output | Guarantee |
|-------|-------|--------|-----------|
| **Tailer** | File descriptors (rotated, truncated, appended) | `*models.LogLine` on output channel | At-most-once per poll cycle (lines may be skipped between polls) |
| **Masker** | `*models.LogLine` with raw `Content` | Same struct with `Content` rewritten, `Masked=true` if PII found | Best-effort (pattern coverage, not cryptographic) |
| **Metrics** | `*models.LogLine` with `Masked` and `Sampled` flags | Prometheus counter/histogram increments | At-least-once per line (idempotent) |
| **Sampling** | Severity, source, content per event | `ShouldSample() bool` decision | Probabilistic (Bernoulli trial per line) |
| **Buffer** | Serialized `LogEntry` structs | On-disk Badger LSM-tree entries | At-least-once (fsync on commit, not on write) |
| **Transport** | Batched `LogEntry` arrays | HTTP(S) POST to remote endpoint | At-least-once with retry, circuit breaker at 10 consecutive failures |

### 1.2 Non-Destructive Compliance Gateway

İYE satisfies legal compliance frameworks (KVKK, GDPR, Turkey Law 5651 on auditing) through its pipeline isolation model.

#### How Compliance Works

A pipeline tagged with `compliance: true` bypasses the sampling stage entirely. The `SamplingController` is created with `Enabled: false`, which causes `ShouldSample()` to unconditionally return `true`. Every line entering a compliance pipeline is:

1. Read by the tailer
2. Masked (if masker is enabled for that pipeline)
3. Counted by metrics
4. Written to the disk buffer
5. Transported to the backend

No lines are dropped, no probabilistic decisions are made. The `DropLine` codepath is never invoked for compliance sources.

#### Practical Implications

| Log Type | Pipeline Tag | Behavior | Legal Impact |
|----------|-------------|----------|--------------|
| Auth/audit logs (`/var/log/auth/*`) | `compliance: true` | 100% delivery, PII masked | KVKK Article 12, GDPR Art. 5(1)(e), 5651 md.5 satisfied |
| Application debug logs | (default) | Sampling drops 90-99% of steady-state noise | Not legally retained; metric summaries suffice |
| Error/crash logs | (default) | Anomaly detection escalates to 100% sampling | Errors are preserved during incidents |
| Financial transaction logs | `compliance: true` | 100% delivery, credit card numbers masked | PCI-DSS requirement 3.4 satisfied via `preserve_length: true` |

#### The Metrics Contract

Even when İYE drops a routine log line (sampling `ShouldSample() == false`), the **metrics are always updated**. The pipeline calls `metricsCollector.ProcessLine()` and `metricsCollector.DropLine()` before discarding the line. This means:

- `iye_log_lines_total` counts every line that enters the pipeline
- `iye_log_lines_dropped_total` counts every line that was sampled out
- The ratio `dropped / total` is observable in real-time via Prometheus
- `iye_log_lines_sampled_total` counts lines actually shipped

For compliance pipelines, `dropped` is always zero.

### 1.3 High-Performance Lifecycle

İYE targets <15MB RAM at idle and <30MB under typical load. Every architectural decision is traced to this constraint.

#### Memory Model: sync.Pool

Two `sync.Pool` instances eliminate heap churn on the hot path:

1. **`lineBufPool`** (`internal/tailer/tailer.go`): Pools 64KB byte slices used to copy line content from the `bufio.Reader` internal buffer. Lines under 64KB are served from the pool with zero allocation. Lines exceeding 64KB trigger a one-off `make([]byte, len(line))` that is garbage-collected after the `LogLine` is consumed by the transport layer.

2. **`logEntryPool`** (`cmd/iye/main.go`): Pools `*models.LogEntry` structs (the serializable JSON representation) with pre-allocated `Labels` maps. After `buf.Write(entry)` the entry is returned to the pool via `Put()`.

The pool lifecycle:

```
Line arrives at tailer
    |
    v
lineBufPool.Get() -> reuse 64KB slice (if available) or allocate
    |
    v
Copy line into slice (or allocate fresh if >64KB)
    |
    v
Wrap in *models.LogLine, send to output channel
    |
    v
Pipeline processes, writes to buffer (consumes Raw bytes)
    |
    v
buffer.Write(entry) -> serializes to JSON, fsyncs to Badger
    |
    v
logEntryPool.Put(entry) -> return to pool (Labels map cleared)
```

#### Lock Elimination

| Hot Path | Before | After | Mechanism |
|----------|--------|-------|-----------|
| Masker stats (`LinesProcessed`, `PatternsMatched`, `BytesMasked`) | `sync.RWMutex.RLock()` on every call | `atomic.Uint64` with `.Add()` / `.Load()` | Separate package-level atomics, zero contention |
| Sampling controller event counters | `sync.Mutex` over full bucket rotation | `sync.Mutex` over <1µs bucket increment + 12-element sum | Critical section minimized; contention window is sub-microsecond |
| Pipeline `Sampled` flag write after sampling decision | No lock (single-writer guarantee per line) | No lock | Per-line processing is single-goroutine; no concurrent access to the same `*models.LogLine` |

#### Disk Buffer Guarantees

The buffer uses **Badger** (an LSM-tree embedded key-value store) as a persistent FIFO queue:

- **Write path:** `buf.Write(entry)` appends to the head of the queue. The write is acknowledged immediately after the in-memory write-ahead log (WAL) entry is committed. There is no fsync on every write — Badger batches fsyncs at configurable intervals (`badger.defaultWriteBatchSize`).
- **Read path:** The transport layer reads from the tail of the queue. A read is committed (deleted) only after the HTTP POST succeeds with a 2xx response. If the POST fails, the entry remains in the queue for the next retry.
- **Crash recovery:** On restart, Badger replays the WAL to restore the queue to its pre-crash state. Any entries written but not yet transported are preserved.
- **Circuit breaker:** After 10 consecutive transport failures, the circuit breaker trips. The transport goroutine stops retrying and logs a fatal error. This prevents buffer poisoning (retrying against a dead endpoint indefinitely, filling disk).

#### Execution Loop

The main processing loop is a single `select` statement per pipeline goroutine:

```go
for {
    select {
    case <-ctx.Done():
        return
    case line := <-w.tailer.Output():
        processLogLine(line, w.masker, w.metrics, w.sampler, w.buffer, logger)
    case err := <-w.tailer.Errors():
        logger.Error("Tailer error", ...)
    }
}
```

No epoll, no async I/O, no reactor pattern. Go's goroutine scheduler and buffered channels (channel size: 10,000 lines) provide backpressure: if the pipeline cannot keep up, the tailer's output channel blocks, which backs up file reads, which is absorbed by the `bufio.Reader`'s internal buffer. If that overflows, the tailer drops lines (logged as `TruncatedLines`).

---

## 2. SETUP & QUICKSTART GUIDES

### 2.1 Interactive CLI Setup Wizard

İYE ships with a pure-stdlib interactive configuration wizard accessible via the `init` subcommand. It requires no external libraries (no `cobra`, no `survey`, no `promptui`) — only `bufio.Scanner`, `fmt`, and `os`.

#### Invocation

```bash
# Writes to ./iye.yaml by default
./iye init

# Writes to a specific path
./iye init /etc/iye/config.yaml

# Writes to a path in the current directory
./iye init ./production.yaml
```

#### Wizard Flow

```
$ ./iye init /etc/iye/config.yaml

İYE Log Squeezer & Anomaly Detector — Configuration Wizard
==========================================================
Press Enter to accept defaults shown in [brackets].

  Log file paths (comma-separated)         [/var/log/pods/**/*.log]:
  Poll interval (ms)                       [100]:
  Max line size (KB)                       [1024]:
  Read buffer size (KB)                    [64]:
  Enable PII masking                       [yes]:
  Mask replacement string                  [[MASKED]]:
  Enable Prometheus metrics                [yes]:
  Metrics listen address                   [:9090]:
  Enable adaptive sampling                 [yes]:
  Minimum sample rate (0.0-1.0)            [0.01]:
  Maximum sample rate (0.0-1.0)            [1]:
  Enable remote transport                  [no]:
  Remote endpoint URL                      []:
  Batch size                               [1000]:

Configuration written to /etc/iye/config.yaml
```

#### Prompt Semantics

- Pressing Enter with no input accepts the default shown in `[brackets]`.
- Entering a value overrides the default.
- Entering partial input (e.g., a comma-separated path list) is parsed verbatim.
- Numeric fields default to safe production values on invalid input (e.g., entering "abc" for port falls back to `:9090`).
- If transport is disabled, the buffer is also disabled (no point writing to disk if nothing reads it).

#### Output Validation

After the wizard writes the file, the config is validated via `cfg.Validate()`:

```yaml
# Fields validated:
# - At least one tailer path required
# - Poll interval > 0
# - Error threshold 0.0-1.0
# - Min/Max sample rate 0.0-1.0, min <= max
# - Metrics listen_address non-empty
# - Metrics path non-empty
# - Buffer path non-empty (if enabled)
# - Transport endpoint non-empty (if enabled)
# - Pipeline names non-empty (if defined)
# - Pipeline paths non-empty (if defined)
# - Pipeline sampling configs validated if present
```

If validation fails, the wizard prints the error to stderr and exits with code 1. No file is written.

### 2.2 Local Binary Operations

#### Prerequisites

```bash
# Go 1.23 or later
go version  # go version go1.23.0 linux/amd64

# Linux kernel 4.18+ (inode-based file rotation detection)
uname -r    # 5.15.0-xx-generic
```

#### Build

```bash
# Production build (stripped, no DWARF, no symbol table)
go build -ldflags="-s -w" -o iye ./cmd/iye

# Development build with version metadata
go build -ldflags="\
  -X main.version=$(git describe --tags 2>/dev/null || echo dev) \
  -X main.commit=$(git rev-parse --short HEAD) \
  -X main.buildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  -o iye ./cmd/iye
```

#### Flags

```
-c, -config string    Path to configuration file (YAML)
-v, -version          Show version information and exit
-log-level string     Override log level (debug, info, warn, error)
```

Config file search order (when `-c` is not specified):

1. `/etc/iye/config.yaml`
2. `/etc/iye/config.yml`
3. `./config.yaml`
4. `./config.yml`
5. `$HOME/.iye/config.yaml`
6. If none found, `DefaultConfig()` is used (reads `/var/log/pods/**/*.log`)

#### Environment Variables

The binary itself does not read environment variables for configuration. All configuration is file-based. However, container orchestration platforms may inject environment for log-level or config path overrides via the command line.

#### Signals

| Signal | Behavior |
|--------|----------|
| `SIGINT` (Ctrl+C) | Graceful shutdown: stop tailer, flush buffer, stop transport, wait for goroutines |
| `SIGTERM` | Same as SIGINT |
| `SIGHUP` | Ignored (config reload not yet implemented) |

#### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Normal shutdown |
| 1 | Configuration error, initialization failure |
| 2 | Transport circuit breaker triggered |
| >0 | `init` wizard validation failure |

### 2.3 Containerized & Orchestrated Deployments

#### Multi-Stage Docker Build

```dockerfile
# Stage 1: Build
FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /iye ./cmd/iye

# Stage 2: Runtime
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /iye /iye
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 9090
USER nonroot:nonroot
ENTRYPOINT ["/iye"]
```

Build:

```bash
docker build -t iye:latest -f deployments/docker/Dockerfile .
```

The image is approximately 10MB compressed, ~25MB on disk. It contains only the static binary, CA certificates (for TLS transport), and the nonroot user.

#### Docker Run

```bash
# Minimal: mount logs, expose metrics
docker run -d \
  --name iye \
  -v /var/log:/var/log:ro \
  -p 9090:9090 \
  iye:latest

# With custom config and persistent buffer
docker run -d \
  --name iye \
  -v /var/log:/var/log:ro \
  -v /data/iye/config.yaml:/etc/iye/config.yaml:ro \
  -v /data/iye/buffer:/var/lib/iye/buffer \
  -p 9090:9090 \
  iye:latest -c /etc/iye/config.yaml
```

#### Kubernetes DaemonSet (Complete Manifest)

**Namespace:**

```yaml
# deployments/k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: iye
```

**ConfigMap:**

```yaml
# deployments/k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: iye-config
  namespace: iye
data:
  config.yaml: |
    tailer:
      paths:
        - /var/log/pods/**/*.log
      poll_interval: 100ms
      max_line_size: 1048576
      read_buffer_size: 65536
    masker:
      enabled: true
      mask_replacement: "[MASKED]"
    sampling:
      enabled: true
      error_threshold: 0.05
      window_size: 60s
      window_buckets: 6
      bucket_duration: 10s
      cooldown_period: 5m
      min_sample_rate: 0.01
      max_sample_rate: 1.0
    metrics:
      enabled: true
      listen_address: ":9090"
      metrics_path: "/metrics"
      scrape_interval: 15s
    buffer:
      enabled: true
      path: /var/lib/iye/buffer
      max_size_bytes: 536870912
    transport:
      enabled: true
      type: http
      endpoint: http://loki-gateway.loki.svc.cluster.local/loki/api/v1/push
      compression: zstd
      batch_size: 2000
      batch_timeout: 5s
    log_level: info
```

**RBAC:**

```yaml
# deployments/k8s/rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: iye
  namespace: iye
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: iye
rules:
  - apiGroups: [""]
    resources: ["nodes", "nodes/proxy"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: iye
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: iye
subjects:
  - kind: ServiceAccount
    name: iye
    namespace: iye
```

**DaemonSet:**

```yaml
# deployments/k8s/daemonset.yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: iye
  namespace: iye
  labels:
    app: iye
spec:
  selector:
    matchLabels:
      app: iye
  template:
    metadata:
      labels:
        app: iye
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: iye
      hostPID: true
      containers:
        - name: iye
          image: iye:latest
          args:
            - -c
            - /etc/iye/config.yaml
          ports:
            - containerPort: 9090
              name: metrics
          volumeMounts:
            - name: config
              mountPath: /etc/iye/config.yaml
              subPath: config.yaml
              readOnly: true
            - name: varlog
              mountPath: /var/log
              readOnly: true
            - name: buffer
              mountPath: /var/lib/iye/buffer
          resources:
            requests:
              memory: "32Mi"
              cpu: "50m"
            limits:
              memory: "128Mi"
              cpu: "200m"
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
      volumes:
        - name: config
          configMap:
            name: iye-config
        - name: varlog
          hostPath:
            path: /var/log
            type: Directory
        - name: buffer
          hostPath:
            path: /var/lib/iye/buffer
            type: DirectoryOrCreate
      tolerations:
        - operator: Exists
```

**Service:**

```yaml
# deployments/k8s/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: iye
  namespace: iye
  labels:
    app: iye
spec:
  clusterIP: None
  selector:
    app: iye
  ports:
    - name: metrics
      port: 9090
      targetPort: 9090
```

#### Deploy

```bash
kubectl apply -f deployments/k8s/namespace.yaml
kubectl apply -f deployments/k8s/configmap.yaml
kubectl create configmap iye-config --from-file=config.yaml=./my-config.yaml
kubectl apply -f deployments/k8s/rbac.yaml
kubectl apply -f deployments/k8s/daemonset.yaml
kubectl apply -f deployments/k8s/service.yaml
```

#### Demo Environment (Docker Compose)

A full demo environment is available at `deployments/demo/`:

```bash
cd deployments/demo
docker compose up -d
```

This spins up 5 containers:

| Container | Image | Role |
|-----------|-------|------|
| `log-generator` | Go binary (custom) | Produces 50 lines/sec with mixed PII content, periodic error bursts |
| `iye` | Local build | Tails the generator's log file, masks, samples, buffers, transports |
| `receiver` | Go binary (custom) | Accepts POST batches, counts entries, returns 200 |
| `prometheus` | `prom/prometheus` | Scrapes iye metrics every 5s |
| `grafana` | `grafana/grafana` | Pre-provisioned dashboard (sampling rate, masking ratio, anomaly state) |

---

## 3. COMPLETE CONFIGURATION REFERENCE

### 3.1 Global Configuration

The top-level configuration structure:

```yaml
# ============================================================
# İYE Configuration — Full Reference
# ============================================================

# Tailer configuration (required)
tailer:
  # ... see §3.3

# Masker configuration (required)
masker:
  # ... see §3.4

# Sampling configuration (required)
sampling:
  # ... see §3.5

# Metrics configuration (required)
metrics:
  # ... see §3.6

# Buffer configuration (optional, required if transport enabled)
buffer:
  # ... see §3.7

# Transport configuration (optional)
transport:
  # ... see §3.8

# Pipelines — multi-pipeline isolation (optional)
pipelines:
  # ... see §3.2

# Global log level (overridden by -log-level flag)
# Allowed: debug, info, warn, warning, error
# Default: info
log_level: info
```

### 3.2 Tag-Based Multi-Pipeline Specs

Pipelines allow independent log streams to be processed with different policies. Each pipeline is a complete processing unit with its own Tailer, Masker, and SamplingController.

```yaml
pipelines:
  # Example 1: Compliance pipeline — no data loss, full masking
  - name: auth-audit
    # Glob paths for this pipeline (required, at least one)
    paths:
      - /var/log/auth/*.log
      - /var/log/audit/*.log
      - /var/log/secure/*.log

    # Tags for routing and identification (optional)
    tags:
      - compliance
      - audit

    # Compliance mode: sampling disabled, all lines delivered
    # Default: false
    compliance: true

    # Override global masker config (optional, inherits global if omitted)
    masker:
      enabled: true
      mask_replacement: "[AUDIT_MASKED]"
      preserve_length: false
      custom_patterns:
        - (?i)TCKN[=:]\s*(\d{11})
        - (?i)passport[=:]\s*([A-Z0-9]{9})

    # Sampling is IGNORED when compliance: true
    # The SamplingController is created with Enabled: false

  # Example 2: Application pipeline — aggressive sampling, error detection
  - name: app-microservices
    paths:
      - /var/log/app/*.log
    tags:
      - application
      - production

    # Override global sampling config (optional)
    sampling:
      enabled: true
      error_threshold: 0.03       # Lower threshold = more sensitive anomaly
      window_buckets: 12           # 12 buckets x 5s = 60s window
      bucket_duration: 5s
      min_sample_rate: 0.005       # 0.5% during steady state
      max_sample_rate: 1.0
      cooldown_period: 10m         # Stay in anomaly mode longer

  # Example 3: Debug pipeline — no masking, full sampling
  - name: debug-infra
    paths:
      - /var/log/debug/*.log
    compliance: true               # Never drop debug lines
    masker:
      enabled: false               # No PII masking on debug output

  # Example 4: Inherit everything from global config
  - name: web-server
    paths:
      - /var/log/nginx/*.log
      - /var/log/apache2/*.log
    # No masker override -> uses global masker config
    # No sampling override -> uses global sampling config
    # compliance defaults to false
```

**Pipeline Worker Internals:**

Each pipeline creates:
1. A new `tailer.Tailer` with `TailerConfig.Paths` set to the pipeline's paths (all other tailer fields inherit from global)
2. A new `masker.Masker` — either from the pipeline's `masker` block or a clone of the global `MaskerConfig`
3. A new `sampling.SamplingController` — either `Enabled: false` (if compliance), from the pipeline's `sampling` block, or a clone of the global `SamplingConfig`

All pipelines share the same `MetricsCollector`, `DiskBuffer`, and `Transport` instances. This is by design: metrics aggregations are global (Prometheus handles label-based partitioning), and there is exactly one buffer + one transport per node.

**Pipeline Configuration Inheritance Rules:**

| Field | Pipeline Specifies? | Result |
|-------|--------------------|--------|
| `paths` | Required | Must have at least one path |
| `masker` | Omitted | Uses global `masker` config |
| `masker` | Present | Pipeline uses this masker config; global unaffected |
| `sampling` | Omitted AND `compliance: false` | Uses global `sampling` config |
| `sampling` | Present AND `compliance: false` | Pipeline uses this sampling config; global unaffected |
| `compliance: true` | Regardless of `sampling` | Sampling forced to `Enabled: false`, all lines pass |

### 3.3 Tailer Configuration

```yaml
tailer:
  # File paths to watch (glob patterns supported)
  # Required: at least one path
  # Default: ["/var/log/pods/**/*.log"]
  paths:
    - /var/log/**/*.log

  # How often to poll files for new content (Go duration)
  # Default: 100ms
  # Minimum: 10ms (shorter intervals may cause excessive I/O on large file sets)
  poll_interval: 100ms

  # Maximum line size in bytes (hard limit, lines exceeding this are read fully)
  # Default: 1048576 (1MB)
  # Minimum: 4096
  max_line_size: 1048576

  # Read buffer size for bufio.Reader
  # Default: 65536 (64KB)
  # Lines that fit within this buffer are read with ReadSlice (zero alloc).
  # Lines exceeding this trigger ReadBytes fallback (one alloc for the full line).
  read_buffer_size: 65536

  # Follow symlinks when resolving paths
  # Default: true
  follow_symlinks: true

  # Read from beginning of file on first discovery
  # Default: false (reads only new content appended after start)
  from_beginning: false

  # Regex include filter: only process lines matching this pattern
  # Default: "" (no filter, process all lines)
  # Example: "ERROR|FATAL|PANIC" to only capture error-level lines
  include_pattern: ""

  # Regex exclude filter: skip lines matching this pattern
  # Default: "" (no filter)
  # Example: "healthcheck|heartbeat" to filter out routine checks
  exclude_pattern: ""
```

**File Rotation Detection:**

İYE uses inode comparison to detect log rotation. On every poll cycle, the tailer calls `os.Stat()` on each tracked file and compares the inode. If the inode changes:

1. The old file descriptor is closed
2. The new file is opened at offset 0
3. The `bufio.Reader` is reset
4. Reading resumes from the beginning of the new file
5. The `iye_log_lines_rotated_total` counter is incremented

This mechanism is compatible with:
- `logrotate` (copytruncate and create modes)
- Docker's JSON-file logging driver (file recreation on container restart)
- Kubernetes kubelet log rotation

It does NOT require `fsnotify`, `inotify`, or any OS-specific event notification. Polling works reliably in all Linux environments including containers with read-only root filesystems.

**Buffer Growth Behavior:**

When a line exceeds `read_buffer_size` (64KB by default):

1. `bufio.Reader.ReadSlice('\n')` returns `bufio.ErrBufferFull`
2. The tailer falls back to `bufio.Reader.ReadBytes('\n')` which dynamically allocates a buffer exactly sized for the line
3. The line is copied into the pooled buffer (if it fits) or kept as a standalone allocation (if oversized)
4. A debug log is emitted: `"Line exceeds pooled buffer, dynamic allocation"` with `line_len` and `pool_cap` fields

This means no line is ever truncated. The 64KB pool threshold is an optimization boundary, not a limit.

### 3.4 Masker Configuration

```yaml
masker:
  # Enable PII masking
  # Default: true
  enabled: true

  # Custom regex patterns (in addition to 18 built-in patterns)
  # Default: []
  custom_patterns:
    - (?i)TCKN[=:]\s*(\d{11})
    - (?i)passport[=:]\s*([A-Z0-9]{9})
    - "\\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14})\\b"

  # Replacement string for masked values
  # Default: "[MASKED]"
  mask_replacement: "[MASKED]"

  # Preserve original length of masked content (pad with spaces)
  # Default: false
  # When true, "AKIAIOSFODNN7EXAMPLE" -> "[MASKED_AWS_KEY]       "
  # Useful when positional parsing is expected downstream
  preserve_length: false
```

#### Built-in Patterns (18 total)

| # | Name | Literal Prefix | Matches |
|---|------|---------------|---------|
| 1 | `aws_access_key` | `aws_access_key` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` key=value pairs |
| 2 | `aws_secret_key` | `AKIA` | Raw `AKIA` + 16 alphanumeric chars |
| 3 | `gcp_api_key` | `api_key` | GCP service account key, Google API key patterns |
| 4 | `gcp_service_account` | `service_account` | Full GCP service account JSON with `"type":"service_account"` and `private_key` |
| 5 | `jwt_token` | `eyJ` | JWT tokens (two base64url segments + signature) |
| 6 | `bearer_token` | `Bearer` | Authorization headers with Bearer tokens |
| 7 | `api_key_generic` | `api_key` | Generic `api_key`, `apikey`, `api_secret`, `client_secret` |
| 8 | `password_field` | `password` | `password`, `passwd`, `pwd`, `pass` key=value pairs |
| 9 | `database_url` | `://` | Connection strings for postgres, mysql, mongodb, redis with embedded credentials |
| 10 | `email_address` | `@` | Email addresses (RFC 5322 simplified) |
| 11 | `credit_card` | (none) | Visa, MasterCard, Amex, Discover, Diners card numbers (Luhn-not-validated) |
| 12 | `ssn_us` | (none) | US Social Security numbers (`XXX-XX-XXXX`) |
| 13 | `ipv4_address` | (none) | IPv4 addresses (`0.0.0.0` - `255.255.255.255`) |
| 14 | `ipv6_address` | `:` | Full IPv6 addresses (8 groups of 4 hex digits) |
| 15 | `private_key` | `-----BEGIN` | PEM-encoded private keys (RSA, EC, DSA, OPENSSH) |
| 16 | `ssh_private_key` | `-----BEGIN OPENSSH` | OpenSSH format private keys |
| 17 | `certificate` | `-----BEGIN CERTIFICATE` | PEM-encoded X.509 certificates |
| 18 | `generic_secret` | (none) | Generic `secret`, `token`, `key` with 20+ char values |

#### Optimization: Literal Pre-Filter

Every pattern carries a `literalPrefix` string. Before running the regex, the masker checks `strings.Contains(content, literalPrefix)`:

- If `literalPrefix` is empty (patterns 11, 12, 13, 18) — always run the regex (these patterns are short and cheap: `\b\d{3}-\d{2}-\d{4}\b`, etc.)
- If `literalPrefix` is set and NOT found in the content — skip this pattern entirely (zero regex cost)
- If `literalPrefix` is set and found — run `ReplaceAllString` once

For a typical log line like `"User login successful from 192.168.1.1"`, only patterns 13 (IPv4, no prefix) and maybe 10 (email, prefix "@") will be attempted. The other 16 patterns are skipped because their literal prefixes don't appear in the content.

#### Lock-Free Hot Path

```go
// internal/masker/masker.go

// Separate atomic counters (package-level, not struct fields)
type Masker struct {
    // ...
    linesProc   atomic.Uint64
    patternsMat atomic.Uint64
    bytesMasked atomic.Uint64
}

// Public stats API returns plain uint64 snapshots
type MaskerStats struct {
    LinesProcessed  uint64
    PatternsMatched uint64
    BytesMasked     uint64
}

func (m *Masker) Stats() MaskerStats {
    return MaskerStats{
        LinesProcessed:  m.linesProc.Load(),
        PatternsMatched: m.patternsMat.Load(),
        BytesMasked:     m.bytesMasked.Load(),
    }
}

// The only RWMutex acquisition is in AddPattern/RemovePattern/GetPatterns
// (admin operations called at startup or via API, never on hot path)
```

### 3.5 Sampling Configuration

```yaml
sampling:
  # Enable adaptive sampling
  # Default: true
  enabled: true

  # Error ratio threshold for anomaly detection (0.0 to 1.0)
  # Default: 0.05 (5% error rate triggers anomaly)
  # When error_ratio > threshold, anomaly mode activates
  error_threshold: 0.05

  # Sliding window size (Go duration)
  # Default: 60s
  # Used as fallback when window_buckets * bucket_duration would be smaller
  window_size: 60s

  # Number of time buckets in the sliding window
  # Default: 6
  # Effective window = window_buckets * bucket_duration
  window_buckets: 6

  # Duration of each bucket (Go duration)
  # Default: 10s
  # With window_buckets=6 and bucket_duration=10s, window is 60s
  bucket_duration: 10s

  # Cooldown period after anomaly ends (Go duration)
  # Default: 5m
  # The controller stays in anomaly mode for this duration after
  # the error ratio drops below 70% of threshold (hysteresis)
  cooldown_period: 5m

  # Minimum sample rate (0.0 to 1.0)
  # Default: 0.01 (1% of normal traffic during steady state)
  min_sample_rate: 0.01

  # Maximum sample rate (0.0 to 1.0)
  # Default: 1.0 (100% during anomaly)
  max_sample_rate: 1.0

  # Anomaly window in minutes for rate calculation
  # Default: 5
  anomaly_window_minutes: 5
```

#### How Sampling Works

1. **Event Processing:** Every log line triggers `SamplingController.ProcessEvent(severity, message, source)`. If the severity is `Error`, `Fatal`, or `Panic`, the error counter for the current bucket is incremented. The total counter is always incremented.

2. **Window Rotation:** On each event, the controller checks if `time.Now() - windowStart > numBuckets * bucketDuration`. If so, all buckets are zeroed and the window start is advanced. This is O(1) — a single struct reset per bucket, no linked-list traversal.

3. **Error Ratio:** `errorRatio = sum(errors across all buckets) / sum(total across all buckets)`. This is O(k) where k = number of buckets (typically 6-12).

4. **Anomaly State Machine:**

```
                    errorRatio > threshold
    NORMAL ──────────────────────────────────> ANOMALY
      ^                                          |
      |                                          | errorRatio < threshold * 0.7
      |                                          | AND cooldownPeriod elapsed
      +──────────────────────────────────────────┘
```

5. **Sample Rate:** In normal mode, `sampleRate = min(max(errorRatio, minSampleRate), maxSampleRate)`. In anomaly mode, `sampleRate = 1.0` (100%). The Bernoulli trial `rand.Float64() < sampleRate` determines whether each line passes.

6. **Reset:** `Reset()` sets `minSampleRate` on the next `ShouldSample()` call, ensuring the controller returns to baseline sampling after configuration reloads or circuit breaker events.

#### Hysteresis Detail

Without hysteresis, an error ratio hovering exactly at the threshold would cause the controller to oscillate in and out of anomaly mode on every event. İYE introduces a 70% exit threshold:

```go
exitThreshold := s.config.ErrorThreshold * 0.7  // e.g., 0.035 for a 0.05 threshold
if s.inAnomaly && errorRatio < exitThreshold {
    s.exitAnomaly(now)
}
```

This means:
- Enter anomaly at error_ratio > 0.05
- Exit anomaly only when error_ratio drops below 0.035 AND cooldown period elapsed
- For error ratios between 0.035 and 0.05, the controller stays in whatever state it was in

### 3.6 Metrics Configuration

```yaml
metrics:
  # Enable Prometheus metrics endpoint
  # Default: true
  enabled: true

  # Listen address for HTTP server
  # Default: ":9090"
  listen_address: ":9090"

  # HTTP path for Prometheus metrics
  # Default: "/metrics"
  metrics_path: "/metrics"

  # Scrape interval for Prometheus target annotation
  # Default: 15s
  scrape_interval: 15s

  # Custom histogram buckets for processing duration
  # Default: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
  buckets:
    - 0.005
    - 0.01
    - 0.025
    - 0.05
    - 0.1
    - 0.25
    - 0.5
    - 1
    - 2.5
    - 5
    - 10

  # Custom metric extractors — see §3.9
  custom_metrics: []
```

### 3.7 Buffer Configuration

```yaml
buffer:
  # Enable disk buffer (required if transport is enabled)
  # Default: true
  enabled: true

  # Buffer storage path (Badger LSM-tree directory)
  # Default: "/var/lib/iye/buffer"
  path: /var/lib/iye/buffer

  # Maximum buffer size in bytes
  # Default: 536870912 (512MB)
  # When the buffer reaches this size, writes fail with ErrBufferFull
  max_size_bytes: 536870912

  # Synchronize writes to disk on every write
  # Default: false
  # When true, every Write() call issues fsync (slower but safer).
  # When false, Badger batches fsync at its internal write batch interval.
  sync_writes: false
```

### 3.8 Secure Ingestion & Transport Blueprint

```yaml
transport:
  # Enable remote transport
  # Default: false
  enabled: true

  # Transport protocol
  # Allowed: http, https
  # Default: http
  type: https

  # Remote endpoint URL
  # Required when enabled
  endpoint: https://loki.example.com/loki/api/v1/push

  # Compression algorithm
  # Allowed: none, gzip, zstd
  # Default: zstd
  compression: zstd

  # Maximum batch size (number of log entries per POST)
  # Default: 1000
  batch_size: 1000

  # Maximum time to wait before flushing a partial batch (Go duration)
  # Default: 5s
  batch_timeout: 5s

  # --- TLS Configuration ---

  # Path to TLS client certificate file (PEM)
  # Default: "" (no client certificate)
  tls_cert_file: /etc/iye/certs/client.crt

  # Path to TLS client key file (PEM)
  # Default: "" (no client key)
  tls_key_file: /etc/iye/certs/client.key

  # Path to CA certificate file for server verification (PEM)
  # Default: "" (uses system CA pool)
  ca_cert_file: /etc/iye/certs/ca.crt

  # Skip server certificate verification
  # Default: false
  # WARNING: Setting this to true disables TLS server identity verification.
  # This makes the connection vulnerable to man-in-the-middle attacks.
  # Only use in isolated test environments with network-level controls.
  insecure_skip_verify: false
```

#### Security Profile of `insecure_skip_verify`

| Setting | Risk Profile | Recommended For |
|---------|-------------|-----------------|
| `false` (default) | Full chain verification against system CA or custom CA | All production deployments |
| `true` | MitM attack surface: attacker can present any cert and İYE will connect | Air-gapped test environments, internal CAs not yet deployed |

**mTLS Configuration (Mutual TLS):**

When `tls_cert_file` and `tls_key_file` are both set, İYE presents a client certificate to the server. The server must be configured to validate this certificate. This provides two-way authentication:

1. İYE verifies the server's certificate (via system CA or `ca_cert_file`)
2. The server verifies İYE's certificate (`tls_cert_file` + `tls_key_file`)

mTLS prevents:
- Unauthorized servers from accepting İYE data (data exfiltration prevention)
- Unauthorized agents from injecting data into the log pipeline (log poisoning prevention)

#### Circuit Breaker Logic

```go
const maxConsecutiveFailures = 10

// Transport internal state
var consecutiveFailures int

func (t *Transport) sendBatch(entries []models.LogEntry) error {
    // ... send HTTP POST ...
    if err != nil {
        consecutiveFailures++
        if consecutiveFailures >= maxConsecutiveFailures {
            t.logger.Fatal("Transport circuit breaker triggered: 10 consecutive failures")
            // os.Exit(2) — operator attention required
        }
        return err
    }
    consecutiveFailures = 0
    // ... mark entries as consumed in buffer ...
    return nil
}
```

After 10 consecutive POST failures, the transport logs a fatal error and exits with code 2. This prevents:
- Infinite retry loops consuming CPU and buffer I/O
- Disk buffer filling up with entries that can never be delivered
- Silent data loss (better to crash noisily than drop silently)

#### Backoff Strategy

Between retries within the same batch, İYE uses exponential backoff:

```go
// Simplified backoff implementation
baseDelay := 100 * time.Millisecond
maxDelay := 30 * time.Second

for attempt := 0; attempt < maxRetries; attempt++ {
    delay := baseDelay * (1 << attempt)  // 100ms, 200ms, 400ms, ...
    if delay > maxDelay {
        delay = maxDelay
    }
    time.Sleep(delay)
    // retry POST
}
```

### 3.9 Custom Telemetry Extractors

Custom metric extractors allow you to define named regex patterns in the config that auto-generate Prometheus counters. This is a code-free way to track application-specific events.

```yaml
metrics:
  custom_metrics:
    # Track HTTP response status codes
    - name: http_status
      pattern: "status=(\\d+)"

    # Track API response times
    - name: response_time_ms
      pattern: "duration=([\\d.]+)ms"

    # Track database query performance
    - name: db_query_time
      pattern: "query_time=([\\d.]+)s"

    # Track user registration events
    - name: user_signup
      pattern: "user_registered=([a-zA-Z0-9_]+)"

    # Track payment amounts (floats extracted as metric values)
    - name: payment_amount
      pattern: "amount=\\$?([\\d.]+)"

    # Track error codes from any source
    - name: error_code
      pattern: "code=([A-Z0-9_]+)"
```

Each configuration entry:

1. **Is registered at startup:** `NewMetricsCollector` iterates `config.CustomMetrics` and calls `AddCustomPattern(name, pattern)` for each
2. **Creates a Prometheus counter:** `iye_custom_{sanitized_name}_matches_total` with labels `pattern` and `source`
3. **Is matched on every line:** `MetricsCollector.matchCustomPatterns()` runs in `ProcessLine`, after masking but before sampling
4. **Increments on match:** If the regex matches the line content, the counter is incremented with the pattern name and source file as label values

**Auto-generated Prometheus metrics from the example above:**

```
# HELP iye_custom_http_status_matches_total Matches for custom pattern: http_status
# TYPE iye_custom_http_status_matches_total counter
iye_custom_http_status_matches_total{pattern="http_status",source="/var/log/app/api.log"} 142

# HELP iye_custom_response_time_ms_matches_total Matches for custom pattern: response_time_ms
# TYPE iye_custom_response_time_ms_matches_total counter
iye_custom_response_time_ms_matches_total{pattern="response_time_ms",source="/var/log/app/api.log"} 89

# HELP iye_custom_error_code_matches_total Matches for custom pattern: error_code
# TYPE iye_custom_error_code_matches_total counter
iye_custom_error_code_matches_total{pattern="error_code",source="/var/log/app/error.log"} 12
```

**Important:** The pattern captures groups (`(\d+)`) are not exposed as metric values in the current version. The counter is incremented by 1 for each line that matches the pattern, regardless of what the capture group contains. The capture group is present for forward compatibility (future versions may expose captured values as gauge labels or metric values).

---

## 4. INTEGRATIONS & PROTOCOL BINDINGS

### 4.1 Vendor-Agnostic Transport Layer

İYE is **not locked to any specific backend**. It transmits logs via raw HTTP(S) POST requests with a JSON payload. There is no proprietary protocol, no vendor SDK, no agent plugin, no Sidecar requirement.

#### Wire Format

```json
POST /api/logs HTTP/1.1
Host: receiver.example.com
Content-Type: application/json
Content-Encoding: zstd
User-Agent: iye/0.1.0

{
  "entries": [
    {
      "ts": "2026-06-08T10:42:23.024Z",
      "src": "/var/log/app/api.log",
      "msg": "User login: email=[MASKED_EMAIL] password=[MASKED]",
      "lvl": "info"
    },
    {
      "ts": "2026-06-08T10:42:23.025Z",
      "src": "/var/log/app/api.log",
      "msg": "Payment declined for order #12345, amount=120.93",
      "lvl": "error"
    }
  ],
  "count": 2,
  "compressed": true,
  "algorithm": "zstd"
}
```

The wire format contract:
- `entries`: Array of log entry objects, each with `ts` (ISO 8601), `src` (source file path), `msg` (masked content), `lvl` (severity level)
- `count`: Integer matching `len(entries)` — servers can validate completeness
- `compressed`: Boolean indicating whether the payload body is compressed
- `algorithm`: Compression algorithm name (`zstd`, `gzip`, or empty for uncompressed)

This format is compatible with any HTTP-capable log receiver. No vendor-specific metadata, no service discovery, no custom headers.

#### Compression Behavior

| `config.transport.compression` | `Content-Encoding` header | `Payload.compressed` | `Payload.algorithm` |
|-------------------------------|---------------------------|----------------------|---------------------|
| `"none"` | Not set | `false` | `""` |
| `"gzip"` | `gzip` | `true` | `"gzip"` |
| `"zstd"` | Not set (non-standard) | `true` | `"zstd"` |

**Note on zstd:** `Content-Encoding: zstd` is not natively supported by Go's HTTP client or all receivers. The `compressed` flag in the payload body acts as a secondary signal. Receivers that do not support `Content-Encoding: zstd` should check the `compressed` field and the `algorithm` field to determine how to decode the body. In practice, most Loki-compatible receivers (Grafana Loki, Axiom, Chronosphere) support zstd via the `Content-Encoding` header or via the payload metadata.

#### Receiver Implementation (Minimal)

A minimal Go receiver that accepts İYE payloads:

```go
package main

import (
    "bytes"
    "encoding/json"
    "io"
    "log"
    "net/http"

    "github.com/klauspost/compress/zstd"
)

type LogEntry struct {
    Timestamp string `json:"ts"`
    Source    string `json:"src"`
    Message   string `json:"msg"`
    Level     string `json:"lvl"`
}

type Payload struct {
    Entries    []LogEntry `json:"entries"`
    Count      int        `json:"count"`
    Compressed bool       `json:"compressed"`
    Algorithm  string     `json:"algorithm,omitempty"`
}

func handler(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "read error", 400)
        return
    }

    // Sniff the payload header to check compression
    var header struct {
        Compressed bool   `json:"compressed"`
        Algorithm  string `json:"algorithm,omitempty"`
    }
    json.Unmarshal(body, &header)

    if header.Compressed {
        switch header.Algorithm {
        case "zstd":
            decoder, _ := zstd.NewReader(nil)
            defer decoder.Close()
            var decompressed bytes.Buffer
            decoder.Reset(bytes.NewReader(body))
            io.Copy(&decompressed, decoder)
            body = decompressed.Bytes()
        default:
            http.Error(w, "unsupported compression: "+header.Algorithm, 400)
            return
        }
    }

    var payload Payload
    if err := json.Unmarshal(body, &payload); err != nil {
        http.Error(w, "parse error", 400)
        return
    }

    log.Printf("Received %d entries", payload.Count)
    w.WriteHeader(http.StatusOK)
}
```

(See `deployments/demo/receiver/main.go` for a complete working implementation.)

### 4.2 Grafana Loki

**Endpoint:** `POST /loki/api/v1/push`

**Transport Configuration:**

```yaml
transport:
  enabled: true
  type: https
  endpoint: https://loki-prod.example.com/loki/api/v1/push
  compression: zstd
  batch_size: 2000
  batch_timeout: 5s
```

**Loki Label Mapping:**

Loki requires structured log streams with labels. İYE does not send Loki-format protobuf payloads directly. Instead, you configure a proxy or use Loki's `push` API which accepts JSON:

```bash
# Using nginx as a proxy to convert the format:
# NOTE: This is a simplified example; a full Loki JSON API adapter
# converts the flat entry format to Loki's stream format.

# Loki's JSON push API expects:
# {
#   "streams": [
#     {
#       "stream": { "source": "/var/log/app.log", "level": "info" },
#       "values": [ [ "timestamp", "message" ] ]
#     }
#   ]
# }
```

For direct integration, use a lightweight adapter like Vector or Fluentd as an intermediary (see §4.4).

### 4.3 Elasticsearch / OpenSearch

**Endpoint:** `POST /_bulk`

**Transport Configuration:**

```yaml
transport:
  enabled: true
  type: https
  endpoint: https://elasticsearch-prod.example.com:9200/_bulk
  compression: zstd
  batch_size: 500          # ES recommends smaller batches
  batch_timeout: 5s
```

**Bulk Format Adapter:**

İYE's JSON format differs from ES's bulk format. A lightweight adapter is needed:

```python
# adapter.py — transforms iye entries to ES bulk format
# Can run as a sidecar or intermediate HTTP service

from flask import Flask, request, jsonify
import json

app = Flask(__name__)

@app.route("/_bulk", methods=["POST"])
def forward_bulk():
    payload = request.get_json()
    lines = []
    for entry in payload.get("entries", []):
        action = {"index": {"_index": "logs-iye"}}
        doc = {
            "@timestamp": entry["ts"],
            "source": entry["src"],
            "message": entry["msg"],
            "level": entry["lvl"],
        }
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc))
    bulk_body = "\n".join(lines) + "\n"
    # Forward to Elasticsearch
    # ... requests.post("http://elasticsearch:9200/_bulk", data=bulk_body, ...)
    return jsonify({"accepted": len(payload.get("entries", []))})
```

### 4.4 Logstash / Apache Kafka / Vector

All three can receive İYE payloads via HTTP input plugins.

**Logstash:**

```ruby
# logstash.conf
input {
  http {
    port => 8080
    codec => json
    additional_codecs => { "zstd" => "json" }
  }
}

output {
  elasticsearch {
    hosts => ["http://elasticsearch:9200"]
    index => "logs-iye-%{+YYYY.MM.dd}"
  }
}
```

**Vector:**

```yaml
# vector.toml
[sources.iye_http]
type = "http"
address = "0.0.0.0:8080"
framing.method = "json"
decoding.codec = "json"

[transforms.parse_iye]
type = "remap"
inputs = ["iye_http"]
source = '''
  . = parse_json!(.message) ?? .
  .entries = .entries ?? [.]
  . = .entries[0]
'''

[sinks.loki]
type = "loki"
inputs = ["parse_iye"]
endpoint = "http://loki:3100"
encoding.codec = "json"
```

**Apache Kafka (via HTTP Bridge):**

```yaml
transport:
  enabled: true
  type: http
  endpoint: http://kafka-rest-proxy:8082/topics/iye-logs
  compression: zstd
  # Note: Kafka REST Proxy expects a specific format.
  # Use a lightweight adapter (e.g., Kafka HTTP Bridge) that
  # converts JSON arrays to Kafka messages.
```

### 4.5 Custom Webhooks / Internal SIEM

Any HTTP endpoint that accepts POST requests with JSON bodies can receive İYE data:

```yaml
transport:
  enabled: true
  type: https
  endpoint: https://siem.internal.example.com/api/v1/ingest
  compression: gzip            # SIEM systems often prefer gzip over zstd
  batch_size: 1000
  batch_timeout: 10s
```

**Custom Header Injection:**

To add authentication tokens or routing headers, deploy a reverse proxy in front of İYE:

```nginx
# nginx.conf — injects API key header before forwarding to SIEM
server {
    listen 8080;
    location / {
        proxy_pass https://siem.internal.example.com/api/v1/ingest;
        proxy_set_header X-API-Key "your-siem-api-key";
        proxy_set_header X-Source "iye";
    }
}
```

Then configure İYE to point at the proxy:

```yaml
transport:
  endpoint: http://localhost:8080
```

---

## 5. FAQ & REAL-WORLD TROUBLESHOOTING

### 5.1 Data Integrity

**Q: Does İYE drop data?**

A: Yes, intentionally and measurably. İYE is designed to drop **noise** — routine log lines that carry no signal value. The key differentiator is **which** lines are dropped and how that decision is made:

| Line Type | Pipeline Tag | Dropped? | Rationale |
|-----------|-------------|----------|-----------|
| `INFO: healthcheck passed` | default | ~99% | Repeated every 10s, zero operational value |
| `DEBUG: entering function foo` | default | ~99% | Development artifact |
| `ERROR: connection refused to db` | default | 0% during anomaly | Escalated to 100% sampling |
| `WARN: memory usage 85%` | default | ~95% | Trend visible in metrics |
| `AUDIT: user admin deleted record 42` | compliance | 0% | Legal retention requirement |
| `FINANCIAL: transaction TX1234 completed` | compliance | 0% | PCI-DSS retention requirement |

The sampling controller provides two guarantees:

1. **Probabilistic fairness:** Every line has `sampleRate` chance of being sampled, regardless of content. There is no content-based filtering (that's the masker's job). The Bernoulli trial is `rand.Float64() < sampleRate`.

2. **Anomaly escalation:** When error rate exceeds threshold, sampling escalates to 100%. Errors during incidents are never dropped.

Compliance pipelines provide a third guarantee:

3. **Zero-drop compliance:** `compliance: true` pipelines skip sampling entirely. Every line is buffered and transported. No exceptions.

**Q: What happens if the disk buffer fills up?**

A: The buffer has a configurable `max_size_bytes` (default 512MB). When this limit is reached, `buf.Write()` returns `ErrBufferFull`. The pipeline logs a warning and drops the line. The metrics counter `iye_buffer_dropped_total` is incremented.

In practice, the buffer should be sized to hold approximately 2-4 hours of log data at peak volume. Transport failures within that window are survivable. Beyond that, either:
- The circuit breaker will trigger (10 consecutive failures → process exit)
- The buffer will fill up and start dropping new lines

Monitoring `iye_buffer_size_bytes` and alerting at 80% capacity is recommended.

### 5.2 Resilience

**Q: What happens if the central log server goes down?**

A: İYE's behavior depends on the transport configuration:

1. **Transport enabled + buffer configured (recommended):** The transport layer continues retrying with exponential backoff. The buffer fills up as new lines arrive. When the server comes back online, the batcher flushes the backlog. If the outage exceeds the buffer capacity, new lines are dropped (see above).

2. **Transport enabled + buffer NOT configured:** İYE exits with a fatal error on transport failure. This is deliberate: without a buffer, İYE cannot guarantee delivery semantics. Running without a buffer in production is unsupported.

3. **Transport disabled:** İYE runs as a local-only processor. Lines are masked and counted but never shipped. This is useful for audit-only deployments where logs are read directly from the buffer path.

**Q: How does İYE handle log rotation?**

A: İYE detects rotation by comparing inode numbers on every poll cycle. When a rotation is detected:

1. The old file descriptor is closed
2. The new file is opened at offset 0
3. The `bufio.Reader` is reset
4. Reading resumes from the beginning of the new file

This handles:
- `logrotate create` (file replaced with new inode)
- `logrotate copytruncate` (same inode, but file size drops below offset → truncation detected)
- Docker/kubelet log rotation (container stdout redirected to new file)

Between the rotation event and the next poll cycle, any lines written to the old file are lost. With a 100ms poll interval, this window is at most 100ms.

**Q: What happens if İYE crashes?**

A: On crash:

1. **In-flight lines (in tailer output channel, in masker, in pipeline):** Lost. These lines were read from disk but not yet written to the buffer.
2. **Buffered lines (on disk via Badger):** Preserved. On restart, the buffer replays the Badger WAL and recovers all entries.

This is the "at-least-once" delivery guarantee: every line written to the buffer will be transported at least once. Lines between file offset and buffer may be lost (at-most-once from file to buffer).

### 5.3 Masking Accuracy

**Q: Is masking accurate?**

A: Masking is regex-pattern-based, not cryptographic. Accuracy depends on:

1. **Pattern coverage:** The 18 built-in patterns cover the most common PII types (AWS keys, GCP service accounts, JWT tokens, email, credit cards, SSNs, IPs, private keys). Each pattern was tested against real-world log samples.

2. **False positives:** Patterns are intentionally broad. For example, the email regex `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}` will match any email-like string, including false positives like `example@nonexistent`. The tradeoff is deliberate: better to over-mask than expose PII.

3. **False negatives:** Patterns may miss:
   - Non-standard PII formats (e.g., `email: user at example dot com`)
   - Encoded/escaped PII (e.g., `email%3Duser%40example.com`)
   - PII split across multiple lines
   
   Custom patterns (`masker.custom_patterns`) should be added for domain-specific PII types.

4. **The demo's masking percentage:** In the demo environment, the log generator produces lines with a high density of PII content. The `~75% masking rate` in the demo represents the log generator's PII density, not İYE's detection rate. In production, masking rates vary from 5-30% depending on the application's logging patterns.

**Q: Can İYE be bypassed by an attacker?**

A: If an attacker gains root access to the host, they can:
- Read log files directly (before İYE masks them)
- Stop the İYE process
- Modify the İYE binary or config

İYE provides defense-in-depth, not absolute protection. Its security value is:
- Preventing accidental PII exposure in centralized log systems
- Reducing the blast radius of a compromised SIEM/loki account
- Providing audit trails at the edge (if buffer is preserved after incidents)

mTLS transport prevents unauthorized agents from injecting data, and requires authorized agents to present valid client certificates.

### 5.4 Performance Characteristics

**Q: What is İYE's resource footprint?**

| Resource | Idle | Typical Load | Peak |
|----------|------|-------------|------|
| RAM | ~12-15 MB | ~20-30 MB | ~50 MB (1MB line buffered) |
| CPU | <0.1% | 1-3% (1000 lines/sec) | 10-15% (10000 lines/sec) |
| Disk (buffer) | 0 | Up to 512 MB | Configurable |

RAM profile is dominated by:
- Badger LSM-tree memory map (configurable, ~8MB default)
- Tailer output channel (10,000 * ~200 bytes = ~2MB)
- Pipeline goroutine stacks (~8KB each, negligible)

**Q: What throughput can İYE handle?**

On the tested hardware (11th Gen Intel i5-1135G7 @ 2.40GHz):

| Lines/sec | CPU Usage | Bottleneck |
|-----------|-----------|------------|
| 100 | <0.5% | Idle |
| 1,000 | 1-3% | Masker (21µs/line) |
| 10,000 | 10-15% | Masker + serialization |
| 50,000 | 50-70% | Buffer I/O |

Beyond 50,000 lines/sec, the disk buffer becomes the bottleneck (Badger write throughput). For higher throughput, either:
- Disable the buffer for non-critical pipelines (risk: no crash recovery)
- Use multiple İYE instances with partitioned log files
- Tune Badger options (not exposed in config currently)

### 5.5 Operational Concerns

**Q: How do I monitor İYE health?**

A: Two mechanisms:

1. **Prometheus metrics** (`/metrics` endpoint): Track `iye_log_lines_total`, `iye_buffer_size_bytes`, `iye_anomaly_events_total`. Alert on buffer capacity >80%, anomaly events, or sudden drops in line throughput.

2. **Health endpoints:**
   - `GET /healthz` → Always returns `200 OK` (process is alive)
   - `GET /readyz` → Returns `200 OK` when server is initialized, `503 Service Unavailable` otherwise

**Q: How do I update İYE without dropping logs?**

A: Rolling update strategy:

1. Deploy new İYE binary alongside the old one (different binary path)
2. Send SIGTERM to old process (triggers graceful shutdown: flush buffer, stop transport)
3. Verify old process has exited (buffer is drained)
4. Start new process with same config (replays buffer from last committed position)

During the window between step 2 and step 4, log files are still being written but İYE is not reading them. On startup, the tailer reads from the end of each file (unless `from_beginning: true`), so no lines are duplicated. A small number of lines written during the restart window are lost (at-most-once guarantee from file to buffer).

For zero-downtime updates, run two İYE instances with disjoint file paths (partitioned by pipeline).

**Q: How do I debug İYE?**

A:

```bash
# Increase log verbosity
./iye -log-level debug

# Check configuration validity without running
# (Parse the config in isolation)
# Currently not available as a subcommand; use `iye init` validation output

# Inspect Prometheus metrics
curl http://localhost:9090/metrics | grep iye_

# Check buffer contents
ls -la /var/lib/iye/buffer/

# Trace a single line through the pipeline
# Set log-level=debug, grep for the source file:
tail -f /var/log/iye/iye.log | grep "/var/log/app/api.log"

# Verify masking output
# Run iye with a small test log file and inspect the receiver output
```

**Q: Does İYE support Windows?**

No. İYE relies on Linux-specific features:
- Inode-based file rotation detection (`syscall.Stat_t.Ino`)
- `gcr.io/distroless` base image for Docker
- Go's `os.Stat()` returns different struct on Windows

---

## 6. APPENDIX: PROMETHEUS METRICS REFERENCE

### Counter Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `iye_log_lines_total` | Counter | `source`, `level` | Total log lines entering the pipeline |
| `iye_log_bytes_total` | Counter | `source` | Total bytes of log content processed |
| `iye_log_lines_masked_total` | Counter | `source` | Lines where at least one PII pattern matched |
| `iye_log_lines_sampled_total` | Counter | `source`, `mode` | Lines selected for output (mode: `normal` or `anomaly`) |
| `iye_log_lines_dropped_total` | Counter | `source` | Lines dropped by sampling decision |
| `iye_anomaly_events_total` | Counter | `type`, `source` | Anomaly state transitions: `anomaly_started`, `anomaly_ended` |
| `iye_buffer_dropped_total` | Counter | `buffer` | Lines dropped due to buffer full |
| `iye_custom_pattern_matches_total` | Counter | `pattern`, `source` | Matches for custom metric extraction patterns |

### Gauge Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `iye_current_sampling_mode` | Gauge | `source` | 0 = normal, 1 = anomaly |
| `iye_buffer_size_bytes` | Gauge | `buffer` | Current disk buffer usage in bytes |

### Histogram Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `iye_log_processing_duration_seconds` | Histogram | `stage` | Latency per pipeline stage (`process_line`) |

### Recommended Alerts

```yaml
# prometheus-alerts.yml
groups:
  - name: iye
    rules:
      - alert: IYEBufferCapacityHigh
        expr: iye_buffer_size_bytes / 536870912 > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "İYE buffer capacity >80%"

      - alert: IYEAnomalyActive
        expr: iye_current_sampling_mode > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "İYE in anomaly mode (high error rate detected)"

      - alert: IYEZeroThroughput
        expr: rate(iye_log_lines_total[5m]) == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "İYE processing zero log lines (check tailer config)"
```

---

## 7. APPENDIX: PERFORMANCE BENCHMARKS

### Methodology

All benchmarks run on:
- CPU: 11th Gen Intel Core i5-1135G7 @ 2.40GHz (8 logical cores)
- RAM: 16GB DDR4
- OS: Linux (kernel 6.x)
- Go: 1.23
- Isolation: `GOMAXPROCS=8`, no competing load

Benchmarks use `testing.B` with `-benchtime=200ms` to collect statistically significant samples. Each benchmark reports:
- **ns/op:** Nanoseconds per operation (lower is better)
- **B/op:** Bytes allocated per operation (lower is better)
- **allocs/op:** Heap allocations per operation (lower is better)

### Masker Benchmarks

```
BenchmarkMasker-8          5,176    46,264 ns/op    3,248 B/op    39 allocs/op
BenchmarkMasker_Mixed-8   10,000    21,061 ns/op    1,233 B/op    16 allocs/op
```

**BenchmarkMasker:** Single complex log line with 4 PII types (AWS key, JWT token, email, password). All 4 patterns match, all 18 patterns are attempted (pre-filter eliminates 14, 4 pass through to regex).

**BenchmarkMasker_Mixed:** 7 realistic log lines in rotation:
1. Credit card charge declined (matches CC pattern)
2. Login with email (matches email)
3. MySQL slow query with embedded email (matches email)
4. ZooKeeper transaction (no PII)
5. Apache access log with IP (matches IPv4)
6. JSON health check (no PII)
7. SMS notification with phone number (no built-in pattern for phone, so no match)

Average: 2.3 PII patterns match per line (some lines have 0, some have 2-3).

### Metrics Collector Benchmark

```
BenchmarkMetricsCollector_ProcessLine-8   780,417    303 ns/op    0 B/op    0 allocs/op
```

303 nanoseconds per line. Zero allocations — all counters are pre-registered Prometheus metric handles. The hot path is counter increments + label lookups.

### Sampling Controller Benchmark

```
BenchmarkSamplingController_ProcessEvent-8   3,246,427    73.8 ns/op    0 B/op    0 allocs/op
```

73.8 nanoseconds per event. Zero allocations. The critical section (bucket increment, window rotation check, anomaly evaluation) is a single `sync.Mutex` lock over ~12 memory operations.

### Benchmark Comparison vs Pre-Optimization

| Component | Before | After | Improvement |
|-----------|--------|-------|-------------|
| Masker (dense PII line) | ~72,000 ns | ~46,000 ns | 1.56x |
| Masker (mixed realistic) | N/A | ~21,000 ns | N/A (new benchmark) |
| Metrics (per line) | ~25,000 ns | ~303 ns | 82.5x |
| Sampling (per event) | ~9,000 ns | ~74 ns | 121.6x |

The dramatic improvement in Metrics and Sampling is primarily from:
- Eliminating `container/list.List` traversal (was O(n) per event)
- Removing `RWMutex.RLock()` from masker hot path
- Using `atomic.Uint64` instead of mutex-protected counters
- Optimizing Prometheus metric registration to happen once at startup

### Key Architectural Wins

1. **Masker pre-filter (A2):** 12 of 18 patterns have a `literalPrefix` that rarely appears in non-matching lines. This eliminates ~14 regex runs per line on average.

2. **Bucket-based window (B1):** `list.List` cleanup was O(n) with n up to 10,000. Bucket cleanup is O(1) — reset a fixed-size array of at most 12 elements.

3. **Atomic stats (A3):** The old code acquired `sync.RWMutex.RLock()` for every `Mask()` call (18 patterns x 2 regex ops). The new code uses `atomic.Uint64.Add()` which compiles to a single `LOCK ADD` instruction on x86-64.

4. **Zero-alloc metrics:** Prometheus `promauto` counters are pre-registered at startup. At runtime, `WithLabelValues()` returns a cached reference. The first call to `WithLabelValues` for each unique label set allocates; subsequent calls reuse the cached `Counter` handle.

5. **Pooled buffers (A4):** The old code allocated a fresh `[]byte` for every line's `Raw` field. The new code reuses pooled buffers for 99.9% of lines (those under 64KB). Dynamic growth handles the remaining 0.1% with a one-off allocation.
