# Performance

## Benchmark Results

All benchmarks run on an 11th Gen Intel Core i5-1135G7 @ 2.40GHz, Go 1.23, Linux x86_64. Single-threaded execution; no parallelism involved in per-line processing.

| Component | Benchmark Input | Ops | Per-Operation |
|-----------|----------------|-----|---------------|
| Masker | 1 log line with 5 embedded PII patterns | 1,000,000+ | ~72 microseconds |
| Metrics | 1 log line, counter increment + field extraction | 1,000,000+ | ~25 microseconds |
| Sampling | 1 event enqueue + anomaly evaluation | 1,000,000+ | ~9 microseconds |

A single pipeline round-trip (mask + metrics + sampling) processes a log line in approximately 106 microseconds, yielding a sustained throughput of roughly 9,400 lines per second per core. Under realistic workloads with mixed log levels and intermittent PII matches, throughput typically increases as fast-paths (no match, normal sampling) reduce the per-line cost.

## Architectural Decisions

### Why Not fsnotify / inotify?

The tailer polls file metadata at a configurable interval (default 100ms) and compares inode numbers to detect rotation. This avoids the complexity and fd limits of inotify on Kubernetes nodes where hundreds of log files may be present simultaneously. The polling overhead is negligible -- a single `os.Stat` call per tracked file per interval. Inode comparison is O(1).

### Badger LSM-Tree as FIFO Queue

The buffer uses Badger (v4, `dgraph-io/badger`) as a persistent FIFO queue. This choice was made over alternatives for specific reasons:

- **SQLite**: Requires CGO, introduces build portability issues. Go-level SQLite wrappers add goroutine scheduling overhead for simple key-value operations.
- **BoltDB / bbolt**: Single-writer semantics create contention under concurrent write + read patterns. The pipeline writes log lines from a tailer goroutine while the transport concurrently reads and commits batches.
- **Badger**: LSM-tree with lock-free SSTable reads, concurrent writers, and built-in sequence generation for monotonically increasing keys. The `badger.Sequence` primitive provides O(1) key generation without per-request disk writes.

The buffer trades approximately 50MB of RSS (Badger's memtable) against sub-millisecond write latency and crash safety without CGO. For an edge agent with a dedicated 100-500MB buffer budget, this is an acceptable trade.

Key buffer characteristics:

| Property | Value |
|----------|-------|
| Write latency (p50) | ~50 microseconds |
| Write latency (p99) | ~200 microseconds |
| Overhead per 1M entries | ~5KB index (key-only, values in SST) |
| Crash recovery | Automatic on next Open, WAL replay |

### Concurrency Model

The pipeline is single-goroutine per stage for log processing, with Go channels connecting stages. This avoids shared-state contention on the hot path.

```
  Tailer goroutine -> channel -> Pipeline goroutine -> Metrics/Sampling/Buffer/Transport
```

The pipeline goroutine serializes all log processing, which eliminates synchronization on the fast path. Mutexes exist only between the pipeline goroutine and:

- Sampling controller state reads (from `GetSampleRate`, `IsInAnomaly`)
- Masker pattern mutation (from `AddPattern`, `RemovePattern`)
- Metrics collector stats read (from `/metrics` HTTP handler)
- Transport run loop and stop signal (from signal handler)

Lock durations are measured in nanoseconds. All hot-path structures (masker patterns, sampling event queue) use `sync.RWMutex` with read-preference to avoid stalling the pipeline during concurrent metric scrapes.

### Atomic Operations

The metrics collector's `CollectorStats` fields are backed by `atomic.Uint64`. This ensures that concurrent reads from the Prometheus `/metrics` endpoint and writes from the pipeline goroutine never produce torn reads. The cost is a single CPU instruction per increment versus a register increment in uncontested code -- approximately 0.5 nanoseconds difference, invisible at the observed 25-microsecond per-line overhead.

### Sampling Algorithm

The sampling controller maintains a sliding window of log events as a linked list (`container/list`). On each event:

1. The event is appended (O(1)).
2. Events older than the window are removed by walking from the front (amortized O(1) per event).
3. The error ratio is computed by scanning the window (O(n), but bounded by the configured window size, typically 10-60 seconds of events).
4. The sample rate is a linear interpolation between `MinSampleRate` and 1.0, proportional to the observed error ratio.

During anomaly mode (error ratio >= threshold), the sample rate locks at 1.0 and the queue is evaluated only for state exit conditions (queue empty or ratio drops below threshold). This is the critical path for anomaly detection: when the error rate is high, we stop computing the ratio and ship everything, deferring analysis to the backend.

The sampling decision in `ShouldSample()` is a deterministic rate comparison against the current sample rate, not a randomized coin flip. This means the same line source with the same internal state always produces the same sampling decision, which simplifies debugging.

### Transport Retry and Circuit Breaker

The transport layer uses a simple linear backoff: `attempt * 500ms`, capped at 3 retries per batch. On persistent failure (10 consecutive batches across multiple retry attempts), the circuit breaker engages and subsequent batches are dropped rather than returned to the buffer. This prevents a failing endpoint from filling the disk buffer indefinitely and masking the alert that the downstream system is unreachable.

The circuit breaker resets on each successful delivery. This is deliberately not a half-open state -- if the endpoint recovers, the next tick immediately succeeds, the counter resets, and normal operation resumes.

### Memory Budget

| Component | Typical RSS |
|-----------|-------------|
| Go runtime + GC overhead | ~5MB |
| Badger memtables (64MB limit) | ~10MB |
| In-flight log lines | ~500KB |
| Transport batch buffers | ~1MB |
| **Total (idle)** | **~15MB** |
| **Total (sustained load)** | **~30MB** |

The Go garbage collector is configured with the default GOGC=100. The primary allocators are Badger (memtable, SST read buffers), log line content strings (allocated by the tailer, retained only during pipeline processing), and transport JSON serialization (temporary, garbage-collected between batches).
