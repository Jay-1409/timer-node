# Benchmarking `atimer`

This guide explains how to benchmark `atimer` across multiple performance dimensions:
1. **Ingestion Throughput & Latency** (HTTP scheduling capacity)
2. **Timer Accuracy & Drift** (Timing fidelity and delivery guarantees)
3. **Flood / Burst Resilience** (Simultaneous expirations & worker pool draining)
4. **Sharded Multi-Heap Scaling** (Lock contention reduction across heaps)
5. **Internal Microbenchmarks** (Heap operations, atomic routing, memory allocations)

---

## Quick Start

Run the entire benchmark suite with a single command:

```bash
make bench-all
```

Or build the standalone benchmark tool:

```bash
make build
```

---

## Benchmark Scenarios

### 1. Ingestion Throughput (`throughput`)
Measures maximum HTTP request scheduling rate (RPS) and response latency distribution.

```bash
./bin/benchmark -scenario throughput -requests 10000 -concurrency 50
```

**Key Metrics**:
- **Throughput (req/s)**: Max scheduling speed.
- **Latency Percentiles**: $p50$, $p90$, $p95$, $p99$, $p99.9$, and max response latency.
- **Success Rate**: Number of accepted vs rejected/overflowed tasks.

---

### 2. Timer Accuracy & Drift Analysis (`accuracy`)
Schedules timers with a target delay and measures the difference between scheduled fire time and the exact instant the HTTP callback arrives at the receiver.

```bash
./bin/benchmark -scenario accuracy -requests 2000 -concurrency 20 -delay 1s
```

**Key Metrics**:
- **Delivery Rate (%)**: Percentage of scheduled timers that reached the callback receiver.
- **Mean / Median Drift**: Average and median lag in milliseconds/microseconds.
- **P95 / P99 Drift**: Tail jitter under load.

---

### 3. Task Flood & Burst Handling (`flood`)
Schedules thousands of timers configured to expire at the **exact same instant** $T_{\text{target}}$. Tests min-heap popping throughput and worker pool queue draining under heavy burst load.

```bash
./bin/benchmark -scenario flood -requests 2000 -concurrency 50 -delay 1s
```

---

### 4. Sharded Multi-Heap Scaling (`scaling`)
Sweeps through heap counts ($1, 2, 4, 8, 16$ heaps) with identical workloads to demonstrate lock contention reduction and throughput speedup.

```bash
./bin/benchmark -scenario scaling -requests 10000 -concurrency 50
```

---

## Command Line Flags

| Flag | Default | Description |
|---|---|---|
| `-scenario` | `throughput` | Benchmark scenario (`throughput`, `accuracy`, `flood`, `scaling`, `all`) |
| `-requests` | `5000` | Total timer requests to schedule |
| `-concurrency` | `50` | Number of concurrent client worker goroutines |
| `-rate` | `0` | Max requests per second rate limit (0 = unlimited) |
| `-delay` | `1s` | Timer duration for accuracy and flood tests |
| `-heaps` | `4` | Number of heaps for embedded instance |
| `-workers` | `4` | Number of notification workers per heap |
| `-queue-size` | `100000` | Task queue capacity per heap |
| `-target` | `""` | Target URL (e.g. `http://localhost:8080`). Runs embedded if empty |
| `-receiver-port` | `0` | Port for mock callback receiver (0 = auto ephemeral) |
| `-receiver-latency`| `0s` | Artificial callback delay to simulate slow webhooks |
| `-output` | `table` | Output format: `table`, `json`, `markdown` |
| `-report-file` | `""` | Optional filepath to save report (`.md` or `.json`) |

---

## Internal Go Microbenchmarks

Run Go's native benchmark engine to measure raw data structure speed and memory allocations:

```bash
make bench-unit
# Or directly with go test:
go test -bench=. -benchmem ./internals/timer/...
```

This runs:
- `BenchmarkTimerTaskHeap_PushPop`: Min-heap push and pop throughput.
- `BenchmarkTimerHeap_AddTask_SingleGoroutine`: Single-goroutine mutex acquisition & insertion.
- `BenchmarkTimerHeap_AddTask_Parallel`: Parallel lock contention on a single heap.
- `BenchmarkTimerHeap_FireExpired_Flood`: Mass expiration popping throughput.
- `BenchmarkTimer_AddTask_1Heap` to `BenchmarkTimer_AddTask_16Heaps`: Multi-heap atomic round-robin scaling.
