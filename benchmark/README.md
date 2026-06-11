# Isolate vs direct disk I/O latency benchmark

Compares end-to-end latency for the multi-isolate [`MultiIsolateKvStoreClient`](../lib/multi_isolate_kv_store_client.dart) against equivalent **caller-isolate** `File` I/O via [`lib/direct_kv_io.dart`](lib/direct_kv_io.dart).

## What is measured

| ID | Operation | Payload |
|----|-----------|---------|
| W1 | truncate write | 256 B, 1 chunk |
| W2 | truncate write | 4 KiB, 1 chunk |
| W3 | truncate write | 64 KiB, 1 chunk |
| W4 | truncate write | 1 MiB, 16×64 KiB chunks |
| R1 | read | 4 KiB (validates direct-read baseline) |
| D1 | delete | after 4 KiB write |

Each scenario: **5 warmup** + **50 measured** iterations. Reports mean, min, max, p50, p95 (microseconds) and writes `results/latest.json`.

## Run

```bash
cd benchmark
dart pub get
dart run bin/latency_benchmark.dart
```

### Many small files (read-heavy workload)

Sequential read of many small KV files (default: 1000 × 512 B):

```bash
dart run bin/many_small_reads_benchmark.dart
dart run bin/many_small_reads_benchmark.dart 2000 1024
```

Compares `readStream` via the isolate client, direct helper, raw `openRead`, and `readAsBytes`. Writes `results/many_small_reads.json`.

Run on an idle machine for stable numbers. Results vary by OS and storage (SSD vs HDD, Windows vs Linux).

## Architecture notes

- **Reads** in the production client already use caller-isolate `File.openRead()`; R1 should show ~1.0× overhead.
- **Writes/deletes** route through router + worker isolates; W* and D1 show isolate message-passing and coordination cost.
- Direct baseline uses the same [`HashedKvPath`](../lib/hashed_kv_path.dart) layout and temp-file + rename truncate semantics as workers.

## Interpreting overhead

`overheadRatio` = isolate mean latency / direct mean latency. Values well above 1.0 on small writes indicate round-trip dominated workloads; large writes amortize isolate cost across chunk I/O.
