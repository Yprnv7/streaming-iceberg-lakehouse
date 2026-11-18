# Streaming Apache Iceberg Lakehouse Benchmark Report

**Benchmark Suite:** Real-Time Streaming Ingestion, Watermark Enforcement & Small-File Compaction
**Execution Date:** 2026-11-18
**Hardware Platform:** Apple A18 Pro | 8.0 GB RAM | Darwin 25.4.0 (arm64)
**Python Runtime:** Python 3.14.6 | PyArrow + DuckDB + Apache Iceberg

---

## 1. Streaming Ingestion & Watermarking Performance

| Metric | Measured Value | Operational SLA | Status |
| :--- | :--- | :--- | :--- |
| **Ingestion Throughput** | **15231.3 events/sec** | > 2,000 events/sec | **Exceeded** |
| **Micro-Batch Commit p50** | **0.54 ms** | < 15.0 ms | **Exceeded** |
| **Micro-Batch Commit p95** | **0.85 ms** | < 50.0 ms | **Exceeded** |
| **Event-Time Watermark Lag** | **10.0 minutes** | Configurable | Enforced |
| **Late Events Handled / Dropped** | **54 events** | Zero corruption | Verified |
| **Duplicate Records Committed** | **0 (Exactly-Once Deduplicated)** | 0 | **Verified** |

---

## 2. Small-File Compaction Benchmark (`rewrite_data_files`)

High-frequency streaming commits generate numerous small files (file fragmentation). The Dagster compaction job consolidates small Parquet files into optimized columnar blocks:

| State | Number of Data Files | Analytical Query Latency (ms) | Speedup Factor |
| :--- | :--- | :--- | :--- |
| **Before Compaction (Streaming Output)** | **25 files** | **1.673 ms** | Baseline |
| **After Compaction (Consolidated Block)** | **1 file** | **0.499 ms** | **70.17% faster** |

---

## 3. Deterministic 24-Hour Stream Replay & Cryptographic Parity

Full reprocessing of raw event logs asserts bit-for-bit mathematical equality against the live table:

| Verification Metric | Live Stream Table | Reprocessed Shadow Table | Status |
| :--- | :--- | :--- | :--- |
| **Row Count** | **200 rows** | **200 rows** | Exact Match |
| **Cumulative SHA-256 Digest** | `ae6aedacad50b37e97248d95...` | `ae6aedacad50b37e97248d95...` | **100% Cryptographic Parity** |
| **Audit Status** | Validated | Validated | **PASS** |

---

## 4. How to Reproduce

Run the automated lakehouse benchmark on your machine:
```bash
python scripts/run_iceberg_benchmarks.py --output docs/BENCHMARKS.md
```
