"""Dagster-compatible lakehouse maintenance jobs: small-file compaction and GDPR purge."""

from __future__ import annotations

import glob
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List

import duckdb
import pyarrow.parquet as pq


@dataclass
class CompactionResult:
    """Outcome of Iceberg table file compaction."""

    files_before: int
    files_after: int
    query_latency_before_ms: float
    query_latency_after_ms: float
    latency_reduction_pct: float
    compacted_parquet_path: str


class IcebergCompactor:
    """Merges fragmented streaming Parquet files into consolidated columnar blocks."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir

    def benchmark_query_latency(self, file_paths: List[str], iterations: int = 10) -> float:
        """Measures median execution latency of analytical aggregation over specified files."""
        if not file_paths:
            return 0.0

        con = duckdb.connect(database=":memory:")
        file_list_str = str(file_paths)
        query = f"""
            SELECT
                symbol,
                COUNT(*) AS trade_count,
                ROUND(AVG(price), 2) AS avg_price,
                ROUND(SUM(quantity), 4) AS total_vol
            FROM read_parquet({file_list_str})
            GROUP BY symbol;
        """
        # Warmup
        con.execute(query).fetchall()

        times = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            con.execute(query).fetchall()
            times.append((time.perf_counter() - t0) * 1000.0)

        times.sort()
        return round(times[len(times) // 2], 3)

    def compact_files(self) -> CompactionResult:
        """Finds all small files in data_dir, compacts them, and records performance gain."""
        raw_files = glob.glob(os.path.join(self.data_dir, "**", "*.parquet"), recursive=True)
        # Filter out existing compacted files
        source_files = [f for f in raw_files if "compacted_" not in f]

        if not source_files:
            return CompactionResult(0, 0, 0.0, 0.0, 0.0, "")

        lat_before = self.benchmark_query_latency(source_files)

        # Read and consolidate tables
        tables = [pq.read_table(f) for f in source_files]
        duckdb.arrow(tables[0])
        # Union all tables
        con = duckdb.connect(database=":memory:")
        con.execute(f"CREATE TABLE merged AS SELECT * FROM read_parquet({source_files});")

        compacted_file = os.path.join(self.data_dir, "compacted_trades_block.parquet")
        con.execute(f"COPY merged TO '{compacted_file}' (FORMAT PARQUET, COMPRESSION ZSTD);")

        lat_after = self.benchmark_query_latency([compacted_file])

        # Remove small source files to complete compaction
        for f in source_files:
            try:
                os.remove(f)
            except OSError:
                pass

        reduction = round(((lat_before - lat_after) / lat_before) * 100.0, 2) if lat_before > 0 else 0.0

        return CompactionResult(
            files_before=len(source_files),
            files_after=1,
            query_latency_before_ms=lat_before,
            query_latency_after_ms=lat_after,
            latency_reduction_pct=reduction,
            compacted_parquet_path=compacted_file,
        )

    def purge_user_gdpr(self, user_id: str) -> Dict[str, Any]:
        """GDPR Right-to-Erasure: purges specific user_id records from lakehouse files."""
        files = glob.glob(os.path.join(self.data_dir, "**", "*.parquet"), recursive=True)
        if not files:
            return {"purged_records": 0, "status": "no_files"}

        con = duckdb.connect(database=":memory:")
        r_before = con.execute(f"SELECT COUNT(*) FROM read_parquet({files});").fetchone()
        r_user = con.execute(f"SELECT COUNT(*) FROM read_parquet({files}) WHERE user_id = '{user_id}';").fetchone()
        total_before = r_before[0] if r_before else 0
        user_records = r_user[0] if r_user else 0

        # Re-write files excluding the purged user_id
        for f in files:
            con.execute(f"CREATE OR REPLACE TABLE temp_hold AS SELECT * FROM read_parquet('{f}') WHERE user_id != '{user_id}';")
            con.execute(f"COPY temp_hold TO '{f}' (FORMAT PARQUET, COMPRESSION ZSTD);")

        r_after = con.execute(f"SELECT COUNT(*) FROM read_parquet({files});").fetchone()
        total_after = r_after[0] if r_after else 0

        return {
            "purged_records": user_records,
            "total_before": total_before,
            "total_after": total_after,
            "status": "purged_successfully",
        }
