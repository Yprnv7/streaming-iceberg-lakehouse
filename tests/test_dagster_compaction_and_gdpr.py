"""Tests for Iceberg small-file compaction and GDPR right to erasure."""

import pyarrow as pa
import pyarrow.parquet as pq

from orchestration.dagster_jobs import IcebergCompactor


def test_iceberg_small_file_compaction(tmp_path):
    data_dir = str(tmp_path)
    # Write 5 small files
    for i in range(5):
        row = [{"event_id": f"ev_{i}", "symbol": "BTC/USDT", "price": 65000.0, "quantity": 0.5, "trade_side": "BUY", "user_id": f"usr_{i}"}]
        pq.write_table(pa.Table.from_pylist(row), f"{data_dir}/part_{i}.parquet")

    compactor = IcebergCompactor(data_dir=data_dir)
    res = compactor.compact_files()

    assert res.files_before == 5
    assert res.files_after == 1
    assert res.latency_reduction_pct >= 0.0


def test_gdpr_purge_removes_user_records(tmp_path):
    data_dir = str(tmp_path)
    rows = [
        {"event_id": "ev_1", "symbol": "BTC/USDT", "price": 65000.0, "quantity": 1.0, "trade_side": "BUY", "user_id": "usr_purge_me"},
        {"event_id": "ev_2", "symbol": "ETH/USDT", "price": 3400.0, "quantity": 2.0, "trade_side": "SELL", "user_id": "usr_keep_me"},
    ]
    p = f"{data_dir}/data.parquet"
    pq.write_table(pa.Table.from_pylist(rows), p)

    compactor = IcebergCompactor(data_dir=data_dir)
    res = compactor.purge_user_gdpr("usr_purge_me")

    assert res["status"] == "purged_successfully"
    assert res["purged_records"] == 1
    assert res["total_after"] == 1
