"""Tests for deterministic 24-hour stream replay and cryptographic parity."""

import pyarrow as pa
import pyarrow.parquet as pq

from orchestration.deterministic_replay import DeterministicReplayer


def test_identical_streams_produce_matching_sha256(tmp_path):
    rows = [
        {"event_id": f"ev_{i}", "symbol": "BTC/USDT", "price": 65000.0 + i, "quantity": 1.0, "trade_side": "BUY", "user_id": f"u_{i}", "event_time": f"2026-11-18T10:00:0{i}Z"}
        for i in range(10)
    ]

    p_live = str(tmp_path / "live.parquet")
    p_shadow = str(tmp_path / "shadow.parquet")

    pq.write_table(pa.Table.from_pylist(rows), p_live)
    pq.write_table(pa.Table.from_pylist(rows), p_shadow)

    res = DeterministicReplayer.verify_replay_parity(p_live, p_shadow)
    assert res["parity_verified"] is True
    assert res["live_sha256"] == res["shadow_sha256"]
    assert res["live_row_count"] == 10
    assert res["status"] == "deterministic_parity_confirmed"


def test_divergent_streams_flag_checksum_mismatch(tmp_path):
    rows_live = [{"event_id": "ev_1", "symbol": "BTC/USDT", "price": 65000.0, "quantity": 1.0, "trade_side": "BUY", "user_id": "u_1", "event_time": "2026-11-18T10:00:00Z"}]
    rows_tampered = [{"event_id": "ev_1", "symbol": "BTC/USDT", "price": 99999.0, "quantity": 1.0, "trade_side": "BUY", "user_id": "u_1", "event_time": "2026-11-18T10:00:00Z"}]

    p_live = str(tmp_path / "live.parquet")
    p_tampered = str(tmp_path / "tampered.parquet")

    pq.write_table(pa.Table.from_pylist(rows_live), p_live)
    pq.write_table(pa.Table.from_pylist(rows_tampered), p_tampered)

    res = DeterministicReplayer.verify_replay_parity(p_live, p_tampered)
    assert res["parity_verified"] is False
    assert res["status"] == "hash_mismatch"
