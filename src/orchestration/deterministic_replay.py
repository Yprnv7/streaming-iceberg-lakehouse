"""Deterministic event stream replay and cryptographic state hash verification."""

from __future__ import annotations

import hashlib
from typing import Any, Dict

import duckdb


class DeterministicReplayer:
    """Verifies that replaying raw event topics produces bit-for-bit identical lakehouse tables."""

    @staticmethod
    def compute_table_checksum(parquet_path: str) -> str:
        """Computes deterministic cumulative SHA-256 hash over sorted rows in Parquet table."""
        con = duckdb.connect(database=":memory:")
        rows = con.execute(f"""
            SELECT event_id, symbol, price, quantity, trade_side, user_id, event_time
            FROM read_parquet('{parquet_path}')
            ORDER BY event_id ASC;
        """).fetchall()

        hasher = hashlib.sha256()
        for r in rows:
            row_str = "|".join(str(v) for v in r)
            hasher.update(row_str.encode("utf-8"))

        return hasher.hexdigest()

    @classmethod
    def verify_replay_parity(cls, live_parquet_path: str, shadow_parquet_path: str) -> Dict[str, Any]:
        """Compares live and replayed shadow tables asserting exact checksum parity."""
        live_hash = cls.compute_table_checksum(live_parquet_path)
        shadow_hash = cls.compute_table_checksum(shadow_parquet_path)

        match = (live_hash == shadow_hash)

        con = duckdb.connect(database=":memory:")
        r_live = con.execute(f"SELECT COUNT(*) FROM read_parquet('{live_parquet_path}');").fetchone()
        r_shadow = con.execute(f"SELECT COUNT(*) FROM read_parquet('{shadow_parquet_path}');").fetchone()
        live_count = r_live[0] if r_live else 0
        shadow_count = r_shadow[0] if r_shadow else 0

        return {
            "parity_verified": match,
            "live_sha256": live_hash,
            "shadow_sha256": shadow_hash,
            "live_row_count": live_count,
            "shadow_row_count": shadow_count,
            "status": "deterministic_parity_confirmed" if match else "hash_mismatch",
        }
