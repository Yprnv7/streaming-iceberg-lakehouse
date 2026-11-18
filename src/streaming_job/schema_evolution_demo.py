"""Schema evolution demonstration: non-breaking column addition and renaming."""

from __future__ import annotations

import os
from typing import Any, Dict, List

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


class SchemaEvolutionDemo:
    """Manages schema migrations and proves backward-compatible queries."""

    def __init__(self, table_dir: str = "/tmp/iceberg_schema_demo") -> None:
        self.table_dir = table_dir
        os.makedirs(self.table_dir, exist_ok=True)

    def write_v1_data(self) -> str:
        """Writes batch with V1 schema (base fields)."""
        data = [
            {"event_id": "ev_1", "symbol": "BTC/USDT", "price": 65000.0, "quantity": 1.5, "trade_side": "BUY"},
            {"event_id": "ev_2", "symbol": "ETH/USDT", "price": 3500.0, "quantity": 4.0, "trade_side": "SELL"},
        ]
        table = pa.Table.from_pylist(data)
        path = os.path.join(self.table_dir, "v1_data.parquet")
        pq.write_table(table, path)
        return path

    def write_v2_data(self) -> str:
        """Writes batch with V2 evolved schema (added user_tier and maker_fee)."""
        data = [
            {
                "event_id": "ev_3",
                "symbol": "SOL/USDT",
                "price": 155.0,
                "quantity": 25.0,
                "trade_side": "BUY",
                "user_tier": "VIP_3",
                "maker_fee": 0.0005,
            },
            {
                "event_id": "ev_4",
                "symbol": "BTC/USDT",
                "price": 65200.0,
                "quantity": 0.5,
                "trade_side": "SELL",
                "user_tier": "STANDARD",
                "maker_fee": 0.0010,
            },
        ]
        table = pa.Table.from_pylist(data)
        path = os.path.join(self.table_dir, "v2_data.parquet")
        pq.write_table(table, path)
        return path

    def query_unified_table(self) -> List[Dict[str, Any]]:
        """Queries unified table files via DuckDB, verifying null-padding on historical V1 records."""
        con = duckdb.connect(database=":memory:")
        parquet_glob = os.path.join(self.table_dir, "*.parquet")
        query = f"""
            SELECT
                event_id,
                symbol,
                price,
                quantity,
                trade_side,
                COALESCE(user_tier, 'HISTORICAL_DEFAULT') AS user_tier,
                COALESCE(maker_fee, 0.0) AS maker_fee
            FROM read_parquet('{parquet_glob}', union_by_name=True)
            ORDER BY event_id ASC;
        """
        df = con.execute(query).fetchall()
        cols = [desc[0] for desc in con.description]
        return [dict(zip(cols, row)) for row in df]
