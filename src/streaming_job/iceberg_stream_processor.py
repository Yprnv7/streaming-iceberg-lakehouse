"""Streaming processor with event-time watermarking and Apache Iceberg ACID snapshot commits."""

from __future__ import annotations

import datetime
import json
import os
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.parquet as pq

from producer.live_stream_ingestor import TradeEvent


@dataclass
class SnapshotMetadata:
    """Iceberg table snapshot metadata log."""

    snapshot_id: int
    parent_snapshot_id: Optional[int]
    sequence_number: int
    timestamp_ms: int
    manifest_list: str
    summary: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProcessResult:
    """Outcome of micro-batch streaming execution."""

    accepted_count: int
    late_dropped_count: int
    duplicate_count: int
    watermark: datetime.datetime
    current_snapshot_id: Optional[int]
    manifest_path: Optional[str]


class IcebergStreamProcessor:
    """Stateful stream processor enforcing 10-minute watermarks and Iceberg ACID commits."""

    def __init__(
        self,
        warehouse_dir: str = "/tmp/iceberg_warehouse",
        watermark_delay_minutes: float = 10.0,
    ) -> None:
        self.warehouse_dir = warehouse_dir
        self.watermark_delay = datetime.timedelta(minutes=watermark_delay_minutes)
        self.table_data_dir = os.path.join(self.warehouse_dir, "trades", "data")
        self.table_meta_dir = os.path.join(self.warehouse_dir, "trades", "metadata")

        os.makedirs(self.table_data_dir, exist_ok=True)
        os.makedirs(self.table_meta_dir, exist_ok=True)

        self.max_event_time: datetime.datetime = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        self.watermark: datetime.datetime = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        self.seen_event_ids: Set[str] = set()
        self.snapshots: List[SnapshotMetadata] = []
        self._current_sequence: int = 0

    def update_watermark(self, event_time: datetime.datetime) -> None:
        """Advances current watermark based on max event time observed."""
        if event_time > self.max_event_time:
            self.max_event_time = event_time
            self.watermark = self.max_event_time - self.watermark_delay

    def process_micro_batch(self, events: List[TradeEvent]) -> ProcessResult:
        """Processes micro-batch: filters late/duplicate events, writes Parquet, commits Iceberg snapshot."""
        accepted_events: List[TradeEvent] = []
        late_dropped: List[TradeEvent] = []
        duplicate_count = 0

        # Step 1: Update watermark from current batch
        for ev in events:
            self.update_watermark(ev.event_time)

        # Step 2: Validate watermarks and deduplicate
        for ev in events:
            if ev.event_id in self.seen_event_ids:
                duplicate_count += 1
                continue

            if ev.event_time < self.watermark:
                late_dropped.append(ev)
                continue

            self.seen_event_ids.add(ev.event_id)
            accepted_events.append(ev)

        if not accepted_events:
            return ProcessResult(
                accepted_count=0,
                late_dropped_count=len(late_dropped),
                duplicate_count=duplicate_count,
                watermark=self.watermark,
                current_snapshot_id=self.snapshots[-1].snapshot_id if self.snapshots else None,
                manifest_path=None,
            )

        # Step 3: Write Parquet data file
        file_id = uuid.uuid4().hex[:8]
        partition_date = accepted_events[0].event_time.strftime("%Y-%m-%d")
        part_dir = os.path.join(self.table_data_dir, f"dt={partition_date}")
        os.makedirs(part_dir, exist_ok=True)
        parquet_path = os.path.join(part_dir, f"trades_{file_id}.parquet")

        table = pa.Table.from_pylist([e.to_dict() for e in accepted_events])
        pq.write_table(table, parquet_path, compression="snappy")

        # Step 4: Commit Atomic Iceberg Snapshot
        self._current_sequence += 1
        snapshot_id = int(datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000)
        parent_id = self.snapshots[-1].snapshot_id if self.snapshots else None

        manifest_file = os.path.join(self.table_meta_dir, f"snap-{snapshot_id}.json")
        snap = SnapshotMetadata(
            snapshot_id=snapshot_id,
            parent_snapshot_id=parent_id,
            sequence_number=self._current_sequence,
            timestamp_ms=snapshot_id,
            manifest_list=manifest_file,
            summary={
                "operation": "append",
                "added-data-files": "1",
                "added-records": str(len(accepted_events)),
                "total-records": str(len(self.seen_event_ids)),
                "parquet-path": parquet_path,
            },
        )
        self.snapshots.append(snap)

        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump({
                "snapshot_id": snap.snapshot_id,
                "parent_snapshot_id": snap.parent_snapshot_id,
                "sequence_number": snap.sequence_number,
                "summary": snap.summary,
            }, f, indent=2)

        return ProcessResult(
            accepted_count=len(accepted_events),
            late_dropped_count=len(late_dropped),
            duplicate_count=duplicate_count,
            watermark=self.watermark,
            current_snapshot_id=snapshot_id,
            manifest_path=manifest_file,
        )
