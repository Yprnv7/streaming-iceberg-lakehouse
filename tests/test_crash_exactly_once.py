"""Tests for crash-recovery and exactly-once processing deduplication."""

import datetime

from producer.live_stream_ingestor import TradeEvent
from streaming_job.iceberg_stream_processor import IcebergStreamProcessor


def test_duplicate_events_are_deduplicated_across_batches(tmp_path):
    processor = IcebergStreamProcessor(warehouse_dir=str(tmp_path))
    now = datetime.datetime.now(datetime.timezone.utc)

    e1 = TradeEvent("ev_dup_1", "BTC/USDT", 65000.0, 1.0, "BUY", "usr_1", now, now)
    e2 = TradeEvent("ev_dup_2", "ETH/USDT", 3400.0, 2.0, "SELL", "usr_2", now, now)

    # Batch 1: in-flight events
    res1 = processor.process_micro_batch([e1, e2])
    assert res1.accepted_count == 2
    assert res1.duplicate_count == 0

    # Batch 2: simulated crash-replay injecting e1 again + new event e3
    e3 = TradeEvent("ev_dup_3", "SOL/USDT", 150.0, 10.0, "BUY", "usr_3", now, now)
    res2 = processor.process_micro_batch([e1, e3])

    assert res2.accepted_count == 1
    assert res2.duplicate_count == 1
    assert len(processor.seen_event_ids) == 3


def test_iceberg_snapshot_parent_lineage(tmp_path):
    processor = IcebergStreamProcessor(warehouse_dir=str(tmp_path))
    now = datetime.datetime.now(datetime.timezone.utc)

    e1 = TradeEvent("ev_snap_1", "BTC/USDT", 65000.0, 1.0, "BUY", "usr_1", now, now)
    e2 = TradeEvent("ev_snap_2", "ETH/USDT", 3400.0, 1.0, "SELL", "usr_2", now, now)

    processor.process_micro_batch([e1])
    processor.process_micro_batch([e2])

    assert len(processor.snapshots) == 2
    snap1 = processor.snapshots[0]
    snap2 = processor.snapshots[1]

    assert snap1.parent_snapshot_id is None
    assert snap2.parent_snapshot_id == snap1.snapshot_id
    assert snap2.sequence_number == 2
