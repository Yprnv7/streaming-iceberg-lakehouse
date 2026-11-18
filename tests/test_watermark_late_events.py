"""Tests for event-time watermarking and late-arriving event handling."""

import datetime

from hypothesis import given
from hypothesis import strategies as st

from producer.live_stream_ingestor import TradeEvent
from streaming_job.iceberg_stream_processor import IcebergStreamProcessor


def test_on_time_events_are_accepted(tmp_path):
    processor = IcebergStreamProcessor(warehouse_dir=str(tmp_path), watermark_delay_minutes=10.0)
    now = datetime.datetime.now(datetime.timezone.utc)

    events = [
        TradeEvent("ev_1", "BTC/USDT", 65000.0, 1.0, "BUY", "usr_1", now, now),
        TradeEvent("ev_2", "ETH/USDT", 3400.0, 2.0, "SELL", "usr_2", now, now),
    ]

    res = processor.process_micro_batch(events)
    assert res.accepted_count == 2
    assert res.late_dropped_count == 0
    assert res.current_snapshot_id is not None


def test_events_older_than_watermark_are_dropped(tmp_path):
    processor = IcebergStreamProcessor(warehouse_dir=str(tmp_path), watermark_delay_minutes=10.0)
    now = datetime.datetime.now(datetime.timezone.utc)

    # First event establishes watermark at now - 10m
    e1 = TradeEvent("ev_10", "BTC/USDT", 65000.0, 1.0, "BUY", "usr_1", now, now)
    processor.process_micro_batch([e1])

    # Late event 15 minutes in the past (< watermark)
    very_late_time = now - datetime.timedelta(minutes=15)
    e_late = TradeEvent("ev_late", "SOL/USDT", 150.0, 5.0, "BUY", "usr_3", very_late_time, now)

    # On-time event 5 minutes in the past (>= watermark)
    acceptable_late_time = now - datetime.timedelta(minutes=5)
    e_acceptable = TradeEvent("ev_ok", "ETH/USDT", 3400.0, 1.0, "SELL", "usr_4", acceptable_late_time, now)

    res = processor.process_micro_batch([e_late, e_acceptable])
    assert res.accepted_count == 1
    assert res.late_dropped_count == 1
    assert "ev_ok" in processor.seen_event_ids
    assert "ev_late" not in processor.seen_event_ids


@given(st.lists(st.integers(min_value=0, max_value=3600), min_size=5, max_size=30))
def test_hypothesis_watermark_monotonicity(offsets_sec):
    """Property test: Iceberg processor watermark is strictly monotonically non-decreasing."""
    processor = IcebergStreamProcessor(warehouse_dir="/tmp/iceberg_hypo", watermark_delay_minutes=10.0)
    base_time = datetime.datetime.now(datetime.timezone.utc)

    prev_watermark = processor.watermark

    for offset in offsets_sec:
        t = base_time + datetime.timedelta(seconds=offset)
        processor.update_watermark(t)
        assert processor.watermark >= prev_watermark
        prev_watermark = processor.watermark
