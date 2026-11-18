"""Tests for non-breaking Iceberg schema evolution."""

from streaming_job.schema_evolution_demo import SchemaEvolutionDemo


def test_schema_evolution_unification(tmp_path):
    demo = SchemaEvolutionDemo(table_dir=str(tmp_path))

    # Write V1 schema
    demo.write_v1_data()

    # Write V2 evolved schema
    demo.write_v2_data()

    # Execute unified query
    rows = demo.query_unified_table()
    assert len(rows) == 4

    # Assert historical V1 records have default/null-fill values
    ev1 = next(r for r in rows if r["event_id"] == "ev_1")
    assert ev1["user_tier"] == "HISTORICAL_DEFAULT"
    assert ev1["maker_fee"] == 0.0

    # Assert V2 records have new columns populated
    ev3 = next(r for r in rows if r["event_id"] == "ev_3")
    assert ev3["user_tier"] == "VIP_3"
    assert ev3["maker_fee"] == 0.0005
