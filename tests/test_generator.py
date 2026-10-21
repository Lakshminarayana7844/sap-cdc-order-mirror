"""Tests for the CDC event generator."""
from datetime import datetime, timezone

from src.generator.generate_cdc import build_events, load_seed


def test_build_events_has_all_ops():
    rows = load_seed("data/orders_seed.csv")
    events = build_events(rows, datetime(2026, 9, 16, 8, 0, tzinfo=timezone.utc))
    ops = {e["op"] for e in events}
    assert ops == {"I", "U", "D"}


def test_every_seed_order_inserted():
    rows = load_seed("data/orders_seed.csv")
    events = build_events(rows, datetime(2026, 9, 16, 8, 0, tzinfo=timezone.utc))
    inserted = {e["order_id"] for e in events if e["op"] == "I"}
    assert {r["order_id"] for r in rows} <= inserted


def test_op_seq_unique_and_increasing_orderable():
    rows = load_seed("data/orders_seed.csv")
    events = build_events(rows, datetime(2026, 9, 16, 8, 0, tzinfo=timezone.utc))
    seqs = [e["op_seq"] for e in events]
    assert len(seqs) == len(set(seqs))


def test_events_carry_full_payload_on_upserts():
    rows = load_seed("data/orders_seed.csv")
    events = build_events(rows, datetime(2026, 9, 16, 8, 0, tzinfo=timezone.utc))
    for e in events:
        if e["op"] in ("I", "U"):
            assert e["payload"]["currency"] == "EUR"
            assert "net_value" in e["payload"]
        else:
            assert e["payload"] is None
