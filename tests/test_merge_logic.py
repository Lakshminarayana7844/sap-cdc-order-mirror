"""Tests for the canonical CDC merge semantics."""
from src.streaming.merge import apply_events, latest_per_key, visible_rows

BASE_TS = "2026-09-16T08:00:00+00:00"


def ev(order_id, op, seq, ts=BASE_TS, **payload):
    return {"order_id": order_id, "op": op, "op_ts": ts, "op_seq": seq,
            "payload": payload if op != "D" else None}


def test_latest_per_key_keeps_newest():
    events = [
        ev("A", "I", 1, status="CREATED"),
        ev("A", "U", 3, status="RELEASED"),
        ev("A", "U", 2, status="CANCELLED"),
    ]
    latest = latest_per_key(events)
    assert latest["A"]["payload"]["status"] == "RELEASED"


def test_latest_per_key_breaks_ties_with_seq():
    events = [
        ev("A", "U", 1, status="RELEASED"),
        ev("A", "U", 2, status="COMPLETED"),
    ]
    assert latest_per_key(events)["A"]["payload"]["status"] == "COMPLETED"


def test_insert_then_update_wins():
    state = apply_events({}, [ev("A", "I", 1, status="CREATED"),
                              ev("A", "U", 2, status="RELEASED")])
    assert state["A"]["status"] == "RELEASED"


def test_delete_leaves_tombstone_and_hides_row():
    state = apply_events({}, [ev("A", "I", 1, status="CREATED"),
                              ev("A", "D", 5)])
    assert "A" not in visible_rows(state)
    assert state["A"]["_deleted"] is True


def test_stale_event_ignored():
    state = apply_events({}, [ev("A", "I", 5, status="RELEASED")])
    state = apply_events(state, [ev("A", "U", 2, status="CREATED")])
    assert state["A"]["status"] == "RELEASED"


def test_late_insert_after_delete_stays_deleted():
    state = apply_events({}, [ev("A", "I", 1, status="CREATED"),
                              ev("A", "D", 9)])
    state = apply_events(state, [ev("A", "I", 1, status="CREATED")])
    assert "A" not in visible_rows(state)


def test_newer_insert_after_delete_revives_row():
    state = apply_events({}, [ev("A", "I", 1, status="CREATED"),
                              ev("A", "D", 9)])
    state = apply_events(state, [ev("A", "I", 12, status="CREATED")])
    assert visible_rows(state)["A"]["status"] == "CREATED"


def test_unknown_op_raises():
    try:
        apply_events({}, [ev("A", "X", 1)])
    except ValueError:
        return
    raise AssertionError("expected ValueError")
