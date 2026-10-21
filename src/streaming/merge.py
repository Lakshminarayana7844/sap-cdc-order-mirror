"""Canonical SCD1 merge semantics for CDC events, in plain Python.

This module is the tested source of truth for how events land in the
mirror. The Spark job (mirror_job.py) applies the same rule with a
Delta Lake MERGE:

  1. For each order_id keep only the latest event by (op_ts, op_seq).
  2. I/U upsert the payload; D writes a tombstone (soft delete).
  3. Events older than the current mirror state are ignored, so a stale
     insert cannot resurrect a deleted order, but a genuinely newer
     insert can.

Tombstones are required: without them a late I/U for an already deleted
key would have nothing to compare against and would wrongly recreate
the row.
"""
from __future__ import annotations


def event_key(event: dict) -> tuple:
    return (event["op_ts"], event["op_seq"])


def latest_per_key(events: list[dict]) -> dict[str, dict]:
    """Keep the newest event per order_id by (op_ts, op_seq)."""
    latest: dict[str, dict] = {}
    for ev in events:
        cur = latest.get(ev["order_id"])
        if cur is None or event_key(ev) > event_key(cur):
            latest[ev["order_id"]] = ev
    return latest


def apply_events(current: dict[str, dict], events: list[dict]) -> dict[str, dict]:
    """Apply a batch of CDC events to the mirror state.

    `current` maps order_id -> row dict including op_ts/op_seq; deleted
    orders remain as tombstones {"_deleted": True, "op_ts", "op_seq"}.
    Returns the new mirror state (does not mutate `current`).
    """
    state = dict(current)
    for ev in events:
        oid = ev["order_id"]
        existing = state.get(oid)
        if existing is not None:
            if (ev["op_ts"], ev["op_seq"]) <= (existing["op_ts"], existing["op_seq"]):
                continue  # stale event, ignore
        if ev["op"] == "D":
            state[oid] = {"order_id": oid, "op_ts": ev["op_ts"],
                          "op_seq": ev["op_seq"], "_deleted": True}
        elif ev["op"] in ("I", "U"):
            row = dict(ev["payload"] or {})
            row["order_id"] = oid
            row["op_ts"] = ev["op_ts"]
            row["op_seq"] = ev["op_seq"]
            row["status"] = row.get("status", "CREATED")
            state[oid] = row
        else:
            raise ValueError(f"unknown op: {ev['op']!r}")
    return state


def visible_rows(state: dict[str, dict]) -> dict[str, dict]:
    """Mirror rows a consumer sees: tombstones filtered out."""
    return {k: v for k, v in state.items() if not v.get("_deleted")}
