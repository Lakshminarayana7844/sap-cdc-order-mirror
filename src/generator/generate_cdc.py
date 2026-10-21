"""Generate SAP SLT/ODP-style CDC events from the seed orders CSV.

Emits JSON lines, one event per line:
  {"order_id": ..., "op": "I"|"U"|"D", "op_ts": ..., "op_seq": ...,
   "payload": {order fields}}

Waves:
  wave 1: an insert (I) for every seed order
  wave 2: updates (U) that move some orders CREATED -> RELEASED -> COMPLETED
  wave 3: deletes (D) for a few orders, plus one out-of-order event on
          purpose so consumers must order by (op_ts, op_seq)

Default output is a directory of JSON files (file transport). With
--kafka the same events are produced to a Kafka topic instead.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone

FIELDS = [
    "customer_id", "material", "sales_org", "distribution_channel",
    "division", "net_value", "currency", "order_date", "status",
]


def load_seed(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for row in rows:
        row["net_value"] = float(row["net_value"])
    return rows


def build_events(rows: list[dict], start: datetime, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    seq = 0

    def emit(order: dict, op: str, ts: datetime, payload: dict | None) -> None:
        nonlocal seq
        seq += 1
        events.append({
            "order_id": order["order_id"],
            "op": op,
            "op_ts": ts.isoformat(),
            "op_seq": seq,
            "payload": payload,
        })

    # wave 1: initial inserts
    for i, row in enumerate(rows):
        payload = {k: row[k] for k in FIELDS}
        emit(row, "I", start + timedelta(minutes=i), payload)

    t2 = start + timedelta(hours=2)
    # wave 2: status progression on every third order
    for i, row in enumerate(rows):
        if i % 3 == 0:
            payload = {k: row[k] for k in FIELDS}
            payload["status"] = "RELEASED"
            emit(row, "U", t2 + timedelta(minutes=i), payload)
        if i % 6 == 0:
            payload = {k: row[k] for k in FIELDS}
            payload["status"] = "COMPLETED"
            emit(row, "U", t2 + timedelta(hours=1, minutes=i), payload)

    # wave 3: deletes on two orders
    for row in rows[5:7]:
        emit(row, "D", t2 + timedelta(hours=3), None)

    # one deliberately late/out-of-order insert of a brand new order
    late = dict(rows[0])
    late["order_id"] = "500100200"
    payload = {k: late[k] for k in FIELDS}
    emit(late, "I", start + timedelta(minutes=5), payload)

    # simulate transport disorder: shuffle a middle slice
    mid = len(events) // 2
    window = events[mid:mid + 5]
    rng.shuffle(window)
    events[mid:mid + 5] = window
    return events


def write_files(events: list[dict], out_dir: str) -> int:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "cdc-events-000.json")
    with open(path, "w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return len(events)


def write_kafka(events: list[dict], bootstrap: str, topic: str) -> int:
    from kafka import KafkaProducer  # imported lazily: optional dependency

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for ev in events:
        producer.send(topic, ev)
    producer.flush()
    return len(events)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-csv", default="data/orders_seed.csv")
    parser.add_argument("--out", default="cdc_out")
    parser.add_argument("--kafka", action="store_true", help="produce to Kafka instead of files")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="sap-orders-cdc")
    args = parser.parse_args()

    rows = load_seed(args.seed_csv)
    events = build_events(rows, datetime(2026, 9, 16, 8, 0, tzinfo=timezone.utc))
    if args.kafka:
        n = write_kafka(events, args.bootstrap, args.topic)
        print(f"produced {n} events to {args.topic} on {args.bootstrap}")
    else:
        n = write_files(events, args.out)
        print(f"wrote {n} events to {args.out}/")


if __name__ == "__main__":
    main()
