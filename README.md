# sap-cdc-order-mirror

Near-real-time mirror of SAP SD sales orders from change data capture (CDC)
events, built with PySpark Structured Streaming and Delta Lake.

SAP systems publish changes through delta mechanisms (SLT triggers, ODP delta
queues, CDS-based extraction). This project models that contract: a stream of
`I`/`U`/`D` events with an operation timestamp and queue sequence, merged into
a Delta table so the mirror always shows the latest state of each order,
exactly the way a BW/4HANA aDSO or a Databricks bronze-to-silver pipeline
would consume it.

## What it does

1. **Generate** SAP-style orders and CDC events (`src/generator/`): initial
   inserts, status progressions (`CREATED -> RELEASED -> COMPLETED`), deletes,
   plus one deliberately late event and a shuffled transport slice.
2. **Stream** the events with PySpark Structured Streaming from files
   (default) or Kafka (`--kafka`), with a 10-minute event-time watermark.
3. **Merge** each micro-batch into a Delta table with an SCD1 upsert rule:
   newest `(op_ts, op_seq)` per `order_id` wins, stale events are ignored,
   deletes become tombstones (soft delete).

The merge rule lives in `src/streaming/merge.py` as plain Python (the tested
source of truth) and in `mirror_job.py` as the equivalent Delta `MERGE`.

## Architecture

```
orders_seed.csv -> generate_cdc.py -> CDC events (JSON)
                                        | files            | Kafka topic
                                        v                  v
                              mirror_job.py (Structured Streaming)
                                        |  MERGE: latest (op_ts, op_seq) wins
                                        v
                              Delta table: order mirror (SCD1)
```

## Quickstart

Local demo (needs Java and `pip install -r requirements.txt`):

```bash
make demo          # generate events, merge once, print the mirror
```

Run only the unit tests (pure Python, no Spark needed):

```bash
make test
```

Docker demo:

```bash
docker build -t sap-cdc-order-mirror .
docker run --rm sap-cdc-order-mirror
```

Kafka mode:

```bash
docker compose --profile kafka up -d kafka
python src/generator/generate_cdc.py --kafka --bootstrap localhost:9092
spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  src/streaming/mirror_job.py --kafka --bootstrap localhost:9092 --delta delta/orders --once
```

## Ordering and correctness guarantees

- **Event-time ordering**: the newest event per `order_id` by
  `(op_ts, op_seq)` wins. Late and out-of-order events are handled; the demo
  data includes both on purpose.
- **Deduplication**: `op_seq` is unique per event, so replays are idempotent.
- **Deletes**: a `D` writes a tombstone (`is_deleted = true`), so a stale
  late `I`/`U` is ignored by the ordering rule while a genuinely newer event
  can still revive the order. Consumers filter `is_deleted = false`.
- **Watermark**: 10 minutes in stream mode, so very late events are dropped
  from state but the available-now batch path still sees them in file order.

## Repository layout

```
data/orders_seed.csv         30 SAP-style sales orders
src/generator/generate_cdc.py  SLT/ODP-style event generator (files or Kafka)
src/streaming/merge.py         canonical merge semantics (pure Python, tested)
src/streaming/mirror_job.py    PySpark Structured Streaming + Delta MERGE job
tests/                         pytest suite for generator and merge semantics
scripts/run_demo.sh            end-to-end demo used by the Docker image
.github/workflows/ci.yml       CI: runs the unit tests on every push
```

## Honest limits

- The generator simulates CDC; it is not wired to a real SAP system. The
  event contract (op codes, timestamps, queue sequence) matches what SLT/ODP
  consumers receive.
- File transport is the default so the demo runs anywhere; Kafka is one flag
  away via docker compose.
- Runs on local Spark, not Databricks. The same merge maps directly onto a
  Delta Live Tables pipeline with `APPLY CHANGES INTO`.
- SCD1 only: the mirror keeps the latest state, no history table.
