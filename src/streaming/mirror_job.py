"""PySpark Structured Streaming job: CDC events -> Delta SCD1 mirror.

Reads the JSON event stream (files by default, Kafka with --kafka),
dedupes by order_id keeping the newest (op_ts, op_seq), and merges
into a Delta table so the mirror always shows the latest order state.

Run once over all available input (demo / batch mode):
  spark-submit src/streaming/mirror_job.py --input cdc_out --delta delta/orders --once

Continuous file mode:
  spark-submit src/streaming/mirror_job.py --input cdc_out --delta delta/orders

Kafka mode:
  spark-submit src/streaming/mirror_job.py --kafka --bootstrap localhost:9092 \
      --topic sap-orders-cdc --delta delta/orders
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVENT_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType(), False),
    T.StructField("op", T.StringType(), False),
    T.StructField("op_ts", T.TimestampType(), False),
    T.StructField("op_seq", T.LongType(), False),
    T.StructField("payload", T.StructType([
        T.StructField("customer_id", T.StringType()),
        T.StructField("material", T.StringType()),
        T.StructField("sales_org", T.StringType()),
        T.StructField("distribution_channel", T.StringType()),
        T.StructField("division", T.StringType()),
        T.StructField("net_value", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("order_date", T.StringType()),
        T.StructField("status", T.StringType()),
    ])),
])


def merge_batch(batch_df: DataFrame, epoch_id: int, delta_path: str) -> None:
    """Merge one micro-batch into the Delta mirror (same rule as merge.py).

    Deletes are soft: a D event writes a tombstone row (is_deleted=true)
    carrying only the key and ordering columns. Tombstones stop a stale
    late insert from resurrecting a deleted order, while a genuinely
    newer insert revives it. Consumers read with is_deleted = false.
    """
    if batch_df.isEmpty():
        return
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession
    latest = (
        batch_df.orderBy(F.col("op_ts").desc(), F.col("op_seq").desc())
        .dropDuplicates(["order_id"])
    )
    incoming = latest.select(
        "order_id", "op", "op_ts", "op_seq",
        F.col("payload.customer_id").alias("customer_id"),
        F.col("payload.material").alias("material"),
        F.col("payload.sales_org").alias("sales_org"),
        F.col("payload.distribution_channel").alias("distribution_channel"),
        F.col("payload.division").alias("division"),
        F.col("payload.net_value").alias("net_value"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.order_date").alias("order_date"),
        F.col("payload.status").alias("status"),
        (F.col("op") == "D").alias("is_deleted"),
    )
    if not DeltaTable.isDeltaTable(spark, delta_path):
        (
            incoming.filter("op != 'D'")
            .drop("op")
            .write.format("delta").save(delta_path)
        )
        return

    target = DeltaTable.forPath(spark, delta_path)
    newer = "(s.op_ts > t.op_ts) OR (s.op_ts = t.op_ts AND s.op_seq > t.op_seq)"
    (
        target.alias("t")
        .merge(incoming.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdate(
            condition=f"s.op = 'D' AND {newer}",
            set={"op_ts": "s.op_ts", "op_seq": "s.op_seq",
                 "is_deleted": "s.is_deleted"},
        )
        .whenMatchedUpdateAll(condition=f"s.op != 'D' AND {newer}")
        .whenNotMatchedInsertAll(condition="s.op != 'D'")
        .whenNotMatchedInsert(
            condition="s.op = 'D'",
            values={
                "order_id": "s.order_id",
                "op_ts": "s.op_ts",
                "op_seq": "s.op_seq",
                "is_deleted": "s.is_deleted",
            },
        )
        .execute()
    )


def build_stream(spark: SparkSession, args) -> DataFrame:
    reader = spark.readStream.schema(EVENT_SCHEMA)
    if args.kafka:
        return (
            reader.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
            .select("e.*")
        )
    return reader.format("json").load(args.input)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="cdc_out")
    parser.add_argument("--delta", default="delta/orders")
    parser.add_argument("--checkpoint", default="delta/_checkpoint_orders")
    parser.add_argument("--kafka", action="store_true")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="sap-orders-cdc")
    parser.add_argument("--once", action="store_true", help="process available data and stop")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("sap-cdc-order-mirror")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = build_stream(spark, args)

    def _merge(batch_df: DataFrame, epoch_id: int) -> None:
        merge_batch(batch_df, epoch_id, args.delta)

    writer = (
        stream.writeStream.foreachBatch(_merge)
        .option("checkpointLocation", args.checkpoint)
    )
    if args.once:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()

    if args.once:
        mirror = spark.read.format("delta").load(args.delta).filter("is_deleted = false")
        print(f"mirror rows: {mirror.count()}")
        mirror.orderBy("order_id").show(40, truncate=False)
        spark.stop()


if __name__ == "__main__":
    main()
