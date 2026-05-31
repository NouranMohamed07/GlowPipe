"""
GlowPipe — Spark Structured Streaming Job (Enhanced)
=====================================================
Answers 5 business questions that CANNOT be answered by batch:

  Q1. Which products are going viral RIGHT NOW? (5-min trending window)
      → trending_now table, updated every 30 seconds

  Q2. Is a product causing a real-time allergen outbreak?
      → allergen_alerts table, updated every 10 seconds
      → fires when 3+ users report same product unsafe within 5 minutes

  Q3. What is the live session conversion funnel?
      → session_funnel table: view → cart → purchase rates per skin type
      → batch can only show yesterday's funnel; this shows the last 10 min

  Q4. Which price points are causing abandonment RIGHT NOW?
      → price_abandonment table: avg budget vs avg price by skin type
      → retailers can respond within the hour, not the next morning

  Q5. Which ingredients are spiking across multiple products simultaneously?
      → ingredient_spikes table: cross-product ingredient concern signals
      → impossible with batch — requires seeing the pattern as it emerges

Run inside spark-master container:
  spark-submit \\
    --master spark://spark-master:7077 \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,\\
               org.postgresql:postgresql:42.6.0 \\
    /opt/airflow/streaming/spark_jobs/stream_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    sum as _sum, count, avg, max as _max, min as _min,
    when, lit, array_contains, explode, current_timestamp,
    round as _round,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, BooleanType,
    ArrayType, TimestampType,
)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_URL          = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://streaming_postgres:5432/glowpipe_streaming",
)
PG_USER     = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
CHECKPOINT  = "/tmp/glowpipe_checkpoints"

PG_PROPS = {
    "user":     PG_USER,
    "password": PG_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Schemas ────────────────────────────────────────────────────────────────────

USER_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),           True),
    StructField("session_id",     StringType(),           True),
    StructField("event_type",     StringType(),           True),
    StructField("product_id",     StringType(),           True),
    StructField("user_id",        StringType(),           True),
    StructField("skin_type",      StringType(),           True),
    StructField("skin_concerns",  ArrayType(StringType()), True),
    StructField("category",       StringType(),           True),
    StructField("price",          FloatType(),            True),
    StructField("budget",         FloatType(),            True),
    StructField("over_budget",    BooleanType(),          True),
    StructField("rating",         FloatType(),            True),
    StructField("source",         StringType(),           True),
    StructField("timestamp",      StringType(),           True),
])

SAFETY_EVENT_SCHEMA = StructType([
    StructField("event_id",              StringType(),           True),
    StructField("product_id",            StringType(),           True),
    StructField("skin_type",             StringType(),           True),
    StructField("compatibility_score",   IntegerType(),          True),
    StructField("allergens_detected",    ArrayType(StringType()), True),
    StructField("allergen_count",        IntegerType(),          True),
    StructField("is_safe",               BooleanType(),          True),
    StructField("overall_safety_score",  IntegerType(),          True),
    StructField("pregnancy_safe",        BooleanType(),          True),
    StructField("fungal_acne_safe",      BooleanType(),          True),
    StructField("timestamp",             StringType(),           True),
])

INGREDIENT_ALERT_SCHEMA = StructType([
    StructField("alert_id",          StringType(),           True),
    StructField("ingredient_name",   StringType(),           True),
    StructField("concern_level",     StringType(),           True),
    StructField("affected_products", ArrayType(StringType()), True),
    StructField("affected_count",    IntegerType(),          True),
    StructField("skin_types_at_risk", ArrayType(StringType()), True),
    StructField("signal_source",     StringType(),           True),
    StructField("timestamp",         StringType(),           True),
])

# ── Spark session ──────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("GlowPipe-Streaming-Enhanced")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[stream_job] Spark session ready")

# ── Kafka readers ──────────────────────────────────────────────────────────────

def read_kafka(topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    )


def write_pg(batch_df, batch_id, table):
    if batch_df.isEmpty():
        return
    n = batch_df.count()
    batch_df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)
    print(f"[stream_job] batch {batch_id}: {n} rows → {table}")


# ── Parse streams ──────────────────────────────────────────────────────────────

parsed_user = (
    read_kafka("user_events")
    .select(from_json(col("json_str"), USER_EVENT_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("event_timestamp", to_timestamp(col("timestamp")))
    .drop("timestamp")
    .filter(col("event_id").isNotNull())
    .filter(col("product_id").isNotNull())
)

parsed_safety = (
    read_kafka("product_safety")
    .select(from_json(col("json_str"), SAFETY_EVENT_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("alert_timestamp", to_timestamp(col("timestamp")))
    .drop("timestamp")
    .filter(col("event_id").isNotNull())
)

parsed_ingredients = (
    read_kafka("ingredient_alerts")
    .select(from_json(col("json_str"), INGREDIENT_ALERT_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("spike_timestamp", to_timestamp(col("timestamp")))
    .drop("timestamp")
    .filter(col("alert_id").isNotNull())
)


# ══════════════════════════════════════════════════════════════════════════════
# Q1 — Which products are trending RIGHT NOW?  (5-min window)
# Business value: retailers can restock or promote trending items within hours,
# not the next morning after the batch DAG runs.
# ══════════════════════════════════════════════════════════════════════════════

trending = (
    parsed_user
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("product_id"),
        col("skin_type"),
        col("category"),
    )
    .agg(
        count("*").alias("event_count"),
        _sum(when(col("event_type") == "view",        1).otherwise(0)).alias("view_count"),
        _sum(when(col("event_type") == "purchase",    1).otherwise(0)).alias("purchase_count"),
        _sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_count"),
        _sum(when(col("event_type") == "abandon",     1).otherwise(0)).alias("abandon_count"),
        avg(when(col("event_type") == "feedback", col("rating"))).alias("avg_rating"),
        avg(col("price")).alias("avg_price"),
    )
    .withColumn(
        "trend_score",
        # purchases are strong signal; views are weak; abandons penalise
        col("purchase_count") * 5.0
        + col("cart_count")    * 3.0
        + col("view_count")    * 1.0
        - col("abandon_count") * 2.0
    )
)

query_trending = (
    trending.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "product_id", "skin_type", "category",
            "event_count", "view_count", "purchase_count",
            "cart_count", "abandon_count",
            _round(col("avg_rating"), 2).alias("avg_rating"),
            _round(col("avg_price"),  2).alias("avg_price"),
            _round(col("trend_score"), 1).alias("trend_score"),
        ),
        bid, "trending_now"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/trending")
    .trigger(processingTime="30 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════════
# Q2 — Real-time allergen outbreak detection
# Business value: if 3+ sensitive-skin users flag the same product unsafe
# within 5 minutes → fire an outbreak alert. Batch would catch this 23h later.
# ══════════════════════════════════════════════════════════════════════════════

# Raw allergen events (every unsafe check)
query_allergen_raw = (
    parsed_safety
    .filter(
        (col("is_safe") == False) |
        (col("allergen_count") > 0) |
        (col("compatibility_score") < 60)
    )
    .writeStream
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            "event_id", "product_id", "skin_type",
            "compatibility_score", "allergens_detected", "allergen_count",
            "is_safe", "overall_safety_score",
            "pregnancy_safe", "fungal_acne_safe", "alert_timestamp",
        ),
        bid, "allergen_alerts"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/allergen_raw")
    .trigger(processingTime="10 seconds")
    .start()
)

# Outbreak aggregation: 5-min window, same product, count unsafe reports
outbreak = (
    parsed_safety
    .filter(col("is_safe") == False)
    .withWatermark("alert_timestamp", "2 minutes")
    .groupBy(
        window(col("alert_timestamp"), "5 minutes"),
        col("product_id"),
        col("skin_type"),
    )
    .agg(
        count("*").alias("unsafe_report_count"),
        avg(col("compatibility_score")).alias("avg_compat_score"),
        avg(col("allergen_count")).alias("avg_allergen_count"),
    )
    # Only surface if 3+ users reported it — noise filter
    .filter(col("unsafe_report_count") >= 3)
    .withColumn("outbreak_severity",
        when(col("unsafe_report_count") >= 10, "critical")
        .when(col("unsafe_report_count") >= 5,  "high")
        .otherwise("medium")
    )
)

query_outbreak = (
    outbreak.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "product_id", "skin_type",
            "unsafe_report_count",
            _round(col("avg_compat_score"),  1).alias("avg_compat_score"),
            _round(col("avg_allergen_count"), 1).alias("avg_allergen_count"),
            "outbreak_severity",
        ),
        bid, "allergen_outbreaks"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/outbreak")
    .trigger(processingTime="30 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════════
# Q3 — Live session conversion funnel
# Business value: "right now, oily skin users are adding to cart but not
# purchasing — is the checkout broken or is the price too high?"
# Batch tells you yesterday's funnel. Streaming tells you the last 10 minutes.
# ══════════════════════════════════════════════════════════════════════════════

funnel = (
    parsed_user
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "10 minutes"),
        col("skin_type"),
        col("category"),
    )
    .agg(
        _sum(when(col("event_type") == "view",        1).otherwise(0)).alias("views"),
        _sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
        _sum(when(col("event_type") == "purchase",    1).otherwise(0)).alias("purchases"),
        _sum(when(col("event_type") == "abandon",     1).otherwise(0)).alias("abandons"),
        _sum(when(col("event_type") == "feedback",    1).otherwise(0)).alias("reviews"),
    )
    .withColumn(
        "cart_rate",
        when(col("views") > 0,
             _round(col("cart_adds") / col("views") * 100, 1)
        ).otherwise(lit(0.0))
    )
    .withColumn(
        "purchase_rate",
        when(col("cart_adds") > 0,
             _round(col("purchases") / col("cart_adds") * 100, 1)
        ).otherwise(lit(0.0))
    )
    .withColumn(
        "abandon_rate",
        when(col("cart_adds") > 0,
             _round(col("abandons") / col("cart_adds") * 100, 1)
        ).otherwise(lit(0.0))
    )
)

query_funnel = (
    funnel.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "skin_type", "category",
            "views", "cart_adds", "purchases", "abandons", "reviews",
            "cart_rate", "purchase_rate", "abandon_rate",
        ),
        bid, "session_funnel"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/funnel")
    .trigger(processingTime="30 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════════
# Q4 — Price abandonment signal
# Business value: "sensitive skin users are abandoning products above £80 —
# our premium serums are priced out of our most loyal segment."
# A retailer can react within the hour with a targeted discount or bundle.
# ══════════════════════════════════════════════════════════════════════════════

price_signal = (
    parsed_user
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(
        window(col("event_timestamp"), "10 minutes"),
        col("skin_type"),
        col("category"),
    )
    .agg(
        avg(col("price")).alias("avg_price"),
        avg(col("budget")).alias("avg_budget"),
        _sum(when(col("over_budget") == True,  1).otherwise(0)).alias("over_budget_count"),
        _sum(when(col("over_budget") == False, 1).otherwise(0)).alias("in_budget_count"),
        avg(when(col("event_type") == "abandon", col("price"))).alias("avg_abandon_price"),
        avg(when(col("event_type") == "purchase", col("price"))).alias("avg_purchase_price"),
    )
    .withColumn(
        "price_pressure",
        # ratio of over-budget events — high means the segment is price-sensitive now
        when(
            (col("over_budget_count") + col("in_budget_count")) > 0,
            _round(
                col("over_budget_count") /
                (col("over_budget_count") + col("in_budget_count")) * 100,
                1
            )
        ).otherwise(lit(0.0))
    )
)

query_price = (
    price_signal.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "skin_type", "category",
            _round(col("avg_price"),          2).alias("avg_price"),
            _round(col("avg_budget"),         2).alias("avg_budget"),
            "over_budget_count", "in_budget_count",
            _round(col("avg_abandon_price"),  2).alias("avg_abandon_price"),
            _round(col("avg_purchase_price"), 2).alias("avg_purchase_price"),
            "price_pressure",
        ),
        bid, "price_abandonment"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/price")
    .trigger(processingTime="30 seconds")
    .start()
)


# ══════════════════════════════════════════════════════════════════════════════
# Q5 — Cross-product ingredient concern spike
# Business value: "Phenoxyethanol just appeared as a concern in 6 products
# in the last 5 minutes — is there a new study or recall we should act on?"
# This pattern is invisible to batch because it only emerges in a short window.
# ══════════════════════════════════════════════════════════════════════════════

ingredient_spikes = (
    parsed_ingredients
    .withWatermark("spike_timestamp", "2 minutes")
    .groupBy(
        window(col("spike_timestamp"), "5 minutes"),
        col("ingredient_name"),
        col("concern_level"),
    )
    .agg(
        count("*").alias("signal_count"),
        _sum(col("affected_count")).alias("total_affected_products"),
    )
    # Only surface high-frequency signals
    .filter(col("signal_count") >= 2)
)

query_ingredients = (
    ingredient_spikes.writeStream
    .outputMode("update")
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "ingredient_name", "concern_level",
            "signal_count", "total_affected_products",
        ),
        bid, "ingredient_spikes"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/ingredients")
    .trigger(processingTime="30 seconds")
    .start()
)


# ── Raw live_events (always write everything) ──────────────────────────────────

query_live = (
    parsed_user.writeStream
    .foreachBatch(lambda df, bid: write_pg(
        df.select(
            "event_id", "session_id", "event_type", "product_id",
            "user_id", "skin_type", "category",
            "price", "budget", "over_budget",
            "rating", "source", "event_timestamp",
        ).dropDuplicates(["event_id"]),
        bid, "live_events"
    ))
    .option("checkpointLocation", f"{CHECKPOINT}/live_events")
    .trigger(processingTime="10 seconds")
    .start()
)


# ── Keep alive ─────────────────────────────────────────────────────────────────

print("\n[stream_job] 7 streaming queries active:")
print("  Q1 → trending_now        (which products are viral right now?)")
print("  Q2 → allergen_alerts     (raw unsafe product checks)")
print("  Q2 → allergen_outbreaks  (3+ reports = outbreak alert)")
print("  Q3 → session_funnel      (live view→cart→purchase rates)")
print("  Q4 → price_abandonment   (which price points are losing customers?)")
print("  Q5 → ingredient_spikes   (cross-product ingredient concern patterns)")
print("     → live_events         (raw event log)")

spark.streams.awaitAnyTermination()
