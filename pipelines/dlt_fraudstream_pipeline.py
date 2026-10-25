# ==============================================================================
# FraudStream: Modern Delta Live Tables (DLT) Declarative Pipeline
# Kiến trúc luồng Medallion (Bronze -> Silver -> Gold) sử dụng Delta Live Tables & DLT Expectations
# Tương thích Databricks DLT Pipelines (Serverless & Classic Compute)
# ==============================================================================

import dlt
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, hour, date_format,
    when, isnan, trim, length, current_timestamp, count, lower, upper,
    min as spark_min, max as spark_max, sum as spark_sum, first, abs as spark_abs,
    concat_ws, coalesce, lag
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------
# 1. SCHEMAS CONTRACTS
# ------------------------------------------------------------------------------
transaction_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("fraud_scenario", StringType(), True),
    StructField("session_id", StringType(), True)
])

clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("query", StringType(), True),
    StructField("results_count", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("cart_total", DoubleType(), True),
    StructField("referrer", StringType(), True)
])

# ------------------------------------------------------------------------------
# 2. BRONZE LAYER: INGESTION FROM KAFKA CLOUD
# ------------------------------------------------------------------------------
@dlt.table(
    name="dlt_bronze_transactions",
    comment="Raw streaming financial transactions from Kafka topic transaction-events",
    table_properties={"quality": "bronze"}
)
def dlt_bronze_transactions():
    return spark.readStream.table("bronze_transactions")


@dlt.table(
    name="dlt_bronze_clickstream",
    comment="Raw streaming browsing clickstream events from Kafka topic clickstream-events",
    table_properties={"quality": "bronze"}
)
def dlt_bronze_clickstream():
    return spark.readStream.table("bronze_clickstream")


# ------------------------------------------------------------------------------
# 3. SILVER LAYER: DATA QUALITY WITH DLT EXPECTATIONS & DLQ
# ------------------------------------------------------------------------------
@dlt.table(
    name="dlt_silver_transactions",
    comment="Clean, enriched and validated transactions with watermark deduplication",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL AND amount > 0")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL AND length(trim(user_id)) > 0")
@dlt.expect_or_drop("valid_card_id", "card_id IS NOT NULL AND length(trim(card_id)) > 0")
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL AND length(trim(merchant_id)) > 0")
@dlt.expect_or_drop("valid_timestamp", "clean_event_time IS NOT NULL")
def dlt_silver_transactions():
    return dlt.read_stream("dlt_bronze_transactions") \
        .withColumn(
            "clean_event_time",
            coalesce(
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("event_time"), "dd/MM/yyyy HH:mm"),
                to_timestamp(col("event_time"))
            )
        ) \
        .withWatermark("clean_event_time", "1 hour") \
        .dropDuplicates(["transaction_id", "clean_event_time"]) \
        .withColumn("status", lower(trim(col("status")))) \
        .withColumn("event_date", to_date(col("clean_event_time"))) \
        .withColumn("hour_of_day", hour(col("clean_event_time"))) \
        .withColumn("day_of_week", date_format(col("clean_event_time"), "E")) \
        .withColumn("is_weekend", when(col("day_of_week").isin("Sat", "Sun"), 1).otherwise(0)) \
        .withColumn(
            "amount_tier",
            when(col("amount") < 1000000, "LOW")
            .when(col("amount") <= 10000000, "MEDIUM")
            .when(col("amount") <= 20000000, "HIGH")
            .otherwise("VERY_HIGH")
        )


@dlt.table(
    name="dlt_silver_clickstream",
    comment="Clean clickstream user journeys with watermark deduplication",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_session", "session_id IS NOT NULL AND user_id IS NOT NULL")
def dlt_silver_clickstream():
    return dlt.read_stream("dlt_bronze_clickstream") \
        .withColumn("clean_event_time", to_timestamp(col("event_time"))) \
        .withWatermark("clean_event_time", "1 hour") \
        .dropDuplicates(["event_id", "clean_event_time"])


@dlt.table(
    name="dlt_silver_session_profiles",
    comment="Aggregated user browsing session metrics for botnet and cart correlation",
    table_properties={"quality": "silver"}
)
def dlt_silver_session_profiles():
    return dlt.read("dlt_silver_clickstream") \
        .groupBy("session_id", "user_id") \
        .agg(
            spark_min("clean_event_time").alias("session_start_time"),
            spark_max("clean_event_time").alias("session_end_time"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            spark_sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
            spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_count"),
            spark_max("cart_total").alias("session_cart_total"),
            first("ip_address").alias("session_ip"),
            first("device_id").alias("session_device_id"),
            first("city").alias("session_city")
        ) \
        .withColumn(
            "session_duration_sec",
            spark_abs(col("session_end_time").cast("long") - col("session_start_time").cast("long"))
        )


# ------------------------------------------------------------------------------
# 4. GOLD LAYER: 7-SCENARIOS REAL-TIME FRAUD DETECTION ENGINE
# ------------------------------------------------------------------------------
@dlt.table(
    name="dlt_gold_fraud_alerts",
    comment="Real-time multi-scenario financial and botnet fraud alerts",
    table_properties={"quality": "gold"}
)
def dlt_gold_fraud_alerts():
    txns = dlt.read("dlt_silver_transactions")
    sessions = dlt.read("dlt_silver_session_profiles")

    enriched = txns.join(sessions, on="session_id", how="left")

    user_window = Window.partitionBy("user_id").orderBy(col("clean_event_time").cast("long"))
    user_range_5min = Window.partitionBy("user_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-300, 0)
    device_window = Window.partitionBy("device_id").orderBy(col("clean_event_time").cast("long"))
    device_range_10min = Window.partitionBy("device_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-600, 0)
    card_range_3min = Window.partitionBy("card_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-180, 0)

    features = enriched \
        .withColumn("prev_country", lag("country", 1).over(user_window)) \
        .withColumn("prev_event_time", lag("clean_event_time", 1).over(user_window)) \
        .withColumn(
            "time_diff_minutes_prev_country",
            spark_abs(col("clean_event_time").cast("long") - col("prev_event_time").cast("long")) / 60
        ) \
        .withColumn("user_txn_count_5m", count("transaction_id").over(user_range_5min)) \
        .withColumn("prev_device_user", lag("user_id", 1).over(device_window)) \
        .withColumn("prev_device_time", lag("clean_event_time", 1).over(device_window)) \
        .withColumn(
            "device_user_switch_sec",
            spark_abs(col("clean_event_time").cast("long") - col("prev_device_time").cast("long"))
        ) \
        .withColumn(
            "is_device_user_switch",
            when(
                (col("prev_device_user").isNotNull()) &
                (col("prev_device_user") != col("user_id")) &
                (col("device_user_switch_sec") <= 600),
                1
            ).otherwise(0)
        ) \
        .withColumn("device_user_switches_10m", spark_sum("is_device_user_switch").over(device_range_10min)) \
        .withColumn(
            "card_failed_count_3m",
            spark_sum(when(upper(col("status")).isin("DECLINED", "FAILED"), 1).otherwise(0)).over(card_range_3min)
        )

    evaluated = features \
        .withColumn("flag_scenario_a", when(col("amount") >= 20000000, 1).otherwise(0)) \
        .withColumn("flag_scenario_b", when(col("user_txn_count_5m") >= 3, 1).otherwise(0)) \
        .withColumn(
            "flag_scenario_c",
            when(
                (col("prev_country").isNotNull()) &
                (col("country") != col("prev_country")) &
                (col("time_diff_minutes_prev_country") <= 15),
                1
            ).otherwise(0)
        ) \
        .withColumn("flag_scenario_d", when(col("device_user_switches_10m") >= 1, 1).otherwise(0)) \
        .withColumn("flag_scenario_e", when(col("card_failed_count_3m") >= 2, 1).otherwise(0)) \
        .withColumn(
            "flag_scenario_f",
            when(
                (col("channel") == "web") & (
                    col("session_id").isNull() |
                    (col("session_duration_sec").isNotNull() & (col("session_duration_sec") < 5)) |
                    (col("product_view_count").isNotNull() & (col("product_view_count") == 0))
                ),
                1
            ).otherwise(0)
        ) \
        .withColumn(
            "flag_scenario_g",
            when(
                (col("session_cart_total").isNotNull()) &
                (spark_abs(col("amount") - col("session_cart_total")) > 2000000),
                1
            ).otherwise(0)
        )

    scored = evaluated.withColumn(
        "fraud_risk_score",
        (col("flag_scenario_a") * 35) +
        (col("flag_scenario_b") * 30) +
        (col("flag_scenario_c") * 40) +
        (col("flag_scenario_d") * 25) +
        (col("flag_scenario_e") * 20) +
        (col("flag_scenario_f") * 35) +
        (col("flag_scenario_g") * 30)
    ).withColumn(
        "detected_scenarios",
        concat_ws(", ",
            when(col("flag_scenario_a") == 1, "HIGH_VALUE_SPIKE").otherwise(None),
            when(col("flag_scenario_b") == 1, "VELOCITY_SPIKE").otherwise(None),
            when(col("flag_scenario_c") == 1, "GEOGRAPHIC_ANOMALY").otherwise(None),
            when(col("flag_scenario_d") == 1, "DEVICE_SHARING").otherwise(None),
            when(col("flag_scenario_e") == 1, "REPEATED_ATTEMPTS").otherwise(None),
            when(col("flag_scenario_f") == 1, "BOTNET_AUTOMATION").otherwise(None),
            when(col("flag_scenario_g") == 1, "CART_TAMPERING").otherwise(None)
        )
    ).withColumn(
        "is_predicted_fraud",
        when(col("fraud_risk_score") >= 20, 1).otherwise(0)
    )

    return scored.filter(col("is_predicted_fraud") == 1) \
        .select(
            col("event_id").alias("alert_id"),
            col("transaction_id"),
            col("user_id"),
            col("card_id"),
            col("merchant_id"),
            col("amount"),
            col("currency"),
            col("country"),
            col("clean_event_time").alias("event_time"),
            col("fraud_risk_score"),
            col("detected_scenarios"),
            col("session_id"),
            col("session_duration_sec"),
            col("is_fraud").alias("ground_truth_is_fraud"),
            col("fraud_scenario").alias("ground_truth_scenario"),
            current_timestamp().alias("alert_generated_at")
        )

