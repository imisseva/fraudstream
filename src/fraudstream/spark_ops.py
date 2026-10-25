"""Reusable Spark parsing, quality, deduplication, and fraud scoring transformations."""

import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    collect_set,
    concat_ws,
    count,
    current_timestamp,
    date_format,
    expr,
    from_json,
    hour,
    isnan,
    lag,
    length,
    lit,
    lower,
    min as spark_min,
    max as spark_max,
    size,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

PIPELINE_VERSION = os.environ.get("FRAUDSTREAM_PIPELINE_VERSION", "0.1.0")
RUN_ID = os.environ.get("DATABRICKS_RUN_ID", "local")


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
    StructField("session_id", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
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
    StructField("referrer", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])


def parse_kafka_events(kafka_df: DataFrame, schema: StructType) -> DataFrame:
    """Parse Kafka JSON while retaining raw payload and source coordinates."""
    decoded = kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("_json"),
    ).withColumn(
        "_parsed",
        from_json(
            col("_json"), schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    fields = [col(f"_parsed.{field.name}").alias(field.name) for field in schema.fields]
    parsed = decoded.select(
        "kafka_key", "raw_payload", "kafka_topic", "kafka_partition", "kafka_offset",
        "kafka_timestamp", *fields,
    )
    return parsed.withColumn("schema_version", lit("1.0")) \
        .withColumn(
        "ingest_error",
        when(col("raw_payload").isNull(), "NULL_PAYLOAD")
        .when(col("_corrupt_record").isNotNull(), "CORRUPT_JSON_FIELD")
        .otherwise(None),
    )


def bronze_event_schema(event_schema: StructType) -> StructType:
    """Full Bronze schema, including raw Kafka fields and parsed event contract."""
    metadata = StructType([
        StructField("kafka_key", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("schema_version", StringType(), True),
        StructField("ingest_error", StringType(), True),
    ])
    event_fields = [field for field in event_schema.fields if field.name != "_corrupt_record"]
    return StructType(metadata.fields + event_fields + [StructField("_corrupt_record", StringType(), True)])


def normalize_event_time(df: DataFrame, source_column: str = "event_time") -> DataFrame:
    # Photon/ANSI can still raise on a mismatched to_timestamp format.  The
    # try_to_timestamp SQL function returns NULL instead, allowing the next
    # supported format to be attempted safely.
    def safe_timestamp(pattern: str):
        escaped = pattern.replace("'", "''")
        return expr(f"try_to_timestamp(`{source_column}`, '{escaped}')")

    return df.withColumn(
        "clean_event_time",
        coalesce(
            expr(f"try_to_timestamp(`{source_column}`)"),
            safe_timestamp("dd/MM/yyyy HH:mm"),
        ),
    )


def validate_transactions(df: DataFrame) -> DataFrame:
    normalized = normalize_event_time(df)
    return normalized.withColumn(
        "rejection_reason",
        when(col("ingest_error").isNotNull(), col("ingest_error"))
        .when(col("clean_event_time").isNull(), "MALFORMED_TIMESTAMP")
        .when(col("amount").isNull() | isnan(col("amount")) | (col("amount") <= 0), "INVALID_OR_NEGATIVE_AMOUNT")
        .when(col("event_id").isNull() | (length(trim(col("event_id"))) == 0), "MISSING_EVENT_ID")
        .when(col("transaction_id").isNull() | (length(trim(col("transaction_id"))) == 0), "MISSING_TRANSACTION_ID")
        .when(col("currency").isNull() | (length(trim(col("currency"))) == 0), "MISSING_CURRENCY")
        .when(col("user_id").isNull() | (length(trim(col("user_id"))) == 0), "MISSING_USER_ID")
        .when(col("card_id").isNull() | (length(trim(col("card_id"))) == 0), "MISSING_CARD_ID")
        .when(col("merchant_id").isNull() | (length(trim(col("merchant_id"))) == 0), "MISSING_MERCHANT_ID")
        .otherwise("VALID"),
    )


def clean_transactions(df: DataFrame) -> DataFrame:
    valid = validate_transactions(df).filter(col("rejection_reason") == "VALID")
    if valid.isStreaming:
        valid = valid.withWatermark("clean_event_time", "1 hour")
    return valid.dropDuplicates(["transaction_id", "clean_event_time"]) \
        .withColumn("status", lower(trim(col("status")))) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .withColumn("event_date", to_date(col("clean_event_time"))) \
        .withColumn("hour_of_day", hour(col("clean_event_time"))) \
        .withColumn("day_of_week", date_format(col("clean_event_time"), "E")) \
        .withColumn("is_weekend", when(col("day_of_week").isin("Sat", "Sun"), 1).otherwise(0)) \
        .withColumn("amount_tier", when(col("currency") != "VND", "NON_VND")
                    .when(col("amount") < 1000000, "LOW")
                    .when(col("amount") <= 10000000, "MEDIUM")
                    .when(col("amount") <= 20000000, "HIGH").otherwise("VERY_HIGH")) \
        .withColumn("silver_processed_at", current_timestamp()) \
        .withColumn("pipeline_version", lit(PIPELINE_VERSION)) \
        .withColumn("run_id", lit(RUN_ID)) \
        .withColumn("processing_time", current_timestamp())


def validate_clickstream(df: DataFrame) -> DataFrame:
    normalized = normalize_event_time(df)
    return normalized.withColumn(
        "rejection_reason",
        when(col("ingest_error").isNotNull(), col("ingest_error"))
        .when(col("clean_event_time").isNull(), "MALFORMED_TIMESTAMP")
        .when(col("event_id").isNull() | (length(trim(col("event_id"))) == 0), "MISSING_EVENT_ID")
        .when(col("session_id").isNull() | (length(trim(col("session_id"))) == 0), "MISSING_SESSION_ID")
        .when(col("user_id").isNull() | (length(trim(col("user_id"))) == 0), "MISSING_USER_ID")
        .otherwise("VALID"),
    )


def clean_clickstream(df: DataFrame) -> DataFrame:
    valid = validate_clickstream(df).filter(col("rejection_reason") == "VALID")
    if valid.isStreaming:
        valid = valid.withWatermark("clean_event_time", "1 hour")
    return valid.dropDuplicates(["event_id", "clean_event_time"]) \
        .withColumn("click_date", to_date(col("clean_event_time"))) \
        .withColumn("click_hour", hour(col("clean_event_time"))) \
        .withColumn("silver_processed_at", current_timestamp()) \
        .withColumn("pipeline_version", lit(PIPELINE_VERSION)) \
        .withColumn("run_id", lit(RUN_ID)) \
        .withColumn("processing_time", current_timestamp())


def session_profiles(click_df: DataFrame) -> DataFrame:
    return click_df.groupBy("session_id", "user_id").agg(
        spark_min("clean_event_time").alias("session_start_time"),
        spark_max("clean_event_time").alias("session_end_time"),
        spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        spark_sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
        spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
        spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_count"),
        spark_max("cart_total").alias("session_cart_total"),
    ).withColumn(
        "session_duration_sec",
        spark_abs(col("session_end_time").cast("long") - col("session_start_time").cast("long")),
    )


def score_transactions(
    txns: DataFrame,
    sessions: DataFrame | None = None,
    high_risk_merchant_ids: tuple[str, ...] | list[str] = (),
) -> DataFrame:
    """Apply documented VND fraud rules to a static transaction slice."""
    if sessions is not None:
        profile_cols = sessions.select(
            "session_id", "user_id", "session_duration_sec", "page_view_count", "product_view_count", "session_cart_total",
        ).dropDuplicates(["session_id", "user_id"])
        enriched = txns.join(profile_cols, ["session_id", "user_id"], "left")
    else:
        enriched = txns.withColumn("session_duration_sec", lit(None).cast("long")) \
            .withColumn("product_view_count", lit(None).cast("long")) \
            .withColumn("session_cart_total", lit(None).cast("double"))

    by_card = Window.partitionBy("card_id").orderBy(col("clean_event_time").cast("long"))
    card_5m = Window.partitionBy("card_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-300, 0)
    prior_failures = Window.partitionBy("card_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-180, -1)
    by_device = Window.partitionBy("device_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-600, 0)

    features = enriched.withColumn("previous_country", lag("country").over(by_card)) \
        .withColumn("previous_time", lag("clean_event_time").over(by_card)) \
        .withColumn("country_gap_seconds", spark_abs(col("clean_event_time").cast("long") - col("previous_time").cast("long"))) \
        .withColumn("card_txn_count_5m", count("transaction_id").over(card_5m)) \
        .withColumn("prior_failed_count_3m", spark_sum(when(upper(col("status")).isin("DECLINED", "FAILED"), 1).otherwise(0)).over(prior_failures)) \
        .withColumn("device_users_10m", size(collect_set("user_id").over(by_device)))

    vnd = upper(col("currency")) == "VND"
    high_value_condition = vnd & (col("amount") >= 20000000)
    if high_risk_merchant_ids:
        high_value_condition = high_value_condition & col("merchant_id").isin(*high_risk_merchant_ids)
    else:
        high_value_condition = lit(False)
    evaluated = features.withColumn("flag_scenario_a", when(high_value_condition, 1).otherwise(0)) \
        .withColumn("flag_scenario_b", when(col("card_txn_count_5m") >= 3, 1).otherwise(0)) \
        .withColumn("flag_scenario_c", when(col("previous_country").isNotNull() & (col("country") != col("previous_country")) & (col("country_gap_seconds") <= 900), 1).otherwise(0)) \
        .withColumn("flag_scenario_d", when(col("device_users_10m") >= 2, 1).otherwise(0)) \
        .withColumn("flag_scenario_e", when(col("prior_failed_count_3m") >= 2, 1).otherwise(0)) \
        .withColumn("flag_scenario_f", when((lower(col("channel")) == "web") & (
            col("session_id").isNull() | col("session_duration_sec").isNull() | (col("session_duration_sec") < 5) |
            col("product_view_count").isNull() | (col("product_view_count") == 0)
        ), 1).otherwise(0)) \
        .withColumn("flag_scenario_g", when(vnd & col("session_cart_total").isNotNull() & (spark_abs(col("amount") - col("session_cart_total")) > 2000000), 1).otherwise(0))

    scored = evaluated.withColumn(
        "fraud_risk_score",
        col("flag_scenario_a") * 35 + col("flag_scenario_b") * 30 + col("flag_scenario_c") * 40 +
        col("flag_scenario_d") * 25 + col("flag_scenario_e") * 20 + col("flag_scenario_f") * 35 + col("flag_scenario_g") * 30,
    ).withColumn(
        "detected_scenarios",
        concat_ws(", ",
            when(col("flag_scenario_a") == 1, "HIGH_VALUE_SPIKE"),
            when(col("flag_scenario_b") == 1, "VELOCITY_SPIKE"),
            when(col("flag_scenario_c") == 1, "GEOGRAPHIC_ANOMALY"),
            when(col("flag_scenario_d") == 1, "DEVICE_SHARING"),
            when(col("flag_scenario_e") == 1, "REPEATED_ATTEMPTS"),
            when(col("flag_scenario_f") == 1, "BOTNET_AUTOMATION"),
            when(col("flag_scenario_g") == 1, "CART_TAMPERING"),
        ),
    ).withColumn("is_predicted_fraud", when(col("fraud_risk_score") >= 20, 1).otherwise(0)) \
        .withColumn("pipeline_version", lit(PIPELINE_VERSION)) \
        .withColumn("run_id", lit(RUN_ID)) \
        .withColumn("processing_time", current_timestamp())
    return scored
