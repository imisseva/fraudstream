"""Local Spark tests for the shared quality and fraud-rule transformations."""

from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from fraudstream.spark_ops import (
    clean_transactions,
    parse_kafka_events,
    score_transactions,
    transaction_schema,
    validate_transactions,
)


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[2]").appName("fraudstream-tests") \
        .config("spark.ui.enabled", "false").getOrCreate()
    yield session
    session.stop()


def event(event_id, txn_id, card, user, device, at, *, country="VN", amount=100_000.0,
          currency="VND", status="success", channel="pos", session_id=None, fraud=0, merchant="m1"):
    return {
        "event_id": event_id, "transaction_id": txn_id, "card_id": card, "user_id": user,
        "device_id": device, "clean_event_time": at, "country": country, "amount": amount,
        "currency": currency, "status": status, "channel": channel, "session_id": session_id,
        "merchant_id": merchant, "is_fraud": fraud, "fraud_scenario": None,
    }


@pytest.fixture
def spark_events(spark):
    base = datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        event("v1", "v1", "velocity", "u1", "d1", base),
        event("v2", "v2", "velocity", "u1", "d1", base + timedelta(minutes=2)),
        event("v3", "v3", "velocity", "u1", "d1", base + timedelta(minutes=5)),
        event("g1", "g1", "geo", "u2", "d2", base, country="VN"),
        event("g2", "g2", "geo", "u2", "d2", base + timedelta(minutes=15), country="TH"),
        event("d1", "d1", "device-a", "u3", "shared", base),
        event("d2", "d2", "device-b", "u4", "shared", base + timedelta(minutes=10)),
        event("e1", "e1", "retry", "u5", "d5", base, status="failed"),
        event("e2", "e2", "retry", "u5", "d5", base + timedelta(seconds=30), status="declined"),
        event("e3", "e3", "retry", "u5", "d5", base + timedelta(seconds=60), status="success"),
        event("a1", "a1", "large-vnd", "u6", "d6", base, amount=20_000_000),
        event("a3", "a3", "large-normal", "u10", "d10", base, amount=25_000_000, merchant="low-risk"),
        event("a2", "a2", "large-usd", "u7", "d7", base, amount=50_000_000, currency="USD"),
        event("b1", "b1", "bot", "u8", "d8", base, channel="web"),
        event("c1", "c1", "cart", "u9", "d9", base, amount=3_000_001, channel="mobile", session_id="s1"),
    ]
    schema = StructType([
        StructField("event_id", StringType()), StructField("transaction_id", StringType()),
        StructField("card_id", StringType()), StructField("user_id", StringType()),
        StructField("device_id", StringType()), StructField("clean_event_time", TimestampType()),
        StructField("country", StringType()), StructField("amount", DoubleType()),
        StructField("currency", StringType()), StructField("status", StringType()),
        StructField("channel", StringType()), StructField("session_id", StringType()),
        StructField("merchant_id", StringType()), StructField("is_fraud", IntegerType()),
        StructField("fraud_scenario", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


def test_documented_rules_and_boundaries(spark, spark_events):
    sessions = spark.createDataFrame(
        [("s1", 30, 2, 1_000_000.0)],
        "session_id string, session_duration_sec long, product_view_count long, session_cart_total double",
    )
    scored = {row.transaction_id: row for row in score_transactions(spark_events, sessions, ["m1"]).collect()}
    assert scored["v3"].flag_scenario_b == 1  # exactly five minutes is included
    assert scored["g2"].flag_scenario_c == 1  # exactly fifteen minutes is included
    assert scored["d2"].flag_scenario_d == 1  # exactly ten minutes is included
    assert scored["d2"].is_predicted_fraud == 1
    assert scored["e1"].flag_scenario_e == 0
    assert scored["e2"].flag_scenario_e == 0
    assert scored["e3"].flag_scenario_e == 1  # two earlier failures then another attempt
    assert scored["e3"].is_predicted_fraud == 1
    assert scored["a1"].flag_scenario_a == 1
    assert scored["a2"].flag_scenario_a == 0  # no VND threshold applied to USD
    assert scored["a3"].flag_scenario_a == 0  # amount alone is insufficient outside the risky merchant set
    assert scored["b1"].flag_scenario_f == 1  # missing web session
    assert scored["c1"].flag_scenario_g == 1


def test_non_vnd_and_exact_cart_threshold_do_not_trigger_amount_rules(spark):
    base = datetime(2026, 1, 1)
    rows = [
        event("usd", "usd", "usd", "u1", "d1", base, currency="USD", amount=99_000_000),
        event("cart", "cart", "cart", "u2", "d2", base, amount=3_000_000, channel="mobile", session_id="s"),
    ]
    schema = "event_id string, transaction_id string, card_id string, user_id string, device_id string, clean_event_time timestamp, country string, amount double, currency string, status string, channel string, session_id string, merchant_id string, is_fraud int, fraud_scenario string"
    txns = spark.createDataFrame(rows, schema)
    sessions = spark.createDataFrame([("s", 20, 1, 1_000_000.0)], "session_id string, session_duration_sec long, product_view_count long, session_cart_total double")
    results = {row.transaction_id: row for row in score_transactions(txns, sessions, ["cart"]).collect()}
    assert results["usd"].flag_scenario_a == 0
    assert results["cart"].flag_scenario_g == 0  # strict greater-than 2M


def test_bad_json_and_missing_currency_are_quarantinable(spark):
    kafka_schema = StructType([
        StructField("key", StringType()), StructField("value", StringType()),
        StructField("topic", StringType()), StructField("partition", IntegerType()),
        StructField("offset", LongType()), StructField("timestamp", TimestampType()),
    ])
    kafka_rows = [
        (None, '{"event_id":', "transaction-events", 0, 1, datetime(2026, 1, 1)),
        (None, '{"event_id":"e","event_time":"2026-01-01T00:00:00Z","transaction_id":"t","user_id":"u","card_id":"c","merchant_id":"m","amount":10}', "transaction-events", 0, 2, datetime(2026, 1, 1)),
    ]
    parsed = parse_kafka_events(spark.createDataFrame(kafka_rows, kafka_schema), transaction_schema)
    rejected = validate_transactions(parsed).select("kafka_offset", "rejection_reason").collect()
    by_offset = {row.kafka_offset: row.rejection_reason for row in rejected}
    assert by_offset[1] == "CORRUPT_JSON_FIELD"
    assert by_offset[2] == "MISSING_CURRENCY"


def test_silver_deduplicates_and_keeps_late_valid_events(spark):
    schema = StructType([
        StructField("event_id", StringType()), StructField("event_type", StringType()),
        StructField("event_time", StringType()), StructField("transaction_id", StringType()),
        StructField("user_id", StringType()), StructField("card_id", StringType()),
        StructField("merchant_id", StringType()), StructField("amount", DoubleType()),
        StructField("currency", StringType()), StructField("country", StringType()),
        StructField("channel", StringType()), StructField("payment_method", StringType()),
        StructField("device_id", StringType()), StructField("ip_address", StringType()),
        StructField("status", StringType()), StructField("is_fraud", IntegerType()),
        StructField("fraud_scenario", StringType()), StructField("session_id", StringType()),
        StructField("_corrupt_record", StringType()), StructField("ingest_error", StringType()),
    ])
    late_time = "2025-12-31T00:00:00Z"
    row = ("e", "transaction", late_time, "t", "u", "c", "m", 100.0, "VND", "VN", "pos", "card", "d", "ip", "success", 0, None, None, None, None)
    bronze = spark.createDataFrame([row, row], schema)
    result = clean_transactions(bronze).collect()
    assert len(result) == 1
    assert result[0].clean_event_time.year == 2025
