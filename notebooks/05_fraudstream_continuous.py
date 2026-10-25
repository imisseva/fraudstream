# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Long-running Bronze -> Silver -> Gold pipeline. Start/stop via the Databricks Job.

# COMMAND ----------

import logging
import json
import os
import sys
import threading

from pyspark.sql.functions import col, current_timestamp, expr, lit

workspace_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = "/Workspace/" + workspace_notebook.lstrip("/")
project_root = os.path.dirname(os.path.dirname(workspace_file))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from fraudstream.databricks_config import kafka_options
from fraudstream.observability import DeltaProgressListener, record_batch_metrics
from fraudstream.spark_ops import (
    bronze_event_schema,
    clean_clickstream,
    clean_transactions,
    clickstream_schema,
    parse_kafka_events,
    score_transactions,
    session_profiles,
    transaction_schema,
    validate_clickstream,
    validate_transactions,
)

logger = logging.getLogger("fraudstream.continuous")
logger.setLevel(logging.INFO)

try:
    dbutils.widgets.dropdown("run_mode", "STREAMING", ["STREAMING"], "Pipeline mode")
except Exception:
    pass
try:
    run_mode = dbutils.widgets.get("run_mode").strip().upper()
except Exception:
    run_mode = "STREAMING"
if run_mode != "STREAMING":
    raise ValueError("This notebook only supports run_mode=STREAMING")

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
schema_name = spark.sql("SELECT current_database()").first()[0]
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.fraud_checkpoints")
root = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints"

tables = {
    "bronze_txn": f"{catalog_name}.{schema_name}.bronze_transactions",
    "bronze_click": f"{catalog_name}.{schema_name}.bronze_clickstream",
    "silver_txn": f"{catalog_name}.{schema_name}.silver_transactions",
    "silver_click": f"{catalog_name}.{schema_name}.silver_clickstream",
    "silver_sessions": f"{catalog_name}.{schema_name}.silver_session_profiles",
    "quarantine_txn": f"{catalog_name}.{schema_name}.quarantine_transactions",
    "quarantine_click": f"{catalog_name}.{schema_name}.quarantine_clickstream",
    "gold": f"{catalog_name}.{schema_name}.gold_fraud_alerts",
    "metrics": f"{catalog_name}.{schema_name}.fraudstream_pipeline_metrics",
}

# Serverless/Spark Connect does not allow changing Delta/streaming SQL
# configurations at runtime. Schema additions are handled explicitly by
# ensure_table() and stream writers use mergeSchema where supported.
# Use processing-time triggers for the long-running job. `availableNow` belongs
# to the finite batch notebooks and must not be used here.
trigger_interval = os.environ.get("STREAMING_TRIGGER", "10 seconds")
queries = []
gold_merge_lock = threading.RLock()
progress_listener = DeltaProgressListener(spark, tables["metrics"])
spark.streams.addListener(progress_listener)


def ensure_table(name, empty_df):
    if not spark.catalog.tableExists(name):
        empty_df.limit(0).write.format("delta").mode("overwrite").saveAsTable(name)
        return
    existing_columns = set(spark.table(name).columns)
    missing_fields = [field for field in empty_df.schema.fields if field.name not in existing_columns]
    if missing_fields:
        additions = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()}" for field in missing_fields
        )
        spark.sql(f"ALTER TABLE {name} ADD COLUMNS ({additions})")


def kafka_source(topic):
    reader = spark.readStream.format("kafka")
    for option_name, option_value in kafka_options(dbutils, topic).items():
        reader = reader.option(option_name, option_value)
    return reader.load()


def bronze_writer(topic, schema, table, checkpoint):
    parsed = parse_kafka_events(kafka_source(topic), schema)
    return parsed.writeStream.format("delta").outputMode("append") \
        .option("checkpointLocation", f"{root}/{checkpoint}") \
        .option("mergeSchema", "true") \
        .trigger(processingTime=trigger_interval).toTable(table)


def merge_rows(batch_df, table_name, keys):
    source = batch_df.dropDuplicates(keys)
    if not spark.catalog.tableExists(table_name):
        source.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return
    stage_name = f"fraudstream_merge_{table_name.split('.')[-1]}"
    source.createOrReplaceTempView(stage_name)
    predicate = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    spark.sql(f"""
        MERGE INTO {table_name} target
        USING {stage_name} source
        ON {predicate}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def update_session_profiles(click_batch, batch_id):
    if click_batch.isEmpty():
        return
    record_batch_metrics(spark, tables["metrics"], "silver", "session_profiles", batch_id, click_batch.count())
    touched = click_batch.select("session_id", "user_id").dropDuplicates()
    all_clicks = spark.table(tables["silver_click"]).join(touched, ["session_id", "user_id"], "inner")
    with gold_merge_lock:
        merge_rows(session_profiles(all_clicks).withColumn("profile_updated_at", current_timestamp()),
                   tables["silver_sessions"], ["session_id", "user_id"])
        touched_txns = spark.table(tables["silver_txn"]).join(touched, ["session_id", "user_id"], "inner")
        upsert_alerts(touched_txns)


def alert_stage(scored):
    return scored.select(
        col("event_id").alias("alert_id"), "transaction_id", "user_id", "card_id", "merchant_id",
        "amount", "currency", "country", col("clean_event_time").alias("event_time"),
        "fraud_risk_score", "detected_scenarios", "session_id", "session_duration_sec",
        col("is_fraud").alias("ground_truth_is_fraud"),
        col("fraud_scenario").alias("ground_truth_scenario"), "is_predicted_fraud",
        "pipeline_version", "run_id", "processing_time",
    ).withColumn("alert_generated_at", current_timestamp()).dropDuplicates(["transaction_id"])


def reconcile_alerts(scored):
    stage = alert_stage(scored)
    stage.createOrReplaceTempView("fraudstream_alert_reconcile_stage")
    spark.sql(f"""
        MERGE INTO {tables['gold']} target
        USING fraudstream_alert_reconcile_stage source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED AND source.is_predicted_fraud = 0 THEN DELETE
        WHEN MATCHED AND source.is_predicted_fraud = 1 THEN UPDATE SET
            target.alert_id = source.alert_id,
            target.user_id = source.user_id,
            target.card_id = source.card_id,
            target.merchant_id = source.merchant_id,
            target.amount = source.amount,
            target.currency = source.currency,
            target.country = source.country,
            target.event_time = source.event_time,
            target.fraud_risk_score = source.fraud_risk_score,
            target.detected_scenarios = source.detected_scenarios,
            target.session_id = source.session_id,
            target.session_duration_sec = source.session_duration_sec,
            target.ground_truth_is_fraud = source.ground_truth_is_fraud,
            target.ground_truth_scenario = source.ground_truth_scenario,
            target.pipeline_version = source.pipeline_version,
            target.run_id = source.run_id,
            target.processing_time = source.processing_time,
            target.alert_generated_at = source.alert_generated_at
        WHEN NOT MATCHED AND source.is_predicted_fraud = 1 THEN INSERT (
            alert_id, transaction_id, user_id, card_id, merchant_id, amount, currency,
            country, event_time, fraud_risk_score, detected_scenarios, session_id,
            session_duration_sec, ground_truth_is_fraud, ground_truth_scenario,
            pipeline_version, run_id, processing_time, alert_generated_at
        ) VALUES (
            source.alert_id, source.transaction_id, source.user_id, source.card_id, source.merchant_id,
            source.amount, source.currency, source.country, source.event_time, source.fraud_risk_score,
            source.detected_scenarios, source.session_id, source.session_duration_sec,
            source.ground_truth_is_fraud, source.ground_truth_scenario,
            source.pipeline_version, source.run_id, source.processing_time, source.alert_generated_at
        )
    """)


def upsert_alerts(target_txns):
    if target_txns.isEmpty():
        return
    txn_ids = target_txns.select("transaction_id").dropDuplicates()
    # Include enough preceding event-time history for geo, velocity and retry rules.
    bounds = target_txns.agg({"clean_event_time": "min"}).first()[0]
    if bounds is None:
        return
    history = spark.table(tables["silver_txn"]).filter(
        col("clean_event_time") >= lit(bounds) - expr("INTERVAL 15 MINUTES")
    )
    scored = score_transactions(history, spark.table(tables["silver_sessions"]), high_risk_merchant_ids) \
        .join(txn_ids, "transaction_id", "inner")
    reconcile_alerts(scored)


def score_new_transactions(txn_batch, batch_id):
    if not txn_batch.isEmpty():
        record_batch_metrics(spark, tables["metrics"], "gold", "fraud_detection", batch_id, txn_batch.count())
        with gold_merge_lock:
            upsert_alerts(txn_batch)


# Create stream source/sink tables before downstream Delta streaming reads start.
project_root = os.path.dirname(os.path.dirname(workspace_file))
with open(os.path.join(project_root, "simulator", "data", "merchants.json"), "r", encoding="utf-8") as merchant_file:
    high_risk_merchant_ids = tuple(
        merchant["merchant_id"] for merchant in json.load(merchant_file)
        if merchant.get("category") in ("jewelry", "crypto", "electronics")
    )
empty_txn_bronze = spark.createDataFrame([], bronze_event_schema(transaction_schema))
empty_click_bronze = spark.createDataFrame([], bronze_event_schema(clickstream_schema))
ensure_table(tables["bronze_txn"], empty_txn_bronze)
ensure_table(tables["bronze_click"], empty_click_bronze)
ensure_table(tables["silver_txn"], clean_transactions(empty_txn_bronze))
ensure_table(tables["silver_click"], clean_clickstream(empty_click_bronze))
ensure_table(tables["quarantine_txn"], validate_transactions(empty_txn_bronze).withColumn("quarantined_at", current_timestamp()))
ensure_table(tables["quarantine_click"], validate_clickstream(empty_click_bronze).withColumn("quarantined_at", current_timestamp()))
ensure_table(tables["silver_sessions"], session_profiles(spark.table(tables["silver_click"])).withColumn("profile_updated_at", current_timestamp()))
empty_scored = score_transactions(
    spark.table(tables["silver_txn"]).limit(0),
    spark.table(tables["silver_sessions"]).limit(0),
    high_risk_merchant_ids,
)
ensure_table(tables["gold"], alert_stage(empty_scored).drop("is_predicted_fraud"))

# Bronze Kafka readers use the same checkpoint paths and query plans as batch ingestion.
queries.append(bronze_writer("transaction-events", transaction_schema, tables["bronze_txn"], "txn_bronze"))
queries.append(bronze_writer("clickstream-events", clickstream_schema, tables["bronze_click"], "click_bronze"))

txn_stream = spark.readStream.table(tables["bronze_txn"])
txn_validated = validate_transactions(txn_stream)
queries.append(txn_validated.filter(col("rejection_reason") != "VALID")
    .withColumn("quarantined_at", current_timestamp()).writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{root}/dlq_txn").option("mergeSchema", "true")
    .trigger(processingTime=trigger_interval).toTable(tables["quarantine_txn"]))
queries.append(clean_transactions(txn_stream).writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{root}/silver_txn").option("mergeSchema", "true")
    .trigger(processingTime=trigger_interval).toTable(tables["silver_txn"]))

click_stream = spark.readStream.table(tables["bronze_click"])
click_validated = validate_clickstream(click_stream)
queries.append(click_validated.filter(col("rejection_reason") != "VALID")
    .withColumn("quarantined_at", current_timestamp()).writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{root}/dlq_click").option("mergeSchema", "true")
    .trigger(processingTime=trigger_interval).toTable(tables["quarantine_click"]))
clean_click_stream = clean_clickstream(click_stream)
queries.append(clean_click_stream.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", f"{root}/silver_click").option("mergeSchema", "true")
    .trigger(processingTime=trigger_interval).toTable(tables["silver_click"]))

profile_stream = spark.readStream.table(tables["silver_click"])
queries.append(profile_stream.writeStream.foreachBatch(update_session_profiles)
    .option("checkpointLocation", f"{root}/silver_sessions")
    .trigger(processingTime=trigger_interval).start())

gold_source = spark.readStream.table(tables["silver_txn"])
queries.append(gold_source.writeStream.foreachBatch(score_new_transactions)
    .option("checkpointLocation", f"{root}/gold_detection")
    .trigger(processingTime=trigger_interval).start())

logger.info("Started %s FraudStream queries; trigger=%s", len(queries), trigger_interval)
try:
    spark.streams.awaitAnyTermination()
    raise RuntimeError("A FraudStream query terminated unexpectedly; inspect query status and logs.")
finally:
    for query in queries:
        if query.isActive:
            query.stop()
    spark.streams.removeListener(progress_listener)