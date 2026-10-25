# Databricks notebook source
# Silver -> Gold rule-based fraud detection and synthetic-label evaluation.
# COMMAND ----------
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp

workspace_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = "/Workspace/" + workspace_notebook.lstrip("/")
project_root = os.path.dirname(os.path.dirname(workspace_file))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from fraudstream.spark_ops import score_transactions

try:
    run_mode = dbutils.widgets.get("run_mode")
except Exception:
    run_mode = "BATCH"
if run_mode != "BATCH":
    raise ValueError("Gold batch notebook requires run_mode=BATCH; continuous mode uses notebook 05.")

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
schema_name = spark.sql("SELECT current_database()").first()[0]
silver_txn = f"{catalog_name}.{schema_name}.silver_transactions"
silver_sessions = f"{catalog_name}.{schema_name}.silver_session_profiles"
gold_alerts = f"{catalog_name}.{schema_name}.gold_fraud_alerts"
gold_perf = f"{catalog_name}.{schema_name}.gold_model_performance_history"
# Serverless does not allow changing Delta SQL configuration at notebook
# runtime; Gold schema evolution is handled explicitly by the MERGE columns.

transactions = spark.table(silver_txn)
sessions = spark.table(silver_sessions) if spark.catalog.tableExists(silver_sessions) else None
with open(os.path.join(project_root, "simulator", "data", "merchants.json"), "r", encoding="utf-8") as merchant_file:
    high_risk_merchant_ids = tuple(
        merchant["merchant_id"] for merchant in json.load(merchant_file)
        if merchant.get("category") in ("jewelry", "crypto", "electronics")
    )
scored = score_transactions(transactions, sessions, high_risk_merchant_ids)
alerts = scored.filter(col("is_predicted_fraud") == 1).select(
    col("event_id").alias("alert_id"), "transaction_id", "user_id", "card_id", "merchant_id",
    "amount", "currency", "country", col("clean_event_time").alias("event_time"),
    "fraud_risk_score", "detected_scenarios", "session_id", "session_duration_sec",
    col("is_fraud").alias("ground_truth_is_fraud"),
    col("fraud_scenario").alias("ground_truth_scenario"),
    "pipeline_version", "run_id", "processing_time",
).dropDuplicates(["transaction_id"]).withColumn("alert_generated_at", current_timestamp())
reconcile_stage = scored.select(
    col("event_id").alias("alert_id"), "transaction_id", "user_id", "card_id", "merchant_id",
    "amount", "currency", "country", col("clean_event_time").alias("event_time"),
    "fraud_risk_score", "detected_scenarios", "session_id", "session_duration_sec",
    col("is_fraud").alias("ground_truth_is_fraud"),
    col("fraud_scenario").alias("ground_truth_scenario"), "is_predicted_fraud",
    "pipeline_version", "run_id", "processing_time",
).withColumn("alert_generated_at", current_timestamp()).dropDuplicates(["transaction_id"])

if not spark.catalog.tableExists(gold_alerts):
    alerts.write.format("delta").mode("overwrite").saveAsTable(gold_alerts)
else:
    reconcile_stage.createOrReplaceTempView("fraudstream_alerts_stage")
    spark.sql(f"""
        MERGE INTO {gold_alerts} target
        USING fraudstream_alerts_stage source
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

labeled = scored.filter(col("is_fraud").isin(0, 1))
counts = {(int(row["is_fraud"]), int(row["is_predicted_fraud"])): int(row["count"])
          for row in labeled.groupBy("is_fraud", "is_predicted_fraud").count().collect()}
tp = counts.get((1, 1), 0)
fp = counts.get((0, 1), 0)
fn = counts.get((1, 0), 0)
tn = counts.get((0, 0), 0)
precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
accuracy = (tp + tn) / (tp + fp + fn + tn) if tp + fp + fn + tn else 0.0

evaluation = spark.createDataFrame([(
    datetime.now(timezone.utc).replace(tzinfo=None), tp, fp, fn, tn,
    round(accuracy * 100, 2), round(precision * 100, 2), round(recall * 100, 2), round(f1 * 100, 2),
    tp + fp + fn + tn, "rule_based_synthetic_labels",
)], ["evaluated_at", "true_positives", "false_positives", "false_negatives", "true_negatives",
    "accuracy_pct", "precision_pct", "recall_pct", "f1_score_pct", "total_evaluated", "evaluation_scope"])
if not spark.catalog.tableExists(gold_perf):
    evaluation.write.format("delta").mode("overwrite").saveAsTable(gold_perf)
else:
    evaluation.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(gold_perf)

total_alerts = spark.table(gold_alerts).count()
display(labeled.groupBy("is_fraud", "is_predicted_fraud").count())
display(spark.table(gold_alerts).orderBy(col("fraud_risk_score").desc(), col("event_time").desc()).limit(20))
metrics = {"status": "SUCCESS", "gold_alerts": total_alerts,
           "accuracy_pct": round(accuracy * 100, 2), "precision_pct": round(precision * 100, 2),
           "recall_pct": round(recall * 100, 2), "f1_score_pct": round(f1 * 100, 2),
           "evaluation_scope": "rule-based benchmark on simulator ground truth"}
dbutils.notebook.exit(json.dumps(metrics, ensure_ascii=False))
