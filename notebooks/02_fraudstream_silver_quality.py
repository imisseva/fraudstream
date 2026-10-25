# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Bronze -> Silver quality and quarantine. Continuous processing is in notebook 05.

# COMMAND ----------

import json
import os
import sys
from pyspark.sql.functions import col, current_timestamp

workspace_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = "/Workspace/" + workspace_notebook.lstrip("/")
project_root = os.path.dirname(os.path.dirname(workspace_file))
src_root = os.path.join(project_root, "src")
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from fraudstream.spark_ops import clean_clickstream, clean_transactions, session_profiles, validate_clickstream, validate_transactions

try:
    run_mode = dbutils.widgets.get("run_mode")
except Exception:
    run_mode = "BATCH"
if run_mode != "BATCH":
    raise ValueError("Silver batch notebook requires run_mode=BATCH; use notebook 05 for continuous mode.")

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
schema_name = spark.sql("SELECT current_database()").first()[0]
root = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints"
bronze_txn = f"{catalog_name}.{schema_name}.bronze_transactions"
bronze_click = f"{catalog_name}.{schema_name}.bronze_clickstream"
silver_txn = f"{catalog_name}.{schema_name}.silver_transactions"
silver_click = f"{catalog_name}.{schema_name}.silver_clickstream"
silver_sessions = f"{catalog_name}.{schema_name}.silver_session_profiles"
quarantine_txn = f"{catalog_name}.{schema_name}.quarantine_transactions"
quarantine_click = f"{catalog_name}.{schema_name}.quarantine_clickstream"

# These cluster-level tuning settings are intentionally omitted: Databricks
# Free Edition/serverless (Spark Connect) does not allow changing the
# state-store provider or Delta write optimizations at notebook runtime.

txn_stream = spark.readStream.table(bronze_txn)
txn_validated = validate_transactions(txn_stream)
txn_bad = txn_validated.filter(col("rejection_reason") != "VALID").withColumn("quarantined_at", current_timestamp())
txn_good = clean_transactions(txn_stream)

click_stream = spark.readStream.table(bronze_click)
click_validated = validate_clickstream(click_stream)
click_bad = click_validated.filter(col("rejection_reason") != "VALID").withColumn("quarantined_at", current_timestamp())
click_good = clean_clickstream(click_stream)

queries = [
    txn_bad.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{root}/dlq_txn").option("mergeSchema", "true").trigger(availableNow=True).toTable(quarantine_txn),
    txn_good.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{root}/silver_txn").option("mergeSchema", "true").trigger(availableNow=True).toTable(silver_txn),
    click_bad.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{root}/dlq_click").option("mergeSchema", "true").trigger(availableNow=True).toTable(quarantine_click),
    click_good.writeStream.format("delta").outputMode("append").option("checkpointLocation", f"{root}/silver_click").option("mergeSchema", "true").trigger(availableNow=True).toTable(silver_click),
]
try:
    for query in queries:
        query.awaitTermination()
except Exception:
    for query in queries:
        query.stop()
    raise

profiles = session_profiles(spark.table(silver_click))
if not spark.catalog.tableExists(silver_sessions):
    profiles.write.format("delta").mode("overwrite").saveAsTable(silver_sessions)
else:
    profiles.createOrReplaceTempView("fraudstream_session_profiles_stage")
    spark.sql(f"""
        MERGE INTO {silver_sessions} target
        USING fraudstream_session_profiles_stage source
        ON target.session_id = source.session_id AND target.user_id = source.user_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

txn_count = spark.table(silver_txn).count()
txn_quarantine_count = spark.table(quarantine_txn).count()
click_count = spark.table(silver_click).count()
click_quarantine_count = spark.table(quarantine_click).count()
session_count = spark.table(silver_sessions).count()
reason_counts = spark.table(quarantine_txn).groupBy("rejection_reason").count().orderBy(col("count").desc())
display(reason_counts)
print(f"Silver transactions={txn_count:,}; transaction quarantine={txn_quarantine_count:,}; clickstream={click_count:,}; click quarantine={click_quarantine_count:,}; sessions={session_count:,}")
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS", "silver_transactions": txn_count,
    "quarantine_transactions": txn_quarantine_count, "silver_clickstream": click_count,
    "quarantine_clickstream": click_quarantine_count, "session_profiles": session_count,
}, ensure_ascii=False))
