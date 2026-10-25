# ==============================================================================
# FraudStream: Aiven Kafka -> Bronze Delta (finite availableNow ingestion)
# This notebook is intended to run from the checked-out FraudStream project root.
# ==============================================================================
# COMMAND ----------
import json
import os
import sys

workspace_notebook = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_file = "/Workspace/" + workspace_notebook.lstrip("/")
project_root = os.path.dirname(os.path.dirname(workspace_file))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from fraudstream.databricks_config import kafka_options
from fraudstream.spark_ops import clickstream_schema, parse_kafka_events, transaction_schema

try:
    run_mode = dbutils.widgets.get("run_mode")
except Exception:
    dbutils.widgets.dropdown("run_mode", "BATCH", ["BATCH", "STREAMING"], "Pipeline mode")
    run_mode = dbutils.widgets.get("run_mode")
if run_mode not in ("BATCH", "STREAMING"):
    raise ValueError("run_mode must be BATCH or STREAMING")

catalog_name = spark.sql("SELECT current_catalog()").first()[0]
schema_name = spark.sql("SELECT current_database()").first()[0]
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.fraud_checkpoints")

target_txn_table = f"{catalog_name}.{schema_name}.bronze_transactions"
target_click_table = f"{catalog_name}.{schema_name}.bronze_clickstream"
checkpoint_root = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints"


def create_kafka_stream(topic_name):
    reader = spark.readStream.format("kafka")
    for option_name, option_value in kafka_options(dbutils, topic_name).items():
        reader = reader.option(option_name, option_value)
    return reader.load()


def start_bronze_query(topic, schema, checkpoint):
    parsed = parse_kafka_events(create_kafka_stream(topic), schema)
    writer = parsed.writeStream.format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{checkpoint_root}/{checkpoint}") \
        .option("mergeSchema", "true")
    return writer.trigger(availableNow=True)


# The finite ingestion notebook is for BATCH mode only. Full continuous mode is
# orchestrated by 05_fraudstream_continuous.py so downstream stages stay active.
if run_mode != "BATCH":
    raise ValueError("Run the continuous pipeline through 05_fraudstream_continuous, not this batch task.")

query_txn = start_bronze_query("transaction-events", transaction_schema, "txn_bronze").toTable(target_txn_table)
query_click = start_bronze_query("clickstream-events", clickstream_schema, "click_bronze").toTable(target_click_table)

try:
    query_txn.awaitTermination()
    query_click.awaitTermination()
except Exception:
    query_txn.stop()
    query_click.stop()
    raise

txn_count = spark.table(target_txn_table).count()
click_count = spark.table(target_click_table).count()
print(f"Bronze transactions: {txn_count:,}; clickstream: {click_count:,}")
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "bronze_transactions": txn_count,
    "bronze_clickstream": click_count,
}))
