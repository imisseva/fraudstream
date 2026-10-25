"""Small, dependency-free observability helpers for Databricks streaming jobs."""

import os
import json
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.streaming import StreamingQueryListener


PIPELINE_VERSION = os.environ.get("FRAUDSTREAM_PIPELINE_VERSION", "0.1.0")


def with_audit_columns(df: DataFrame, run_id: Optional[str] = None) -> DataFrame:
    """Attach immutable processing metadata to Silver/Gold records."""
    return df.withColumn("pipeline_version", lit(PIPELINE_VERSION)) \
        .withColumn("run_id", lit(run_id or os.environ.get("DATABRICKS_RUN_ID", "local"))) \
        .withColumn("processing_time", current_timestamp())


def record_batch_metrics(
    spark: SparkSession,
    table_name: str,
    layer: str,
    query_name: str,
    batch_id: int,
    row_count: int,
) -> None:
    """Append one progress row; create the Delta table on first use."""
    metrics = spark.createDataFrame([
        (PIPELINE_VERSION, layer, query_name, int(batch_id), int(row_count)),
    ], ["pipeline_version", "layer", "query_name", "batch_id", "row_count"])
    metrics = metrics.withColumn("recorded_at", current_timestamp())
    metrics.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)


class DeltaProgressListener(StreamingQueryListener):
    """Persist Spark query progress, throughput and Kafka source offsets."""

    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name

    def onQueryStarted(self, event):
        return None

    def onQueryProgress(self, event):
        progress = event.progress
        sources = getattr(progress, "sources", []) or []
        source_offsets = [getattr(source, "endOffset", None) for source in sources]
        row = self.spark.createDataFrame([(
            PIPELINE_VERSION,
            progress.name,
            progress.id,
            int(progress.batchId),
            int(getattr(progress, "numInputRows", 0)),
            float(getattr(progress, "inputRowsPerSecond", 0.0)),
            float(getattr(progress, "processedRowsPerSecond", 0.0)),
            json.dumps(source_offsets, default=str),
        ], ["pipeline_version", "query_name", "query_id", "batch_id", "num_input_rows",
            "input_rows_per_second", "processed_rows_per_second", "source_end_offsets"])
        row.withColumn("recorded_at", current_timestamp()).write.format("delta") \
            .mode("append").option("mergeSchema", "true").saveAsTable(self.table_name)

    def onQueryTerminated(self, event):
        return None
