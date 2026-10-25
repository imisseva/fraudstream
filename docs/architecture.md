# FraudStream architecture

```text
Docker simulator / application producers
        -> Aiven Kafka (transaction-events, clickstream-events)
        -> Databricks Spark Structured Streaming
             -> Bronze Delta (raw payload + Kafka metadata)
             -> Silver Delta (validated/cleaned data)
             -> Quarantine Delta (rejected records + reason)
             -> Gold Delta (idempotent fraud alerts)
             -> Analytics/dashboard
```

The primary deployment is Databricks notebooks managed by Databricks Asset
Bundles. `workflows/databricks_workflow_job.json` is retained only as a legacy
manual-import definition. `pipelines/dlt_fraudstream_pipeline.py` is an
optional DLT example that consumes existing Bronze data; it is not an
independent Kafka source or part of the primary DAG.

## Execution modes

- `BATCH`: `01` through `04` use finite `availableNow` micro-batches.
- `STREAMING`: `05` uses stable checkpoints and processing-time triggers on a
  classic Databricks job cluster. Databricks Free Edition Serverless may
  restrict this to `availableNow`; in that environment run the pipeline as
  scheduled incremental micro-batches.

Batch and streaming must not target the same tables and checkpoints at the
same time.
