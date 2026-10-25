# Operations runbook

## Before a run

1. Confirm Aiven topics and Kafka credentials.
2. Confirm the `fraudstream` Secret Scope contains
   `aiven-kafka-password`.
3. Confirm the CA PEM is readable at the configured Volume path.
4. Ensure no other batch/streaming run owns the same checkpoints.

## Batch smoke test

Run `notebooks/00_run_all_pipeline.py` with `run_mode=BATCH`, then verify
Bronze, Silver, quarantine, Gold, and dashboard tables.

## Streaming smoke test

Run `notebooks/05_fraudstream_continuous.py` with `run_mode=STREAMING` on a
classic job cluster. Verify active queries, increasing Bronze counts, Gold
alerts, and `fraudstream_pipeline_metrics`. Stop and restart the job to verify
checkpoint resume.

## Incident checks

- Query stopped: inspect the first exception in the query event log, not the
  wrapper `STREAM_FAILED` message.
- Timestamp error: inspect `clean_event_time` and quarantine reasons.
- Duplicate alerts: group Gold by `transaction_id`; the result should be empty.
- Rising quarantine rate: inspect raw payload and schema version in Bronze.
