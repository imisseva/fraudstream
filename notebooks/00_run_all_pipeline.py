# Databricks notebook source
# Databricks master entrypoint. BATCH runs the finite DAG; STREAMING stays active.
# COMMAND ----------
import json
import time
from datetime import datetime, timezone

try:
    dbutils.widgets.dropdown("run_mode", "BATCH", ["BATCH", "STREAMING"], "Pipeline mode")
    dbutils.widgets.text("kafka_password", "", "Aiven Kafka Password (Optional if secret configured)")
except Exception:
    pass

run_mode = dbutils.widgets.get("run_mode").strip().upper()
if run_mode not in ("BATCH", "STREAMING"):
    raise ValueError("run_mode must be BATCH or STREAMING")

try:
    kafka_pwd = dbutils.widgets.get("kafka_password")
except Exception:
    kafka_pwd = ""

print(f"Started at {datetime.now(timezone.utc).isoformat()} | run_mode={run_mode}")


def run_step(label, path, timeout=1800):
    started = time.time()
    print(f"Starting {label}: {path}")
    args = {"run_mode": run_mode}
    if kafka_pwd:
        args["kafka_password"] = kafka_pwd
    try:
        output = dbutils.notebook.run(path, timeout_seconds=timeout, arguments=args)
    except Exception as exc:
        raise RuntimeError(f"Pipeline step '{label}' failed: {exc}") from exc
    duration = round(time.time() - started, 2)
    payload = json.loads(output) if output else {}
    if payload.get("status") == "FAILED":
        raise RuntimeError(f"Pipeline step '{label}' returned FAILED: {payload}")
    print(f"Completed {label} in {duration}s: {payload}")
    return {"step": label, "duration_sec": duration, "result": payload}


if run_mode == "STREAMING":
    # This call intentionally remains open; stopping the Workflow run stops all queries.
    run_step("Continuous Bronze-Silver-Gold", "./05_fraudstream_continuous", timeout=0)
else:
    results = []
    results.append(run_step("Bronze ingestion", "./01_fraudstream_ingestion"))
    results.append(run_step("Silver quality and profiles", "./02_fraudstream_silver_quality"))
    results.append(run_step("Gold fraud detection", "./03_fraudstream_gold_detection"))
    results.append(run_step("Analytics", "./04_fraud_dashboard_analytics"))
    display(spark.createDataFrame([
        (item["step"], item["duration_sec"], json.dumps(item["result"], ensure_ascii=False))
        for item in results
    ], ["step", "duration_sec", "result_json"]))
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "steps": results}, ensure_ascii=False))
