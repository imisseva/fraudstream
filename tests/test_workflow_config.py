"""Ensure the exported Databricks Job exposes only the non-secret run mode."""

import json
from pathlib import Path


def test_workflow_has_batch_default_and_no_password_parameter():
    job = json.loads((Path(__file__).parents[1] / "workflows" / "databricks_workflow_job.json").read_text(encoding="utf-8"))
    assert job["parameters"] == [{"name": "run_mode", "default": "BATCH"}]
    assert "kafka_password" not in json.dumps(job).lower()
    assert job["tasks"][0]["notebook_task"]["notebook_path"] == "notebooks/00_run_all_pipeline"
