"""Schema contract tests independent of a live Kafka or Databricks workspace."""

from fraudstream.spark_ops import clickstream_schema, transaction_schema


def test_transaction_contract_has_required_fields_and_versionable_shape():
    fields = {field.name for field in transaction_schema.fields}
    assert {"event_id", "event_time", "transaction_id", "user_id", "card_id", "amount", "currency"} <= fields
    assert "fraud_scenario" in fields


def test_clickstream_contract_has_required_fields_and_versionable_shape():
    fields = {field.name for field in clickstream_schema.fields}
    assert {"event_id", "event_time", "user_id", "session_id", "event_type"} <= fields
