"""Configuration contract tests; no live Databricks or Kafka connection is made."""

from unittest.mock import Mock

from fraudstream.databricks_config import kafka_options


def test_kafka_options_use_secret_and_verified_ca(monkeypatch, tmp_path):
    ca_file = tmp_path / "aiven-ca.pem"
    ca_file.write_text("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----", encoding="utf-8")
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "broker.example:1234")
    monkeypatch.setenv("KAFKA_CA_PATH", str(ca_file))
    monkeypatch.setenv("DATABRICKS_SECRET_SCOPE", "fraudstream-test")
    monkeypatch.setenv("DATABRICKS_KAFKA_PASSWORD_KEY", "kafka-pass")
    dbutils = Mock()
    dbutils.secrets.get.return_value = "secret-value"

    options = kafka_options(dbutils, "transaction-events")

    dbutils.secrets.get.assert_called_once_with(scope="fraudstream-test", key="kafka-pass")
    assert options["kafka.bootstrap.servers"] == "broker.example:1234"
    assert options["kafka.ssl.truststore.certificates"].startswith("-----BEGIN CERTIFICATE-----")
    assert options["subscribe"] == "transaction-events"
    assert "password=\"secret-value\"" in options["kafka.sasl.jaas.config"]
    assert "enable.ssl.certificate.verification" not in options
