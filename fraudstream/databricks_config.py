"""Databricks configuration helpers. Secrets are read at runtime, never passed as job parameters."""

import os


def kafka_options(dbutils, topic_name: str) -> dict:
    """Return verified SASL/SSL options for the configured Aiven Kafka topic."""
    username = os.environ.get("SASL_USERNAME", "avnadmin")
    password = dbutils.secrets.get(
        scope=os.environ.get("DATABRICKS_SECRET_SCOPE", "fraudstream"),
        key=os.environ.get("DATABRICKS_KAFKA_PASSWORD_KEY", "aiven-kafka-password"),
    )
    ca_path = os.environ.get("KAFKA_CA_PATH")
    if not ca_path:
        raise ValueError("Set KAFKA_CA_PATH to the Aiven CA PEM file on the Databricks cluster.")
    with open(ca_path, "r", encoding="utf-8") as ca_file:
        ca_pem = ca_file.read()
    if "BEGIN CERTIFICATE" not in ca_pem:
        raise ValueError("KAFKA_CA_PATH does not contain a PEM certificate.")

    def jaas_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{jaas_escape(username)}" password="{jaas_escape(password)}";'
    )
    return {
        "kafka.bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": os.environ.get("SASL_MECHANISM", "SCRAM-SHA-256"),
        "kafka.sasl.jaas.config": jaas,
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.certificates": ca_pem,
        "subscribe": topic_name,
        "startingOffsets": "earliest",
    }
