"""Databricks configuration helpers. Supports Databricks Secrets, cluster env vars, and widget fallbacks."""

import os

DEFAULT_BOOTSTRAP_SERVERS = "flaudstream-smartwasher-server.g.aivencloud.com:27404"
DEFAULT_AIVEN_CA = """-----BEGIN CERTIFICATE-----
MIIEUDCCArigAwIBAgIUZ2o82aXW/lo+hFE/7E9TEhhvf04wDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1MDZiYTIwMDctNTc5OS00MDkwLThjODQtMTgwODg4ZmMw
YWY0IEdFTiAxIFByb2plY3QgQ0EwHhcNMjUxMTE2MDUzMjE3WhcNMzUxMTE0MDUz
MjE3WjBAMT4wPAYDVQQDDDUwNmJhMjAwNy01Nzk5LTQwOTAtOGM4NC0xODA4ODhm
YzBhZjQgR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
AYoCggGBAK+2bn9Xzhr+7VLIEj7va2958Lx6e3BUkCScEFmh8hiZ7wsUQYMa9fMx
lbgWh1QJz5sg5LsPxXajRMZHX0tIrtvuSH405TqEWO7Zv47Jfj8GmOj1xUmaLrbn
1MZMWhhZ6ZtdMFsaeM8nDmaJmJ43mhOLxUjgvujBK6wCuqWWEXuDjSTHZ+GpQW11
OoG8ya6Iu4JauLVs0nS0faLzU9PTppJpEGZxGg/N6CcDEsoGiLGl0AyjPjvlJPN7
szRn9wbDeDuIsO0kduhuKHqOXS7Hg9LI20a9gmzuSpFn4eTTc5dwbeMus1/1iazb
FUxShmmKwVoIl8UghkBPymyQPdcJgT0xFcJfM9vFx0STwE+6zeabSCH0hJefhIF2
fSIvYt/SvNdO/JLJYAMbJOzIG2eKZ32B7xmFLipx4QhAjnw2VbC8enAfoZjS0fZC
r0C+rKQg++Kw+rDln6HXvJgvBcFx4N3KG74PjZ72xpHWSkaDxkQsJd+gJx2stOug
+ApuULlVTwIDAQABo0IwQDAdBgNVHQ4EFgQUuhQwI1kztYRuQsyKmH3wYfPjaTUw
EgYDVR0TAQH/BAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQAD
ggGBAK7jMQYzRABMaWafxT783gJtO//r58C2ZttJjvVCaYf3m4eKYPZVxWQEldqN
+YFanUN+YFQu1EzPVVASCS9SyIgz41pikDpvXdm3d4YFBJuzuroKXQhVJRpqnaKM
4IxBi3AjVQ+pf2e1E6VEYzexl+yLB74v4oX054cnMLDxhZIXue4u8fxW5jgaSYqc
GCyFQ7reKRlfPvRwhu4Y4Zw4v3bIIWNEAlbRTXiwG3HavPHiGwn0gV1nP7pl1YKX
u8PHDXRmNVNxdgw1/nvJwn+o4R1hO0FVWwx1tbyn7hBcgSO8VfgIPik9n15E5RVb
9vjLdSlpGi0/CR5KdKsToiMeVUmM/PtGukwItszEFJF5HympD/T2ss6fIt+IFPXp
Oz5f7bApa5zj13Nq1vS8Me0oU7tS/Rp1gbGvE0GIOBH84LRC6j14KazFFND/QrZG
2bdFH7v52QZWXnj89BPp1CTpewDsqfJStXLljgr7Ec79scUEa7LiZwnWoAG0GcfX
gIUHcQ==
-----END CERTIFICATE-----"""


def kafka_options(dbutils, topic_name: str) -> dict:
    """Return verified SASL/SSL options for the configured Aiven Kafka topic."""
    username = os.environ.get("SASL_USERNAME", "avnadmin")

    # 1. Password: Ưu tiên Secret Scope -> fallback sang widget hoặc env var
    password = ""
    try:
        password = dbutils.secrets.get(
            scope=os.environ.get("DATABRICKS_SECRET_SCOPE", "fraudstream"),
            key=os.environ.get("DATABRICKS_KAFKA_PASSWORD_KEY", "aiven-kafka-password"),
        )
    except Exception:
        pass

    if not password:
        try:
            password = dbutils.widgets.get("kafka_password")
        except Exception:
            pass

    if not password:
        password = os.environ.get("SASL_PASSWORD") or os.environ.get("KAFKA_PASSWORD", "")

    # 2. CA Certificate: Ưu tiên KAFKA_CA_PATH -> fallback local ca.pem -> fallback DEFAULT_AIVEN_CA
    ca_pem = ""
    ca_path = os.environ.get("KAFKA_CA_PATH")
    if ca_path:
        if os.path.exists(ca_path):
            with open(ca_path, "r", encoding="utf-8") as ca_file:
                ca_pem = ca_file.read()
        else:
            raise ValueError(f"KAFKA_CA_PATH '{ca_path}' was set but the file does not exist.")
    elif os.path.exists("ca.pem"):
        with open("ca.pem", "r", encoding="utf-8") as ca_file:
            ca_pem = ca_file.read()
    else:
        ca_pem = DEFAULT_AIVEN_CA

    if not ca_pem or "BEGIN CERTIFICATE" not in ca_pem:
        raise ValueError("Could not load a valid Aiven CA PEM certificate.")

    def jaas_escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')

    jaas = (
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{jaas_escape(username)}" password="{jaas_escape(password)}";'
    )
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": os.environ.get("SASL_MECHANISM", "SCRAM-SHA-256"),
        "kafka.sasl.jaas.config": jaas,
        "kafka.ssl.truststore.type": "PEM",
        "kafka.ssl.truststore.certificates": ca_pem,
        "subscribe": topic_name,
        "startingOffsets": "earliest",
    }
