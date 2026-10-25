# ==============================================================================
# FraudStream: Databricks PySpark Structured Streaming Ingestion
# Kết nối Aiven Kafka Cloud -> Đọc song song 2 topics:
#   1. transaction-events  -> bronze_transactions
#   2. clickstream-events  -> bronze_clickstream
# ==============================================================================

# COMMAND ----------
# 1. IMPORTS & ĐỊNH NGHĨA SCHEMAS (TRANSACTIONS & CLICKSTREAM)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

# Schema 16+1 trường giao dịch tài chính
transaction_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("fraud_scenario", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Schema 19 trường hành vi duyệt web (Clickstream Funnel)
clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("user_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("query", StringType(), True),
    StructField("results_count", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("cart_total", DoubleType(), True),
    StructField("referrer", StringType(), True)
])

# COMMAND ----------
# 2. CẤU HÌNH KẾT NỐI AIVEN KAFKA CLOUD (CHUẨN DATABRICKS SHADED + SSL CA PEM)
import os

BOOTSTRAP_SERVERS = "flaudstream-smartwasher-server.g.aivencloud.com:27404"
USERNAME = "avnadmin"

# Nhập mật khẩu qua Databricks Widget hoặc Databricks Secrets để bảo mật tuyệt đối
try:
    dbutils.widgets.text("kafka_password", "", "Aiven Kafka Password")
    PASSWORD = dbutils.widgets.get("kafka_password") or os.environ.get("KAFKA_PASSWORD", "")
except Exception:
    PASSWORD = os.environ.get("KAFKA_PASSWORD", "")

# Khi chạy thực tế trên notebook cá nhân, điền password vào ô widget phía trên hoặc thay thế tạm thời tại đây
if not PASSWORD:
    PASSWORD = "<YOUR_AIVEN_PASSWORD_HERE>"

# Chứng chỉ Root CA của Aiven Project để vượt qua lỗi PKIX SSL Handshake
AIVEN_CA_CERT = """-----BEGIN CERTIFICATE-----
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

# Tiền tố kafkashaded. là bắt buộc trên Databricks để nạp đúng ScramLoginModule
jaas_config = f'kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="{USERNAME}" password="{PASSWORD}";'

def create_kafka_stream(topic_name: str):
    """Hàm tiện ích nạp stream an toàn từ Aiven Kafka."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
        .option("kafka.sasl.jaas.config", jaas_config) \
        .option("kafka.ssl.truststore.type", "PEM") \
        .option("kafka.ssl.truststore.certificates", AIVEN_CA_CERT) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

# COMMAND ----------
# 3. THIẾT LẬP CATALOG, VOLUMES & TÊN BẢNG BRONZE
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.fraud_checkpoints")

target_txn_table = f"{catalog_name}.{schema_name}.bronze_transactions"
target_click_table = f"{catalog_name}.{schema_name}.bronze_clickstream"

checkpoint_txn_path = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/txn_bronze"
checkpoint_click_path = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/click_bronze"

print(f"✅ Target Transactions: {target_txn_table}")
print(f"✅ Target Clickstream:  {target_click_table}")

# COMMAND ----------
# 4. STREAM 1: INGEST TOPIC TRANSACTION-EVENTS -> BRONZE_TRANSACTIONS
kafka_txn_raw = create_kafka_stream("transaction-events")

parsed_txn_df = kafka_txn_raw \
    .selectExpr("CAST(key AS STRING) AS user_key", "CAST(value AS STRING) AS json_value") \
    .select(col("user_key"), from_json(col("json_value"), transaction_schema).alias("data")) \
    .select("data.*")

query_txn = parsed_txn_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_txn_path) \
    .trigger(availableNow=True) \
    .toTable(target_txn_table)

query_txn.awaitTermination()
print("🚀 Đã nạp thành công toàn bộ giao dịch vào bronze_transactions!")

# COMMAND ----------
# 5. STREAM 2: INGEST TOPIC CLICKSTREAM-EVENTS -> BRONZE_CLICKSTREAM
kafka_click_raw = create_kafka_stream("clickstream-events")

parsed_click_df = kafka_click_raw \
    .selectExpr("CAST(key AS STRING) AS user_key", "CAST(value AS STRING) AS json_value") \
    .select(col("user_key"), from_json(col("json_value"), clickstream_schema).alias("data")) \
    .select("data.*")

query_click = parsed_click_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_click_path) \
    .trigger(availableNow=True) \
    .toTable(target_click_table)

query_click.awaitTermination()
print("🌐 Đã nạp thành công toàn bộ hành vi clickstream vào bronze_clickstream!")

# COMMAND ----------
# 6. TRUY VẤN VÀ HIỂN THỊ DỮ LIỆU TỪ 2 BẢNG BRONZE
print("--- PREVIEW BRONZE TRANSACTIONS ---")
display(spark.sql(f"SELECT transaction_id, user_id, amount, currency, event_time, session_id FROM {target_txn_table} ORDER BY event_time DESC LIMIT 20"))

print("--- PREVIEW BRONZE CLICKSTREAM ---")
display(spark.sql(f"SELECT event_id, event_type, session_id, user_id, page, cart_total, event_time FROM {target_click_table} ORDER BY event_time DESC LIMIT 20"))

# COMMAND ----------
# 7. TRẢ VỀ KẾT QUẢ CHO MASTER ORCHESTRATOR
import json
try:
    txn_count = spark.table(target_txn_table).count()
    click_count = spark.table(target_click_table).count()
    result_payload = {
        "status": "SUCCESS",
        "bronze_transactions": txn_count,
        "bronze_clickstream": click_count
    }
    dbutils.notebook.exit(json.dumps(result_payload))
except Exception:
    pass

