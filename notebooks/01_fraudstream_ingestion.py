# ==============================================================================
# FraudStream: Databricks PySpark Structured Streaming Ingestion
# Kết nối Aiven Kafka Cloud -> Đọc topic transaction-events -> Hiển thị Real-time
# ==============================================================================

# COMMAND ----------
# 1. IMPORTS & ĐỊNH NGHĨA SCHEMA 16 TRƯỜNG CHUẨN
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col

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

# COMMAND ----------
# 3. ĐỌC STREAMING TỪ TOPIC TRANSACTION-EVENTS (TÍCH HỢP TRUSTSTORE PEM TRỰC TIẾP)
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("kafka.ssl.truststore.type", "PEM") \
    .option("kafka.ssl.truststore.certificates", AIVEN_CA_CERT) \
    .option("subscribe", "transaction-events") \
    .option("startingOffsets", "earliest") \
    .load()

# COMMAND ----------
# 4. PARSE DỮ LIỆU JSON TỪ CỘT 'VALUE'
parsed_transactions_df = kafka_stream_df \
    .selectExpr("CAST(key AS STRING) AS user_key", "CAST(value AS STRING) AS json_value") \
    .select(col("user_key"), from_json(col("json_value"), transaction_schema).alias("data")) \
    .select("data.*")

# COMMAND ----------
# 5. TỰ ĐỘNG TẠO VOLUME LƯU CHECKPOINTS (CHUẨN UNITY CATALOG BẢO MẬT)
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.fraud_checkpoints")
checkpoint_path = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/txn_bronze"
target_table_name = f"{catalog_name}.{schema_name}.bronze_transactions"

print(f"✅ Checkpoint sẵn sàng tại: {checkpoint_path}")
print(f"✅ Target Bronze Delta Table: {target_table_name}")

# COMMAND ----------
# 6. GHI LUỒNG STREAM VÀO DELTA LAKE BẢNG BRONZE (CHUẨN DATABRICKS SERVERLESS)
# Databricks Serverless yêu cầu trigger(availableNow=True) thay vì ProcessingTime vô tận
query = parsed_transactions_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .toTable(target_table_name)

# Chờ Spark xử lý hết dữ liệu có sẵn từ Kafka rồi hoàn thành batch
query.awaitTermination()
print("🚀 Đã nạp thành công toàn bộ dữ liệu từ Kafka vào Delta Bronze!")

# COMMAND ----------
# 7. TRUY VẤN VÀ HIỂN THỊ DỮ LIỆU REAL-TIME TỪ DELTA TABLE
# Chạy cell này bất cứ lúc nào để xem các giao dịch mới nhất được nạp vào
display(spark.sql(f"SELECT * FROM {target_table_name} ORDER BY event_time DESC LIMIT 50"))

