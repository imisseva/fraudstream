# ==============================================================================
# FraudStream: Databricks PySpark Silver Layer (Data Quality & DLQ)
# Đọc Bronze Delta Table -> Chuẩn hóa Timestamp -> Lọc lỗi DLQ -> Làm giàu Silver
# ==============================================================================

# COMMAND ----------
# %md
# # 🥈 FraudStream: Silver Layer & Data Quality Engine
# ### Chuẩn hóa dữ liệu, Kiểm định chất lượng & Dead Letter Queue (DLQ)
#
# ---
# #### 🎯 Nhiệm vụ chính của Tầng Silver:
# 1. **Chuẩn hóa Timestamp (`clean_event_time`):** Sử dụng `coalesce` để parse đồng nhất các định dạng ngày giờ lộn xộn (`dd/MM/yyyy HH:mm`, ISO 8601...).
# 2. **Bộ luật Data Quality (Expectations):**
#    - `amount > 0` và không được `NULL` / `NaN`.
#    - `user_id`, `card_id`, `merchant_id` bắt buộc có giá trị.
#    - `event_time` phải hợp lệ.
# 3. **Phân luồng Dead Letter Queue (DLQ):**
#    - Bản ghi lỗi $\rightarrow$ Cách ly vào bảng `quarantine_transactions` kèm nhãn `rejection_reason`.
# 4. **Khử trùng lặp & Làm giàu đặc trưng (Enrichment):**
#    - Khử bản ghi duplicate theo `transaction_id`.
#    - Thêm các features: `event_date`, `hour_of_day`, `day_of_week`, `is_weekend`, `amount_tier`.
#    - Ghi dữ liệu sạch vào bảng `silver_transactions`.

# COMMAND ----------
# 1. IMPORTS CÁC HÀM CẦN THIẾT
from pyspark.sql.functions import (
    col, coalesce, to_timestamp, to_date, hour, date_format,
    when, isnan, trim, length, current_timestamp, count, lower
)

# COMMAND ----------
# 2. XÁC ĐỊNH CATALOG, SCHEMA & ĐƯỜNG DẪN BẢNG
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

bronze_table = f"{catalog_name}.{schema_name}.bronze_transactions"
silver_table = f"{catalog_name}.{schema_name}.silver_transactions"
dlq_table = f"{catalog_name}.{schema_name}.quarantine_transactions"

silver_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/silver_txn"
dlq_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/dlq_txn"

print(f"📖 Nguồn Bronze: {bronze_table}")
print(f"✅ Đích Silver:  {silver_table}")
print(f"⚠️  Đích DLQ:     {dlq_table}")

# COMMAND ----------
# 3. ĐỌC DỮ LIỆU STREAMING TỪ BẢNG BRONZE
bronze_stream_df = spark.readStream.table(bronze_table)

# COMMAND ----------
# 4. CHUẨN HÓA TIMESTAMP ĐA ĐỊNH DẠNG (TIMESTAMP NORMALIZATION)
# Xử lý các định dạng lỗi do simulator cố ý tạo ra (như 21/10/2026 15:30)
normalized_df = bronze_stream_df.withColumn(
    "clean_event_time",
    coalesce(
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col("event_time"), "dd/MM/yyyy HH:mm"),
        to_timestamp(col("event_time"))
    )
)

# COMMAND ----------
# 5. DATA QUALITY RULES & GẮN NHÃN LÝ DO LỖI (REJECTION REASON)
validated_df = normalized_df.withColumn(
    "rejection_reason",
    when(col("clean_event_time").isNull(), "MALFORMED_TIMESTAMP")
    .when(col("amount").isNull() | isnan(col("amount")) | (col("amount") <= 0), "INVALID_OR_NEGATIVE_AMOUNT")
    .when(col("user_id").isNull() | (length(trim(col("user_id"))) == 0), "MISSING_USER_ID")
    .when(col("card_id").isNull() | (length(trim(col("card_id"))) == 0), "MISSING_CARD_ID")
    .when(col("merchant_id").isNull() | (length(trim(col("merchant_id"))) == 0), "MISSING_MERCHANT_ID")
    .otherwise("VALID")
)

# COMMAND ----------
# 6. NHÁNH 1: DEAD LETTER QUEUE (DLQ / QUARANTINE TABLE)
# Lọc toàn bộ bản ghi lỗi và lưu vết để audit, không làm mất mát dữ liệu
dlq_df = validated_df.filter(col("rejection_reason") != "VALID") \
    .withColumn("quarantined_at", current_timestamp())

dlq_query = dlq_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", dlq_checkpoint) \
    .trigger(availableNow=True) \
    .toTable(dlq_table)

dlq_query.awaitTermination()
print("⚠️ Đã đồng bộ các bản ghi lỗi vào bảng quarantine_transactions (DLQ)!")

# COMMAND ----------
# 7. NHÁNH 2: LÀM GIÀU ĐẶC TRƯNG & GHI VÀO SILVER TABLE
# Lọc bản ghi hợp lệ, khử trùng lặp có Watermark (chống tràn State Store) và sinh features
clean_df = validated_df.filter(col("rejection_reason") == "VALID") \
    .withWatermark("clean_event_time", "1 hour") \
    .dropDuplicates(["transaction_id", "clean_event_time"]) \
    .withColumn("status", lower(trim(col("status")))) \
    .withColumn("event_date", to_date(col("clean_event_time"))) \
    .withColumn("hour_of_day", hour(col("clean_event_time"))) \
    .withColumn("day_of_week", date_format(col("clean_event_time"), "E")) \
    .withColumn("is_weekend", when(col("day_of_week").isin("Sat", "Sun"), 1).otherwise(0)) \
    .withColumn(
        "amount_tier",
        when(col("amount") < 1000000, "LOW")
        .when(col("amount") <= 10000000, "MEDIUM")
        .when(col("amount") <= 20000000, "HIGH")
        .otherwise("VERY_HIGH")
    ) \
    .withColumn("silver_processed_at", current_timestamp())

silver_query = clean_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_checkpoint) \
    .trigger(availableNow=True) \
    .toTable(silver_table)

silver_query.awaitTermination()
print("🎉 Đã làm sạch và nạp thành công dữ liệu vào bảng silver_transactions!")

# COMMAND ----------
# 8. GIÁM SÁT CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY OBSERVABILITY)
total_silver = spark.table(silver_table).count()
total_quarantine = spark.table(dlq_table).count()
total_all = total_silver + total_quarantine
pass_rate = (total_silver / total_all * 100) if total_all > 0 else 0

print(f"📊 TỔNG BẢN GHI ĐÃ XỬ LÝ: {total_all}")
print(f"✅ BẢN GHI HỢP LỆ (SILVER): {total_silver} ({pass_rate:.2f}%)")
print(f"⚠️ BẢN GHI LỖI (DLQ):        {total_quarantine} ({100 - pass_rate:.2f}%)")

# Hiển thị các lý do lỗi phổ biến nhất trong DLQ
print("\n--- PHÂN BỐ CÁC LÝ DO LỖI DỮ LIỆU (TOP REJECTIONS) ---")
display(spark.table(dlq_table).groupBy("rejection_reason").agg(count("*").alias("count")).orderBy(col("count").desc()))

# COMMAND ----------
# 9. XEM TRƯỚC DỮ LIỆU SẠCH TRONG BẢNG SILVER
display(spark.sql(f"SELECT transaction_id, user_id, amount, amount_tier, clean_event_time, hour_of_day, is_weekend, status FROM {silver_table} ORDER BY clean_event_time DESC LIMIT 50"))
