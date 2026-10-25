# ==============================================================================
# FraudStream: Databricks PySpark Silver Layer (Data Quality, DLQ & Clickstream)
# Đọc Bronze Delta Tables -> Chuẩn hóa & Lọc lỗi DLQ -> Làm giàu Silver & Session Profiles
# ==============================================================================

# COMMAND ----------
# %md
# # 🥈 FraudStream: Silver Layer & Data Quality Engine
# ### Chuẩn hóa dữ liệu, Kiểm định chất lượng, DLQ & Phân tích phiên duyệt web
#
# ---
# #### 🎯 Nhiệm vụ chính của Tầng Silver:
# 1. **Chuẩn hóa Timestamp (`clean_event_time`):** Sử dụng `coalesce` để parse đồng nhất các định dạng ngày giờ (`dd/MM/yyyy HH:mm`, ISO 8601...).
# 2. **Bộ luật Data Quality (Expectations):**
#    - `amount > 0` và không được `NULL` / `NaN`.
#    - `user_id`, `card_id`, `merchant_id` bắt buộc có giá trị.
#    - `event_time` phải hợp lệ.
# 3. **Phân luồng Dead Letter Queue (DLQ):** Bản ghi lỗi $\rightarrow$ Cách ly vào bảng `quarantine_transactions`.
# 4. **Khử trùng lặp có Watermark:** Chống rò rỉ bộ nhớ State Store trên Spark Streaming.
# 5. **Làm sạch Clickstream & Tạo Hồ sơ phiên (`silver_session_profiles`):**
#    - Tính thời lượng phiên (`session_duration_sec`), số lượt xem sản phẩm, lượt tìm kiếm, giỏ hàng.
#    - Làm nền tảng cho bài toán **Stream Join phát hiện Botnet & Gian lận giỏ hàng**.

# COMMAND ----------
# 1. IMPORTS CÁC HÀM CẦN THIẾT
from pyspark.sql.functions import (
    col, coalesce, to_timestamp, to_date, hour, date_format,
    when, isnan, trim, length, current_timestamp, count, lower,
    min as spark_min, max as spark_max, sum as spark_sum, first, abs as spark_abs
)

# Cấu hình RocksDB StateStore Provider (quản lý Watermark Deduplication ngoài JVM Heap chống OOM)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# Bật tính năng tự động gộp file nhỏ (Auto-Compaction) và tối ưu hóa ghi Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# COMMAND ----------
# 2. XÁC ĐỊNH CATALOG, SCHEMA & ĐƯỜNG DẪN CÁC BẢNG
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

bronze_txn_table = f"{catalog_name}.{schema_name}.bronze_transactions"
bronze_click_table = f"{catalog_name}.{schema_name}.bronze_clickstream"

silver_txn_table = f"{catalog_name}.{schema_name}.silver_transactions"
silver_click_table = f"{catalog_name}.{schema_name}.silver_clickstream"
silver_session_table = f"{catalog_name}.{schema_name}.silver_session_profiles"

dlq_table = f"{catalog_name}.{schema_name}.quarantine_transactions"

silver_txn_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/silver_txn"
silver_click_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/silver_click"
dlq_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/dlq_txn"

print(f"📖 Nguồn Bronze: {bronze_txn_table} | {bronze_click_table}")
print(f"✅ Đích Silver:  {silver_txn_table} | {silver_click_table} | {silver_session_table}")
print(f"⚠️  Đích DLQ:     {dlq_table}")

# COMMAND ----------
# 3. ĐỌC DỮ LIỆU STREAMING TỪ BẢNG BRONZE TRANSACTIONS
bronze_txn_stream = spark.readStream.table(bronze_txn_table)

# COMMAND ----------
# 4. CHUẨN HÓA TIMESTAMP ĐA ĐỊNH DẠNG (TRANSACTIONS)
normalized_txn = bronze_txn_stream.withColumn(
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
validated_txn = normalized_txn.withColumn(
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
dlq_df = validated_txn.filter(col("rejection_reason") != "VALID") \
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
# 7. NHÁNH 2: LÀM GIÀU ĐẶC TRƯNG GIAO DỊCH & GHI VÀO SILVER_TRANSACTIONS
clean_txn_df = validated_txn.filter(col("rejection_reason") == "VALID") \
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

silver_txn_query = clean_txn_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", silver_txn_checkpoint) \
    .trigger(availableNow=True) \
    .toTable(silver_txn_table)

silver_txn_query.awaitTermination()
print("🎉 Đã làm sạch và nạp thành công dữ liệu vào bảng silver_transactions!")

# COMMAND ----------
# 8. NHÁNH 3: LÀM SẠCH VÀ NẠP CLICKSTREAM VÀO SILVER_CLICKSTREAM
try:
    bronze_click_stream = spark.readStream.table(bronze_click_table)

    clean_click_df = bronze_click_stream \
        .withColumn("clean_event_time", to_timestamp(col("event_time"))) \
        .filter(col("session_id").isNotNull() & col("user_id").isNotNull() & col("clean_event_time").isNotNull()) \
        .withWatermark("clean_event_time", "1 hour") \
        .dropDuplicates(["event_id", "clean_event_time"]) \
        .withColumn("click_date", to_date(col("clean_event_time"))) \
        .withColumn("click_hour", hour(col("clean_event_time"))) \
        .withColumn("silver_processed_at", current_timestamp())

    silver_click_query = clean_click_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", silver_click_checkpoint) \
        .trigger(availableNow=True) \
        .toTable(silver_click_table)

    silver_click_query.awaitTermination()
    print("🌐 Đã làm sạch và nạp thành công hành vi duyệt web vào silver_clickstream!")
except Exception as e:
    print(f"ℹ️ Chưa có bảng bronze_clickstream hoặc chưa có dữ liệu clickstream: {e}")

# COMMAND ----------
# 9. TỔNG HỢP HỒ SƠ PHIÊN DUYỆT WEB (SILVER_SESSION_PROFILES)
# Gom nhóm theo session_id để tính toán các chỉ số hành vi phục vụ phát hiện Botnet
try:
    session_profiles_df = spark.table(silver_click_table) \
        .groupBy("session_id", "user_id") \
        .agg(
            spark_min("clean_event_time").alias("session_start_time"),
            spark_max("clean_event_time").alias("session_end_time"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
            spark_sum(when(col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
            spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_view_count"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            spark_sum(when(col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_count"),
            spark_max("cart_total").alias("session_cart_total"),
            first("ip_address").alias("session_ip"),
            first("device_id").alias("session_device_id"),
            first("city").alias("session_city")
        ) \
        .withColumn(
            "session_duration_sec",
            spark_abs(col("session_end_time").cast("long") - col("session_start_time").cast("long"))
        )

    session_profiles_df.createOrReplaceTempView("new_session_profiles_stage")

    # Tạo bảng Silver Session Profiles nếu chưa có
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {silver_session_table} (
        session_id STRING,
        user_id STRING,
        session_start_time TIMESTAMP,
        session_end_time TIMESTAMP,
        page_view_count LONG,
        search_count LONG,
        product_view_count LONG,
        add_to_cart_count LONG,
        checkout_count LONG,
        session_cart_total DOUBLE,
        session_ip STRING,
        session_device_id STRING,
        session_city STRING,
        session_duration_sec LONG
    ) USING DELTA
    """)

    # Upsert idempotent bằng MERGE INTO thay vì Overwrite toàn bộ bảng
    spark.sql(f"""
    MERGE INTO {silver_session_table} AS target
    USING new_session_profiles_stage AS source
    ON target.session_id = source.session_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """)

    total_sessions = spark.table(silver_session_table).count()
    print(f"👤 Đã đồng bộ hồ sơ phiên duyệt web thành công! Tổng số session profiles hiện có: {total_sessions:,}")
except Exception as e:
    print(f"ℹ️ Bỏ qua tạo session profiles vì chưa có dữ liệu clickstream: {e}")

# COMMAND ----------
# 10. GIÁM SÁT CHẤT LƯỢNG DỮ LIỆU & HIỆU QUẢ WATERMARK DEDUPLICATION
total_bronze_txn = spark.table(bronze_txn_table).count()
total_silver_txn = spark.table(silver_txn_table).count()
total_quarantine = spark.table(dlq_table).count()
total_txn_all = total_silver_txn + total_quarantine

# Số bản ghi bị Watermark dropDuplicates loại bỏ (trùng ID giao dịch trong cửa sổ 1 giờ)
deduped_count = max(0, total_bronze_txn - total_txn_all)
pass_rate = (total_silver_txn / total_txn_all * 100) if total_txn_all > 0 else 0

print("=" * 65)
print(f"📥 TỔNG SỐ BẢN GHI NHẬN TỪ BRONZE:        {total_bronze_txn:,}")
print(f"🧹 BẢN GHI TRÙNG BỊ KHỬ BỞI WATERMARK:    {deduped_count:,}")
print(f"📊 TỔNG GIAO DỊCH DUY NHẤT ĐÃ ĐÁNH GIÁ:   {total_txn_all:,}")
print(f"✅ GIAO DỊCH HỢP LỆ VÀO SILVER:           {total_silver_txn:,} ({pass_rate:.2f}%)")
print(f"⚠️ GIAO DỊCH LỖI VÀO DLQ (QUARANTINE):    {total_quarantine:,} ({100 - pass_rate:.2f}%)")
print("=" * 65)

# Hiển thị các lý do lỗi phổ biến nhất trong DLQ
print("\n--- PHÂN BỐ CÁC LÝ DO LỖI DỮ LIỆU (TOP REJECTIONS) ---")
display(spark.table(dlq_table).groupBy("rejection_reason").agg(count("*").alias("count")).orderBy(col("count").desc()))

# COMMAND ----------
# 11. XEM TRƯỚC DỮ LIỆU TỪ SILVER TABLES
print("--- PREVIEW SILVER TRANSACTIONS ---")
display(spark.sql(f"SELECT transaction_id, user_id, amount, amount_tier, clean_event_time, session_id, status FROM {silver_txn_table} ORDER BY clean_event_time DESC LIMIT 20"))

try:
    print("--- PREVIEW SILVER SESSION PROFILES ---")
    display(spark.sql(f"SELECT session_id, user_id, session_duration_sec, page_view_count, product_view_count, session_cart_total FROM {silver_session_table} ORDER BY session_duration_sec ASC LIMIT 20"))
except Exception:
    pass

# COMMAND ----------
# 12. BẢO TRÌ VÀ TỐI ƯU HÓA LƯU TRỮ DELTA LAKE (OPTIMIZE & Z-ORDER)
try:
    print("⚡ Đang tối ưu hóa lưu trữ Delta Lake (Z-Ordering)...")
    spark.sql(f"OPTIMIZE {silver_txn_table} ZORDER BY (transaction_id, user_id)")
    if spark.catalog.tableExists(silver_click_table):
        spark.sql(f"OPTIMIZE {silver_click_table} ZORDER BY (session_id, user_id)")
    print("✅ Đã hoàn tất Z-Ordering cho bảng silver_transactions và silver_clickstream!")
except Exception as e:
    print(f"ℹ️ Lưu ý bảo trì Delta Lake: {e}")

# COMMAND ----------
# 13. TRẢ VỀ KẾT QUẢ CHO MASTER ORCHESTRATOR
import json
try:
    session_count = spark.table(silver_session_table).count() if spark.catalog.tableExists(silver_session_table) else 0
    result_payload = {
        "status": "SUCCESS",
        "bronze_total": total_bronze_txn,
        "deduped_by_watermark": deduped_count,
        "silver_transactions": total_silver_txn,
        "quarantine_dlq": total_quarantine,
        "pass_rate_pct": round(pass_rate, 2),
        "session_profiles": session_count
    }
    dbutils.notebook.exit(json.dumps(result_payload))
except Exception:
    pass
