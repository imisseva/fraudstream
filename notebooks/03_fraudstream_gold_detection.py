# ==============================================================================
# FraudStream: Databricks PySpark Gold Layer (Fraud Detection Engine)
# Đọc Silver Cleaned Table -> Phát hiện 5 Kịch bản Gian lận -> Bảng Gold Alerts
# ==============================================================================

# COMMAND ----------
# %md
# # 🥇 FraudStream: Gold Layer & Real-time Fraud Detection Engine
# ### Phát hiện 5 kịch bản gian lận tài chính & Xuất cảnh báo Real-time
#
# ---
# #### 🔍 5 Kịch bản gian lận được phát hiện tự động:
# 1. **Kịch bản A (High-Value Spikes):** Giao dịch có giá trị đột biến vượt ngưỡng ($\ge 20.000.000$ VND).
# 2. **Kịch bản B (Velocity Attack):** Cùng 1 tài khoản thực hiện $\ge 3$ giao dịch liên tiếp trong vòng 5 phút.
# 3. **Kịch bản C (Geographic Anomaly / Impossible Travel):** Cùng 1 user giao dịch tại 2 quốc gia khác nhau trong thời gian ngắn ($< 15$ phút).
# 4. **Kịch bản D (Device/IP Sharing Ring):** Một thiết bị (`device_id`) hoặc IP bị sử dụng chung bởi nhiều `user_id` / `card_id` khác nhau.
# 5. **Kịch bản E (Repeated Failed Attempts):** Thẻ bị thử lại liên tiếp sau khi bị từ chối (`DECLINED`/`FAILED`).

# COMMAND ----------
# 1. IMPORTS CÁC HÀM CẦN THIẾT
from pyspark.sql.functions import (
    col, when, lit, concat_ws, current_timestamp,
    lag, count, countDistinct, sum as spark_sum, unix_timestamp, abs as spark_abs,
    round as spark_round
)
from pyspark.sql.window import Window

# COMMAND ----------
# 2. KHAI BÁO BẢNG NGUỒN SILVER & BẢNG ĐÍCH GOLD
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

silver_table = f"{catalog_name}.{schema_name}.silver_transactions"
gold_alerts_table = f"{catalog_name}.{schema_name}.gold_fraud_alerts"
gold_checkpoint = f"/Volumes/{catalog_name}/{schema_name}/fraud_checkpoints/gold_alerts"

print(f"📖 Nguồn Silver: {silver_table}")
print(f"🚨 Đích Gold Alerts: {gold_alerts_table}")

# COMMAND ----------
# 3. ĐỌC DỮ LIỆU SẠCH TỪ SILVER TABLE
silver_df = spark.table(silver_table)

# COMMAND ----------
# 4. TRIỂN KHAI WINDOWS ĐỂ PHÁT HIỆN TỪNG KỊCH BẢN GIAN LẬN

# Window theo user sắp xếp theo thời gian để kiểm tra Velocity & Impossible Travel
user_time_window = Window.partitionBy("user_id").orderBy(col("clean_event_time").cast("long"))
user_range_5min = Window.partitionBy("user_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-300, 0)

# Window theo device để kiểm tra vòng chia sẻ thiết bị (Device Sharing)
device_range_10min = Window.partitionBy("device_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-600, 0)

# Window theo card để kiểm tra lặp lại giao dịch thất bại
card_range_3min = Window.partitionBy("card_id").orderBy(col("clean_event_time").cast("long")).rangeBetween(-180, 0)

# COMMAND ----------
# 5. TÍNH TOÁN CÁC ĐẶC TRƯNG HÀNH VI (BEHAVIORAL FEATURES)
features_df = silver_df \
    .withColumn("prev_country", lag("country", 1).over(user_time_window)) \
    .withColumn("prev_event_time", lag("clean_event_time", 1).over(user_time_window)) \
    .withColumn(
        "time_diff_minutes_prev_country",
        spark_abs(col("clean_event_time").cast("long") - col("prev_event_time").cast("long")) / 60
    ) \
    .withColumn("user_txn_count_5m", count("transaction_id").over(user_range_5min)) \
    .withColumn("device_txn_count_10m", count("transaction_id").over(device_range_10min)) \
    .withColumn(
        "card_failed_count_3m",
        spark_sum(when(col("status").isin("DECLINED", "FAILED"), 1).otherwise(0)).over(card_range_3min)
    )

# COMMAND ----------
# 6. ĐÁNH GIÁ 5 LUẬT GIAN LẬN (FRAUD RULE ENGINE)
fraud_evaluated_df = features_df \
    .withColumn(
        "flag_scenario_a",
        when(col("amount") >= 20000000, 1).otherwise(0)
    ) \
    .withColumn(
        "flag_scenario_b",
        when(col("user_txn_count_5m") >= 3, 1).otherwise(0)
    ) \
    .withColumn(
        "flag_scenario_c",
        when(
            (col("prev_country").isNotNull()) &
            (col("country") != col("prev_country")) &
            (col("time_diff_minutes_prev_country") <= 15),
            1
        ).otherwise(0)
    ) \
    .withColumn(
        "flag_scenario_d",
        when(col("device_txn_count_10m") >= 3, 1).otherwise(0)
    ) \
    .withColumn(
        "flag_scenario_e",
        when(col("card_failed_count_3m") >= 2, 1).otherwise(0)
    )

# COMMAND ----------
# 7. TÍNH TOÁN RISK SCORE & XÁC ĐỊNH GIAN LẬN
scored_df = fraud_evaluated_df.withColumn(
    "fraud_risk_score",
    (col("flag_scenario_a") * 35) +
    (col("flag_scenario_b") * 30) +
    (col("flag_scenario_c") * 40) +
    (col("flag_scenario_d") * 25) +
    (col("flag_scenario_e") * 20)
).withColumn(
    "detected_scenarios",
    concat_ws(", ",
        when(col("flag_scenario_a") == 1, "HIGH_VALUE_SPIKE").otherwise(None),
        when(col("flag_scenario_b") == 1, "VELOCITY_SPIKE").otherwise(None),
        when(col("flag_scenario_c") == 1, "GEOGRAPHIC_ANOMALY").otherwise(None),
        when(col("flag_scenario_d") == 1, "DEVICE_SHARING").otherwise(None),
        when(col("flag_scenario_e") == 1, "REPEATED_ATTEMPTS").otherwise(None)
    )
).withColumn(
    "is_predicted_fraud",
    when(col("fraud_risk_score") >= 30, 1).otherwise(0)
)

# COMMAND ----------
# 8. LỌC CÁC CẢNH BÁO GIAN LẬN & GHI VÀO BẢNG GOLD ALERTS
alerts_df = scored_df.filter(col("is_predicted_fraud") == 1) \
    .select(
        col("event_id").alias("alert_id"),
        col("transaction_id"),
        col("user_id"),
        col("card_id"),
        col("merchant_id"),
        col("amount"),
        col("currency"),
        col("country"),
        col("clean_event_time").alias("event_time"),
        col("fraud_risk_score"),
        col("detected_scenarios"),
        col("is_fraud").alias("ground_truth_is_fraud"),
        col("fraud_scenario").alias("ground_truth_scenario"),
        current_timestamp().alias("alert_generated_at")
    )

# Ghi vào bảng Gold Delta Table
alerts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(gold_alerts_table)

print(f"🚨 Đã ghi nhận tổng cộng {alerts_df.count()} cảnh báo gian lận vào bảng {gold_alerts_table}!")

# COMMAND ----------
# 9. ĐÁNH GIÁ ĐỘ CHÍNH XÁC CỦA MODEL (MODEL EVALUATION METRICS)
# So sánh nhãn dự đoán (Predicted) với nhãn thực tế từ Simulator (Ground Truth)
confusion_matrix = scored_df.groupBy("is_fraud", "is_predicted_fraud").count()
print("📊 MA TRẬN NHẦM LẪN (CONFUSION MATRIX):")
display(confusion_matrix)

# COMMAND ----------
# 10. TRUY VẤN EXECUTIVE FRAUD KPI DASHBOARD
print("🏆 TOP CẢNH BÁO GIAN LẬN MỚI NHẤT TRONG BẢNG GOLD:")
display(spark.sql(f"""
    SELECT 
        transaction_id,
        user_id,
        amount,
        currency,
        country,
        fraud_risk_score,
        detected_scenarios,
        event_time
    FROM {gold_alerts_table}
    ORDER BY fraud_risk_score DESC, event_time DESC
    LIMIT 20
"""))
