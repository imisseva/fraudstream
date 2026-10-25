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
    round as spark_round, upper, lower
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

# Window theo device để kiểm tra vòng chia sẻ thiết bị (Device Sharing Ring)
device_time_window = Window.partitionBy("device_id").orderBy(col("clean_event_time").cast("long"))
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
    .withColumn("prev_device_user", lag("user_id", 1).over(device_time_window)) \
    .withColumn("prev_device_time", lag("clean_event_time", 1).over(device_time_window)) \
    .withColumn(
        "device_user_switch_sec",
        spark_abs(col("clean_event_time").cast("long") - col("prev_device_time").cast("long"))
    ) \
    .withColumn(
        "is_device_user_switch",
        when(
            (col("prev_device_user").isNotNull()) &
            (col("prev_device_user") != col("user_id")) &
            (col("device_user_switch_sec") <= 600),
            1
        ).otherwise(0)
    ) \
    .withColumn("device_user_switches_10m", spark_sum("is_device_user_switch").over(device_range_10min)) \
    .withColumn(
        "card_failed_count_3m",
        spark_sum(when(upper(col("status")).isin("DECLINED", "FAILED"), 1).otherwise(0)).over(card_range_3min)
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
        when(col("device_user_switches_10m") >= 1, 1).otherwise(0)
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
# 8. LỌC CÁC CẢNH BÁO GIAN LẬN & GHI VÀO BẢNG GOLD ALERTS (DELTA MERGE INTO CHUẨN)
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

# Tạo bảng Gold nếu chưa tồn tại
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_alerts_table} (
    alert_id STRING,
    transaction_id STRING,
    user_id STRING,
    card_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    currency STRING,
    country STRING,
    event_time TIMESTAMP,
    fraud_risk_score INT,
    detected_scenarios STRING,
    ground_truth_is_fraud INT,
    ground_truth_scenario STRING,
    alert_generated_at TIMESTAMP
) USING DELTA
""")

# Sử dụng Delta MERGE INTO để tích lũy cảnh báo mới mà không xóa lịch sử cũ
alerts_df.createOrReplaceTempView("new_alerts_stage")

spark.sql(f"""
MERGE INTO {gold_alerts_table} AS target
USING new_alerts_stage AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
    UPDATE SET
        target.fraud_risk_score = source.fraud_risk_score,
        target.detected_scenarios = source.detected_scenarios,
        target.alert_generated_at = source.alert_generated_at
WHEN NOT MATCHED THEN
    INSERT *
""")

total_alerts = spark.table(gold_alerts_table).count()
print(f"🚨 Tích lũy cảnh báo thành công! Tổng số vụ gian lận hiện có trong bảng Gold: {total_alerts}")

# COMMAND ----------
# 9. ĐÁNH GIÁ ĐỘ CHÍNH XÁC TOÀN DIỆN (MODEL EVALUATION METRICS)
# So sánh nhãn dự đoán (Predicted) với nhãn thực tế từ Simulator (Ground Truth)
confusion_matrix = scored_df.groupBy("is_fraud", "is_predicted_fraud").count()
print("📊 MA TRẬN NHẦM LẪN (CONFUSION MATRIX):")
display(confusion_matrix)

# Tính toán các chỉ số Precision, Recall, F1-Score
cm_rows = confusion_matrix.collect()
m = {"TP": 0, "FP": 0, "FN": 0, "TN": 0}
for row in cm_rows:
    actual = row["is_fraud"]
    pred = row["is_predicted_fraud"]
    cnt = row["count"]
    if actual == 1 and pred == 1:
        m["TP"] = cnt
    elif actual == 0 and pred == 1:
        m["FP"] = cnt
    elif actual == 1 and pred == 0:
        m["FN"] = cnt
    elif actual == 0 and pred == 0:
        m["TN"] = cnt

tp, fp, fn, tn = m["TP"], m["FP"], m["FN"], m["TN"]
precision = (tp / (tp + fp) * 100) if (tp + fp) > 0 else 0.0
recall = (tp / (tp + fn) * 100) if (tp + fn) > 0 else 0.0
f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
accuracy = ((tp + tn) / (tp + tn + fp + fn) * 100) if (tp + tn + fp + fn) > 0 else 0.0

print("\n" + "=" * 60)
print("🎯 CHỈ SỐ ĐÁNH GIÁ HIỆU QUẢ PHÁT HIỆN GIAN LẬN:")
print(f"   - True Positives  (Bắt đúng gian lận) : {tp:,}")
print(f"   - False Positives (Báo nhầm người tốt): {fp:,}")
print(f"   - False Negatives (Bỏ lọt gian lận)   : {fn:,}")
print(f"   - True Negatives  (Giao dịch an toàn) : {tn:,}")
print("-" * 60)
print(f"   ⭐ Accuracy  : {accuracy:.2f}%")
print(f"   ⭐ Precision : {precision:.2f}% (Độ tin cậy của cảnh báo)")
print(f"   ⭐ Recall    : {recall:.2f}% (Khả năng phát hiện gian lận)")
print(f"   ⭐ F1-Score  : {f1:.2f}%")
print("=" * 60)

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
