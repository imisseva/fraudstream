# ==============================================================================
# FraudStream: Executive Fraud & Business Intelligence Dashboard
# Phân tích toàn diện chỉ số gian lận, KPI tài chính, rủi ro kênh và giám sát mô hình
# Thích hợp chạy trực tiếp trên Databricks Notebook hoặc chuyển thành Databricks AI/BI Dashboard
# ==============================================================================

# COMMAND ----------
# %md
# # 📊 FraudStream: Executive Lakehouse Fraud & BI Dashboard
# ### Trung tâm giám sát và phân tích gian lận thời gian thực trên nền tảng Delta Lake & Unity Catalog

# COMMAND ----------
# 1. THIẾT LẬP CATALOG VÀ SCHEMA
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

silver_txn_table = f"{catalog_name}.{schema_name}.silver_transactions"
gold_alerts_table = f"{catalog_name}.{schema_name}.gold_fraud_alerts"
silver_session_table = f"{catalog_name}.{schema_name}.silver_session_profiles"
model_perf_table = f"{catalog_name}.{schema_name}.gold_model_performance_history"

print(f"🏢 Lakehouse Schema: {catalog_name}.{schema_name}")

# COMMAND ----------
# %md
# ## 1️⃣ KPI TỔNG QUAN VỀ GIAN LẬN & RỦI RO TÀI CHÍNH (EXECUTIVE KPIS)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Thẻ chỉ số điều hành: Tổng số giao dịch, cảnh báo gian lận, tỷ lệ rủi ro và tổn thất tài chính được ngăn chặn
# MAGIC WITH stats AS (
# MAGIC     SELECT 
# MAGIC         (SELECT count(*) FROM silver_transactions) AS total_transactions,
# MAGIC         (SELECT count(*) FROM gold_fraud_alerts) AS total_fraud_alerts,
# MAGIC         (SELECT coalesce(sum(amount), 0) FROM gold_fraud_alerts) AS total_fraud_amount_vnd,
# MAGIC         (SELECT coalesce(avg(amount), 0) FROM gold_fraud_alerts) AS avg_fraud_ticket_vnd
# MAGIC )
# MAGIC SELECT 
# MAGIC     total_transactions,
# MAGIC     total_fraud_alerts,
# MAGIC     round((total_fraud_alerts / nullif(total_transactions, 0)) * 100, 2) AS fraud_rate_percentage,
# MAGIC     total_fraud_amount_vnd,
# MAGIC     concat(format_number(round(total_fraud_amount_vnd / 1000000, 1), 1), ' Triệu VND') AS total_fraud_prevented,
# MAGIC     concat(format_number(round(avg_fraud_ticket_vnd / 1000000, 2), 2), ' Triệu VND') AS avg_fraud_ticket
# MAGIC FROM stats;

# COMMAND ----------
# %md
# ## 2️⃣ PHÂN BỐ 7 KỊCH BẢN GIAN LẬN (FRAUD SCENARIOS BREAKDOWN)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Tần suất xuất hiện, tổng thiệt hại và điểm rủi ro trung bình của từng kịch bản (Scenarios A -> G)
# MAGIC WITH unnested AS (
# MAGIC     SELECT 
# MAGIC         explode(split(detected_scenarios, ', ')) AS scenario,
# MAGIC         amount,
# MAGIC         fraud_risk_score
# MAGIC     FROM gold_fraud_alerts
# MAGIC )
# MAGIC SELECT 
# MAGIC     scenario AS `Kịch bản gian lận`,
# MAGIC     count(*) AS `Số vụ vi phạm`,
# MAGIC     concat(format_number(round(sum(amount) / 1000000, 1), 1), ' Tr VND') AS `Tổng tiền vi phạm`,
# MAGIC     round(avg(amount), 0) AS `Giá trị TB (VND)`,
# MAGIC     round(avg(fraud_risk_score), 1) AS `Điểm rủi ro TB`,
# MAGIC     round(count(*) * 100.0 / sum(count(*)) over(), 2) AS `Tỷ trọng (%)`
# MAGIC FROM unnested
# MAGIC WHERE scenario IS NOT NULL AND length(trim(scenario)) > 0
# MAGIC GROUP BY scenario
# MAGIC ORDER BY `Số vụ vi phạm` DESC;

# COMMAND ----------
# %md
# ## 3️⃣ XU HƯỚNG THEO THỜI GIAN & ĐỘT BIẾN THEO GIỜ (TIME-SERIES SPIKES)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Giám sát nhịp độ giao dịch và cảnh báo theo từng giờ (Hourly Trend & Spikes)
# MAGIC SELECT 
# MAGIC     date_format(event_time, 'yyyy-MM-dd HH:00') AS hour_bucket,
# MAGIC     count(*) AS total_fraud_events,
# MAGIC     concat(format_number(round(sum(amount) / 1000000, 1), 1), ' Tr VND') AS hourly_fraud_volume,
# MAGIC     round(avg(fraud_risk_score), 1) AS avg_hourly_risk
# MAGIC FROM gold_fraud_alerts
# MAGIC GROUP BY date_format(event_time, 'yyyy-MM-dd HH:00')
# MAGIC ORDER BY hour_bucket DESC
# MAGIC LIMIT 24;

# COMMAND ----------
# %md
# ## 4️⃣ PHÂN TÍCH RỦI RO THEO KÊNH THANH TOÁN (CHANNEL RISK)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- So sánh mức độ rủi ro giữa các kênh: Web, Mobile App và POS
# MAGIC SELECT 
# MAGIC     s.channel AS `Kênh thanh toán`,
# MAGIC     count(s.transaction_id) AS `Tổng giao dịch`,
# MAGIC     count(g.alert_id) AS `Số vụ gian lận`,
# MAGIC     round(count(g.alert_id) * 100.0 / nullif(count(s.transaction_id), 0), 2) AS `Tỷ lệ gian lận (%)`,
# MAGIC     concat(format_number(round(coalesce(sum(g.amount), 0) / 1000000, 1), 1), ' Tr VND') AS `Tổng tiền rủi ro`
# MAGIC FROM silver_transactions s
# MAGIC LEFT JOIN gold_fraud_alerts g ON s.transaction_id = g.transaction_id
# MAGIC GROUP BY s.channel
# MAGIC ORDER BY `Tỷ lệ gian lận (%)` DESC;

# COMMAND ----------
# %md
# ## 5️⃣ PHÂN TÍCH RỦI RO QUỐC TẾ (GEOGRAPHIC FRAUD DISTRIBUTION)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Phân bố gian lận theo quốc gia (Địa bàn nội địa VN vs Nước ngoài do Impossible Travel)
# MAGIC SELECT 
# MAGIC     country AS `Quốc gia`,
# MAGIC     count(*) AS `Số cảnh báo`,
# MAGIC     concat(format_number(round(sum(amount) / 1000000, 1), 1), ' Tr VND') AS `Tổng tiền gian lận`,
# MAGIC     round(avg(amount), 0) AS `Giá trị trung bình (VND)`,
# MAGIC     round(count(*) * 100.0 / sum(count(*)) over(), 2) AS `Tỷ trọng (%)`
# MAGIC FROM gold_fraud_alerts
# MAGIC GROUP BY country
# MAGIC ORDER BY `Số cảnh báo` DESC;

# COMMAND ----------
# %md
# ## 6️⃣ TOP MERCHANT BỊ NHẮM MỤC TIÊU NHIỀU NHẤT (TOP TARGETED MERCHANTS)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Top các điểm bán / đơn vị chấp nhận thanh toán bị tin tặc tấn công nhiều nhất
# MAGIC SELECT 
# MAGIC     merchant_id AS `Mã Merchant`,
# MAGIC     count(*) AS `Số vụ gian lận`,
# MAGIC     concat(format_number(round(sum(amount) / 1000000, 1), 1), ' Tr VND') AS `Tổng tiền bị quẹt`,
# MAGIC     round(avg(fraud_risk_score), 1) AS `Điểm rủi ro TB`
# MAGIC FROM gold_fraud_alerts
# MAGIC GROUP BY merchant_id
# MAGIC ORDER BY `Số vụ gian lận` DESC
# MAGIC LIMIT 10;

# COMMAND ----------
# %md
# ## 7️⃣ HÀNH VI BOTNET & PHIÊN DUYỆT WEB BẤT THƯỜNG (CLICKSTREAM CORRELATION)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- So sánh đặc trưng hành vi duyệt web giữa Botnet tự động, Khách hàng lương thiện và Gian lận khác
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN g.alert_id IS NOT NULL AND g.detected_scenarios LIKE '%BOTNET_AUTOMATION%' THEN '🤖 Botnet Automation'
# MAGIC         WHEN g.alert_id IS NOT NULL THEN '⚠️ Gian lận thông thường'
# MAGIC         ELSE '✅ Khách hàng hợp lệ'
# MAGIC     END AS `Phân khúc người dùng`,
# MAGIC     count(s.transaction_id) AS `Số giao dịch`,
# MAGIC     round(avg(p.session_duration_sec), 1) AS `Thời lượng phiên TB (s)`,
# MAGIC     round(avg(p.page_view_count), 1) AS `Lượt xem trang TB`,
# MAGIC     round(avg(p.product_view_count), 1) AS `Lượt xem SP TB`,
# MAGIC     round(avg(p.session_cart_total), 0) AS `Giá trị giỏ hàng TB (VND)`
# MAGIC FROM silver_transactions s
# MAGIC LEFT JOIN silver_session_profiles p ON s.session_id = p.session_id
# MAGIC LEFT JOIN gold_fraud_alerts g ON s.transaction_id = g.transaction_id
# MAGIC WHERE s.channel = 'web'
# MAGIC GROUP BY 1
# MAGIC ORDER BY `Số giao dịch` DESC;

# COMMAND ----------
# %md
# ## 8️⃣ THEO DÕI HIỆU NĂNG MÔ HÌNH THEO THỜI GIAN (MODEL PERFORMANCE & DRIFT OBSERVABILITY)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Lịch sử các lần chạy pipeline và biến thiên các chỉ số Precision, Recall, F1-Score
# MAGIC SELECT 
# MAGIC     date_format(evaluated_at, 'yyyy-MM-dd HH:mm:ss') AS `Thời điểm đánh giá`,
# MAGIC     f1_score_pct AS `F1-Score (%)`,
# MAGIC     precision_pct AS `Precision (%)`,
# MAGIC     recall_pct AS `Recall (%)`,
# MAGIC     accuracy_pct AS `Accuracy (%)`,
# MAGIC     true_positives AS `TP (Bắt trúng)`,
# MAGIC     false_positives AS `FP (Báo nhầm)`,
# MAGIC     false_negatives AS `FN (Bỏ lọt)`,
# MAGIC     total_evaluated AS `Tổng mẫu kiểm thử`
# MAGIC FROM gold_model_performance_history
# MAGIC ORDER BY evaluated_at DESC
# MAGIC LIMIT 20;

# COMMAND ----------
# %md
# ## 9️⃣ FEED CẢNH BÁO GIAN LẬN NGUY CƠ CAO (ACTIONABLE FRAUD ANALYST FEED)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bảng phân công xử lý các giao dịch có độ rủi ro cao nhất cần Fraud Ops can thiệp ngay
# MAGIC SELECT 
# MAGIC     alert_id AS `Mã Cảnh Báo`,
# MAGIC     transaction_id AS `Mã Giao Dịch`,
# MAGIC     user_id AS `Mã Khách Hàng`,
# MAGIC     card_id AS `Mã Thẻ`,
# MAGIC     merchant_id AS `Điểm Bán`,
# MAGIC     concat(format_number(round(amount, 0), 0), ' ', currency) AS `Số Tiền`,
# MAGIC     country AS `Quốc Gia`,
# MAGIC     fraud_risk_score AS `Điểm Rủi Ro`,
# MAGIC     detected_scenarios AS `Kịch Bản Phát Hiện`,
# MAGIC     session_duration_sec AS `Thời Lượng Web (s)`,
# MAGIC     event_time AS `Thời Gian Giao Dịch`
# MAGIC FROM gold_fraud_alerts
# MAGIC ORDER BY fraud_risk_score DESC, event_time DESC
# MAGIC LIMIT 50;
