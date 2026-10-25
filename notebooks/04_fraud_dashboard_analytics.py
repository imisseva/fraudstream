# ==============================================================================
# FraudStream: Executive Fraud & Business Intelligence Dashboard
# Truy vấn phân tích tổng quan các chỉ số gian lận, KPI kinh doanh và hành vi bất thường
# Có thể chạy trực tiếp trên Notebook hoặc copy vào Databricks SQL / Lakeview Dashboard
# ==============================================================================

# COMMAND ----------
# %md
# # 📊 FraudStream: Executive Lakehouse Fraud Dashboard
# ### Trung tâm giám sát và phân tích gian lận thời gian thực trên Delta Lake

# COMMAND ----------
# 1. THIẾT LẬP CATALOG VÀ SCHEMA
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

silver_txn_table = f"{catalog_name}.{schema_name}.silver_transactions"
gold_alerts_table = f"{catalog_name}.{schema_name}.gold_fraud_alerts"
silver_session_table = f"{catalog_name}.{schema_name}.silver_session_profiles"

print(f"🏢 Đang phân tích tại Lakehouse Schema: {catalog_name}.{schema_name}")

# COMMAND ----------
# %md
# ## 1️⃣ KPI TỔNG QUAN VỀ GIAN LẬN & RỦI RO TÀI CHÍNH

# COMMAND ----------
# MAGIC %sql
# MAGIC -- KPI Cards: Tổng giao dịch, số vụ gian lận, tỷ lệ gian lận và tổng tiền thiệt hại ngăn chặn được
# MAGIC WITH stats AS (
# MAGIC     SELECT 
# MAGIC         (SELECT count(*) FROM silver_transactions) AS total_transactions,
# MAGIC         (SELECT count(*) FROM gold_fraud_alerts) AS total_fraud_alerts,
# MAGIC         (SELECT coalesce(sum(amount), 0) FROM gold_fraud_alerts) AS total_fraud_amount_vnd
# MAGIC )
# MAGIC SELECT 
# MAGIC     total_transactions,
# MAGIC     total_fraud_alerts,
# MAGIC     round((total_fraud_alerts / nullif(total_transactions, 0)) * 100, 2) AS fraud_rate_percentage,
# MAGIC     total_fraud_amount_vnd,
# MAGIC     concat(format_number(round(total_fraud_amount_vnd / 1000000, 1), 1), ' Triệu VND') AS formatted_fraud_amount
# MAGIC FROM stats;

# COMMAND ----------
# %md
# ## 2️⃣ PHÂN BỐ CÁC KỊCH BẢN GIAN LẬN (FRAUD SCENARIOS BREAKDOWN)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Thống kê tần suất và tổng giá trị vi phạm theo từng kịch bản gian lận
# MAGIC SELECT 
# MAGIC     scenario,
# MAGIC     count(*) AS incident_count,
# MAGIC     round(sum(amount), 0) AS total_amount_vnd,
# MAGIC     round(avg(fraud_risk_score), 1) AS avg_risk_score
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         explode(split(detected_scenarios, ', ')) AS scenario,
# MAGIC         amount,
# MAGIC         fraud_risk_score
# MAGIC     FROM gold_fraud_alerts
# MAGIC )
# MAGIC WHERE scenario IS NOT NULL AND length(scenario) > 0
# MAGIC GROUP BY scenario
# MAGIC ORDER BY incident_count DESC;

# COMMAND ----------
# %md
# ## 3️⃣ PHÂN TÍCH RỦI RO THEO KÊNH THANH TOÁN (CHANNEL RISK)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- So sánh tỷ lệ gian lận giữa các kênh: Web, Mobile, POS
# MAGIC SELECT 
# MAGIC     s.channel,
# MAGIC     count(s.transaction_id) AS total_txns,
# MAGIC     count(g.alert_id) AS fraud_txns,
# MAGIC     round(count(g.alert_id) * 100.0 / count(s.transaction_id), 2) AS fraud_rate_pct,
# MAGIC     round(coalesce(sum(g.amount), 0), 0) AS fraud_amount_vnd
# MAGIC FROM silver_transactions s
# MAGIC LEFT JOIN gold_fraud_alerts g ON s.transaction_id = g.transaction_id
# MAGIC GROUP BY s.channel
# MAGIC ORDER BY fraud_rate_pct DESC;

# COMMAND ----------
# %md
# ## 4️⃣ PHÂN TÍCH RỦI RO QUỐC TẾ (GEOGRAPHIC FRAUD DISTRIBUTION)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Phân bố giao dịch và gian lận theo quốc gia
# MAGIC SELECT 
# MAGIC     country,
# MAGIC     count(*) AS alert_count,
# MAGIC     round(sum(amount), 0) AS total_amount_vnd,
# MAGIC     round(avg(amount), 0) AS avg_amount_vnd
# MAGIC FROM gold_fraud_alerts
# MAGIC GROUP BY country
# MAGIC ORDER BY alert_count DESC;

# COMMAND ----------
# %md
# ## 5️⃣ HÀNH VI BOTNET & PHIÊN DUYỆT WEB BẤT THƯỜNG (CLICKSTREAM CORRELATION)

# COMMAND ----------
# MAGIC %sql
# MAGIC -- So sánh thời lượng phiên duyệt web giữa giao dịch hợp lệ và botnet/gian lận
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN g.alert_id IS NOT NULL AND g.detected_scenarios LIKE '%BOTNET_AUTOMATION%' THEN 'Botnet Automation'
# MAGIC         WHEN g.alert_id IS NOT NULL THEN 'Other Fraud'
# MAGIC         ELSE 'Legitimate User'
# MAGIC     END AS user_category,
# MAGIC     count(s.transaction_id) AS txn_count,
# MAGIC     round(avg(p.session_duration_sec), 1) AS avg_session_duration_sec,
# MAGIC     round(avg(p.page_view_count), 1) AS avg_page_views,
# MAGIC     round(avg(p.product_view_count), 1) AS avg_product_views
# MAGIC FROM silver_transactions s
# MAGIC LEFT JOIN silver_session_profiles p ON s.session_id = p.session_id
# MAGIC LEFT JOIN gold_fraud_alerts g ON s.transaction_id = g.transaction_id
# MAGIC WHERE s.channel = 'web'
# MAGIC GROUP BY 1
# MAGIC ORDER BY txn_count DESC;

# COMMAND ----------
# %md
# ## 6️⃣ DANH SÁCH CẢNH BÁO MỚI NHẤT & MỨC ĐỘ RỦI RO CAO NHẤT

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Bảng trực quan hóa các cảnh báo rủi ro cao nhất cần Fraud Analyst can thiệp ngay
# MAGIC SELECT 
# MAGIC     alert_id,
# MAGIC     transaction_id,
# MAGIC     user_id,
# MAGIC     card_id,
# MAGIC     amount,
# MAGIC     currency,
# MAGIC     country,
# MAGIC     fraud_risk_score,
# MAGIC     detected_scenarios,
# MAGIC     session_duration_sec,
# MAGIC     event_time
# MAGIC FROM gold_fraud_alerts
# MAGIC ORDER BY fraud_risk_score DESC, event_time DESC
# MAGIC LIMIT 50;

