# ==============================================================================
# FraudStream: Master Pipeline Orchestrator
# Điều phối tự động toàn bộ Data Pipeline trên Databricks Lakehouse:
#   Bước 1: 01_fraudstream_ingestion     (Kafka -> Bronze Transactions & Clickstream)
#   Bước 2: 02_fraudstream_silver_quality (Bronze -> Silver Cleaned, DLQ & Session Profiles)
#   Bước 3: 03_fraudstream_gold_detection (Silver -> 7 Fraud Scenarios Detection & Gold Alerts)
# ==============================================================================

# COMMAND ----------
# %md
# # 🚀 FraudStream: Master Lakehouse Pipeline Orchestrator
# ### Tự động hóa chuỗi xử lý luồng Medallion (Bronze ➔ Silver ➔ Gold) chỉ với 1 cú click.

# COMMAND ----------
# 1. THIẾT LẬP THAM SỐ (WIDGETS & CONFIG)
import time
from datetime import datetime

# Định nghĩa các widget điều khiển
dbutils.widgets.text("kafka_password", "", "Aiven Kafka Password (Optional if set in notebook)")
dbutils.widgets.dropdown("run_mode", "ALL", ["ALL", "INGESTION_ONLY", "SILVER_ONLY", "GOLD_ONLY"], "Chế độ chạy")

kafka_pwd = dbutils.widgets.get("kafka_password")
run_mode = dbutils.widgets.get("run_mode")

print(f"⏰ Pipeline Triggered At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⚙️ Chế độ thực thi:       {run_mode}")

# COMMAND ----------
# 2. HÀM ĐIỀU PHỐI THỰC THI NOTEBOOK KÈM LOGGING & XỬ LÝ LỖI
pipeline_stats = []

def run_pipeline_step(step_name: str, notebook_path: str, timeout_sec: int = 600, arguments: dict = None):
    """Thực thi một notebook trong pipeline và đo lường thời gian thực thi."""
    if arguments is None:
        arguments = {}
    
    print(f"\n{'='*70}")
    print(f"▶️ BẮT ĐẦU BƯỚC: {step_name}")
    print(f"📁 Notebook:     {notebook_path}")
    print(f"{'='*70}")
    
    start_time = time.time()
    status = "SUCCESS"
    error_msg = ""
    
    try:
        # Gọi thực thi notebook con trên Databricks
        result = dbutils.notebook.run(notebook_path, timeout_seconds=timeout_sec, arguments=arguments)
        elapsed = round(time.time() - start_time, 2)
        print(f"✅ HOÀN THÀNH: {step_name} trong {elapsed} giây.")
        pipeline_stats.append({
            "step": step_name,
            "status": "SUCCESS",
            "duration_sec": elapsed,
            "error": ""
        })
    except Exception as e:
        elapsed = round(time.time() - start_time, 2)
        status = "FAILED"
        error_msg = str(e)
        print(f"❌ THẤT BẠI: {step_name} sau {elapsed} giây.")
        print(f"Lỗi: {error_msg}")
        pipeline_stats.append({
            "step": step_name,
            "status": "FAILED",
            "duration_sec": elapsed,
            "error": error_msg
        })
        # Dừng toàn bộ pipeline nếu có bước thất bại
        raise Exception(f"Pipeline dừng khẩn cấp do lỗi tại [{step_name}]: {error_msg}")

# COMMAND ----------
# 3. THỰC THI CÁC BƯỚC TRONG PIPELINE THEO THỨ TỰ
arguments = {"kafka_password": kafka_pwd} if kafka_pwd else {}

pipeline_start = time.time()

# BƯỚC 1: INGESTION (BRONZE)
if run_mode in ("ALL", "INGESTION_ONLY"):
    run_pipeline_step(
        step_name="1. Bronze Ingestion (Kafka -> Bronze)",
        notebook_path="./01_fraudstream_ingestion",
        timeout_sec=600,
        arguments=arguments
    )

# BƯỚC 2: SILVER DATA QUALITY & PROFILES
if run_mode in ("ALL", "SILVER_ONLY"):
    run_pipeline_step(
        step_name="2. Silver Quality & Sessions (Bronze -> Silver & DLQ)",
        notebook_path="./02_fraudstream_silver_quality",
        timeout_sec=600
    )

# BƯỚC 3: GOLD FRAUD DETECTION & MODEL EVALUATION
if run_mode in ("ALL", "GOLD_ONLY"):
    run_pipeline_step(
        step_name="3. Gold Fraud Detection (Silver -> 7 Scenarios Gold Alerts)",
        notebook_path="./03_fraudstream_gold_detection",
        timeout_sec=600
    )

total_duration = round(time.time() - pipeline_start, 2)

# COMMAND ----------
# 4. TỔNG KẾT VÀ BÁO CÁO KẾT QUẢ TOÀN BỘ PIPELINE
print("\n" + "#"*70)
print(f"🎉 TOÀN BỘ PIPELINE ĐÃ HOÀN TẤT TRONG {total_duration} GIÂY!")
print("#"*70)

# Tạo DataFrame tổng kết trạng thái
summary_data = [
    (item["step"], item["status"], item["duration_sec"], item["error"])
    for item in pipeline_stats
]
summary_df = spark.createDataFrame(summary_data, ["Pipeline Step", "Status", "Duration (s)", "Error Details"])
display(summary_df)

# COMMAND ----------
# 5. XEM TỔNG HỢP NHANH CÁC BẢNG LỒNG GHÉP TRONG LAKEHOUSE
catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
schema_name = spark.sql("SELECT current_database()").collect()[0][0]

print(f"📊 TỔNG QUAN DỮ LIỆU TẠI CATALOG: {catalog_name}.{schema_name}")

try:
    bronze_txns = spark.table(f"{catalog_name}.{schema_name}.bronze_transactions").count()
    bronze_clicks = spark.table(f"{catalog_name}.{schema_name}.bronze_clickstream").count()
    silver_txns = spark.table(f"{catalog_name}.{schema_name}.silver_transactions").count()
    quarantine_txns = spark.table(f"{catalog_name}.{schema_name}.quarantine_transactions").count()
    silver_sessions = spark.table(f"{catalog_name}.{schema_name}.silver_session_profiles").count()
    gold_alerts = spark.table(f"{catalog_name}.{schema_name}.gold_fraud_alerts").count()

    print(f"   🥉 Bronze Transactions : {bronze_txns:,} records")
    print(f"   🥉 Bronze Clickstream  : {bronze_clicks:,} records")
    print(f"   🥈 Silver Transactions : {silver_txns:,} clean records")
    print(f"   ⚠️  Quarantine DLQ      : {quarantine_txns:,} rejected records")
    print(f"   👤 Silver Sessions     : {silver_sessions:,} session profiles")
    print(f"   🥇 Gold Fraud Alerts   : {gold_alerts:,} detected alerts")
except Exception as e:
    print(f"ℹ️ Không thể đọc một số bảng (có thể chưa chạy đủ các tầng): {e}")

