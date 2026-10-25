# 🛡️ FraudStream: Real-time Financial & Clickstream Fraud Simulator

FraudStream là dự án portfolio mô phỏng giao dịch tài chính và hành vi clickstream, gửi dữ liệu vào Apache Kafka và xử lý bằng Spark trên Databricks. Simulator tạo **5 kịch bản giao dịch A–E** cùng dữ liệu lỗi có chủ đích; pipeline áp dụng thêm hai luật clickstream F–G. Gold là bộ luật minh họa trên dữ liệu tổng hợp, không phải mô hình machine learning đã huấn luyện hoặc được đánh giá trên dữ liệu thực.

Pipeline hỗ trợ **BATCH** (xử lý dữ liệu hiện có bằng `availableNow`) và **STREAMING** (chạy liên tục qua Bronze → Silver → Gold bằng Spark Structured Streaming).

---

## 📌 Kiến trúc hệ thống (Architecture)

```
+---------------------------------------------------------------------------------------+
|                               FraudStream Simulator                                   |
|                                                                                       |
|  +----------------------------+                     +-------------------------------+ |
|  |     5. Reference Data      |                     |      1. User Pool (1000)      | |
|  | - merchants.json (25)      |                     | - Cards (1-3 cards / user)    | |
|  | - countries.json (15)      |                     | - Spending baselines          | |
|  | - devices.json (13)        |                     | - Normal / Suspicious / Fraud | |
|  | - products.json (50)       |                     +---------------+---------------+ |
|  +--------------+-------------+                                     |                 |
|                 |                                                   v                 |
|                 +-----------------------+---------------------------+                 |
|                                         |                                             |
|                     +-------------------+-------------------+                         |
|                     |                                       |                         |
|                     v                                       v                         |
|      +-------------------------------+       +-------------------------------+        |
|      | 2. Transaction Event Gen      |       | 3. Clickstream Event Gen      |        |
|      | - Normal baseline spend       |       | - Funnel: page_view -> search |        |
|      | - Scenario A: High Value      |       |   -> product_view -> add_cart |        |
|      | - Scenario B: Velocity burst  |       +---------------+---------------+        |
|      | - Scenario C: Geo anomaly     |                       |                        |
|      | - Scenario D: Device sharing  |                       |                        |
|      | - Scenario E: Repeat attempts |                       |                        |
|      +--------------+----------------+                       |                        |
|                     |                                        |                        |
|                     v                                        |                        |
|      +-------------------------------+                       |                        |
|      | 4. Data Quality Injector      |                       |                        |
|      | - Duplicate events            |                       |                        |
|      | - Missing required fields     |                       |                        |
|      | - Corrupt types (amount="abc")|                       |                        |
|      | - Late-arriving timestamps    |                       |                        |
|      | - Invalid timestamp format    |                       |                        |
|      +--------------+----------------+                       |                        |
|                     |                                        |                        |
|                     +-------------------+--------------------+                        |
|                                         |                                             |
|                                         v                                             |
|                           +----------------------------+                              |
|                           | Traffic Pattern Controller |                              |
|                           | (Hourly Dynamic Rate)      |                              |
|                           +-------------+--------------+                              |
|                                         |                                             |
|                                         v                                             |
|                           +----------------------------+                              |
|                           | Kafka Producer (Router)    |                              |
|                           +-------------+--------------+                              |
+-----------------------------------------|---------------------------------------------+
                                          |
                     +--------------------+--------------------+
                     |                                         |
                     v                                         v
        +---------------------------+             +---------------------------+
        | Topic: transaction-events |             | Topic: clickstream-events |
        | (Financial Transactions)  |             | (Browsing Funnel Events)  |
        +-------------+-------------+             +-------------+-------------+
                      \                                         /
                       \                                       /
                        v                                     v
                       +---------------------------------------+
                       |     Aiven Kafka Cloud (external)       |
                       +---------------------------------------+
```

---

## 📂 Cấu trúc thư mục mới

```
fraudstream/
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions tests và Spark smoke checks
├── Dockerfile                      # Build image Python 3.11-slim
├── .dockerignore
├── docker-compose.yml              # Simulator nối tới Kafka Cloud bên ngoài
├── requirements.txt                # Dependencies của simulator
├── requirements-test.txt           # Dependencies cho unit tests và local Spark
├── pyproject.toml                  # Package metadata và dev dependencies
├── databricks.yml                  # Databricks Declarative Automation Bundle
├── conf/                           # Environment configuration templates
├── docs/                           # Architecture and operations runbook
│   ├── dev.yml
│   └── prod.yml
├── resources/                      # Databricks jobs deployed by the bundle
│   └── fraudstream_jobs.yml
├── README.md
│
├── notebooks/                      # Notebooks Medallion trên Databricks Lakehouse
│   ├── 00_run_all_pipeline.py      # Master Orchestrator (1-click trigger & metrics)
│   ├── 01_fraudstream_ingestion.py # Bronze Ingestion (Transactions & Clickstream)
│   ├── 02_fraudstream_silver_quality.py # Silver Quality, RocksDB Dedup & Session Profiles
│   ├── 03_fraudstream_gold_detection.py # Gold rule engine & synthetic-label evaluation
│   ├── 04_fraud_dashboard_analytics.py # Executive Lakehouse Fraud & BI Dashboard
│   └── 05_fraudstream_continuous.py # Job dài hạn Bronze-Silver-Gold
│
├── pipelines/
│   └── dlt_fraudstream_pipeline.py # DLT/Lakeflow thử nghiệm, yêu cầu Bronze đã tồn tại
│
├── workflows/
│   └── databricks_workflow_job.json# Legacy/manual Workflow definition
│
├── simulator/
│   ├── __init__.py
│   ├── config.py                   # Cấu hình tỷ lệ fraud A-E, data quality, topics
│   ├── main.py                     # Entrypoint điều khiển generator loop
│   │
│   ├── models/                     # Data contracts & Schemas
│   │   ├── __init__.py
│   │   ├── user.py                 # User, Card, UserProfile
│   │   ├── merchant.py             # Merchant, MerchantCatalog
│   │   ├── transaction.py          # TransactionEvent (16 fields)
│   │   ├── clickstream.py          # ClickstreamEvent
│   │   ├── product.py              # Product, ProductCatalog
│   │   └── session.py              # User browsing session
│   │
│   ├── scenarios/                  # Logic 5 kịch bản gian lận + Normal
│   │   ├── __init__.py
│   │   ├── normal.py               # Giao dịch thường ngày theo baseline
│   │   ├── high_value.py           # Scenario A: Giao dịch lớn đột biến
│   │   ├── velocity.py             # Scenario B: 8-12 txns/phút trên 1 thẻ
│   │   ├── geographic_anomaly.py   # Scenario C: Impossible travel / multi-country
│   │   ├── device_ip_sharing.py    # Scenario D: Nhiều user chung device/IP
│   │   └── repeated_attempts.py    # Scenario E: Thử failed liên tục -> success
│   │
│   ├── generators/                 # Điều phối sinh dữ liệu
│   │   ├── __init__.py
│   │   ├── user_generator.py       # Tạo User pool kèm thẻ ngân hàng
│   │   ├── transaction_generator.py# Điều phối scenarios & queue burst
│   │   ├── clickstream_generator.py# Sinh sự kiện phễu duyệt web
│   │   ├── data_quality_generator.py# Inject duplicates, corrupted types, late data
│   │   └── traffic_pattern.py      # Điều tiết tốc độ theo giờ trong ngày
│   │
│   ├── producers/
│   │   ├── __init__.py
│   │   └── kafka_producer.py       # Route message tới đúng Kafka topic
│   │
│   ├── data/                       # Reference Data Catalogs
│   │   ├── merchants.json          # 25 merchants (electronics, luxury, travel...)
│   │   ├── countries.json          # 15 quốc gia + IP subnets + risk levels
│   │   ├── devices.json            # 13 profile thiết bị có device_id
│   │   ├── products.json           # 50 sản phẩm thương mại điện tử
│   │   └── cities.json             # 20 thành phố Việt Nam
│   │
│   └── utils/
│       ├── __init__.py
│       ├── timestamps.py           # Tiện ích sinh UTC, late timestamps, bad dates
│       └── random_utils.py         # Tiện ích sinh IP, ID, Gaussian distribution
│
├── fraudstream/
│   ├── databricks_config.py        # Kafka options và Databricks Secrets
│   └── spark_ops.py                # Schema, parsing, quality và fraud scoring
└── tests/
    ├── test_simulator.py           # Unit tests cho simulator
    └── test_spark_ops.py           # Spark tests cho quality và fraud rules
```

---

## 📜 Data Contracts (Event Schemas)

### 1. `transaction` Event (`topic: transaction-events`)
Đúng 100% theo hợp đồng dữ liệu chuẩn phục vụ Fraud Detection:

```json
{
  "event_id": "evt_000001",
  "event_type": "transaction",
  "event_time": "2026-10-21T09:15:30Z",
  "transaction_id": "txn_000001",
  "user_id": "user_102",
  "card_id": "card_501",
  "merchant_id": "mer_001",
  "amount": 12500000.0,
  "currency": "VND",
  "country": "VN",
  "channel": "mobile",
  "payment_method": "card",
  "device_id": "dev_001",
  "ip_address": "103.12.44.187",
  "status": "success",
  "is_fraud": 0
}
```

*Lưu ý:* Event có thể kèm metadata `"fraud_scenario": "high_value"` làm ground truth để benchmark bộ luật; trường này không được dùng làm input khi chấm giao dịch.

### 2. `clickstream` Event (`topic: clickstream-events`)

```json
{
  "event_id": "evt_000002",
  "event_type": "product_view",
  "event_time": "2026-10-21T09:14:10Z",
  "user_id": "user_102",
  "session_id": "sess_a1b2c3d4",
  "device_id": "dev_001",
  "ip_address": "103.12.44.187",
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "product_id": "P001",
  "product_name": "iPhone 16 Pro Max 256GB",
  "category": "electronics",
  "price": 34990000.0
}
```

---

## 🚨 7 Kịch bản gian lận chi tiết (Fraud Scenarios)

| Scenario | Tên kịch bản | Chiều phân tích | Mô tả hành vi phát hiện |
| :--- | :--- | :--- | :--- |
| **A** | **High Value Transaction** | Amount / Merchant | Giao dịch từ $20.000.000$ VND trở lên tại merchant electronics, jewelry hoặc crypto được simulator đánh dấu là nhóm rủi ro cao. |
| **B** | **Transaction Velocity** | Time Window | Cùng một user/thẻ phát sinh $\ge 3$ giao dịch chỉ trong cửa sổ trượt 5 phút (burst quẹt thẻ tự động). |
| **C** | **Geographic Anomaly** | Location / IP | Cùng một thẻ xuất hiện ở 2 quốc gia khác nhau trong thời gian di chuyển không khả thi ("Impossible Travel", $\le 15$ phút). |
| **D** | **Device / IP Sharing** | Device / Network | Một thiết bị (`device_id`) có nhiều `user_id` khác nhau luân phiên giao dịch trong 10 phút (Fraud Ring / farm tài khoản). |
| **E** | **Repeated Attempts** | Behavior / Status | Một thẻ bị từ chối liên tiếp $\ge 2$ lần (`DECLINED`/`FAILED`), sau đó cố gắng thực hiện giao dịch mới. |
| **F** | **Botnet / Headless Automation** | Clickstream Correlation | Giao dịch web nhưng không có phiên duyệt web hoặc thời lượng lướt web $< 5$ giây, $0$ lượt xem sản phẩm. |
| **G** | **Cart Parameter Tampering** | Clickstream Correlation | Giá trị thanh toán thực tế chênh lệch bất thường ($> 2.000.000$ VND) so với tổng giỏ hàng ghi nhận từ Clickstream. |

---

## 🏛️ Kiến trúc Databricks Medallion Lakehouse (Notebooks)

Hệ thống xử lý luồng và phát hiện gian lận được tổ chức chuẩn theo mô hình Medallion Lakehouse trên Databricks:

```
                  +---------------------------+
                  |  Aiven Kafka Cloud Topics |
                  +-------------+-------------+
                                |
               +----------------v----------------+
               |  01_fraudstream_ingestion.py   |  (Bronze Layer)
               |  - bronze_transactions         |
               |  - bronze_clickstream          |
               +----------------+----------------+
                                |
               +----------------v----------------+
               | 02_fraudstream_silver_quality.py|  (Silver Layer)
               |  - Data Quality Rules           |
               |  - quarantine_transactions(DLQ) |
               |  - silver_transactions (Dedup)  |
               |  - silver_session_profiles      |
               +----------------+----------------+
                                |
               +----------------v----------------+
               | 03_fraudstream_gold_detection.py|  (Gold Layer)
               |  - Behavioral Feature Windows   |
               |  - 7 Scenarios Detection Engine |
               |  - Delta MERGE INTO (Upsert)    |
               |  - Confusion Matrix & F1-Score  |
               +----------------+----------------+
                                |
               +----------------v----------------+
               | 04_fraud_dashboard_analytics.py|  (BI & Analytics)
               |  - Executive Fraud KPIs         |
               |  - Risk Distribution & Channels |
               |  - Top Fraud Alerts Feed        |
               +---------------------------------+
```

### Chi tiết các Notebook:
- **`00_run_all_pipeline.py`:** entrypoint nhận `run_mode`; batch gọi các tầng theo thứ tự, streaming chạy notebook dài hạn.
- **`01_fraudstream_ingestion.py`:** batch Kafka → Bronze; lưu raw payload và Kafka topic/partition/offset/timestamp cùng các cột đã parse.
- **`02_fraudstream_silver_quality.py`:** batch Bronze → Silver; quarantine transaction và clickstream lỗi, deduplicate theo event time và cập nhật session profiles.
- **`03_fraudstream_gold_detection.py`:** chạy các luật fraud và MERGE alerts idempotent; metric được tính trên nhãn synthetic do simulator sinh ra.
- **`04_fraud_dashboard_analytics.py`:** truy vấn SQL cho KPI, phân bố scenario, kênh, quốc gia và fraud analyst feed.
- **`05_fraudstream_continuous.py`:** job streaming liên tục; Spark query failures làm Workflow thất bại. Cấu hình email/webhook cảnh báo trong Databricks Job.

Notebook dùng module chung trong thư mục `fraudstream/`. Đồng bộ toàn bộ repo lên Databricks Workspace/Git Folder để module này nằm ở thư mục gốc cạnh `notebooks/`.

DLT trong `pipelines/` là ví dụ tùy chọn đọc các bảng `bronze_transactions` và `bronze_clickstream` đã được tạo sẵn. Nó không phải nguồn Kafka và không nằm trong Workflow chính.

---

## 💥 Các kịch bản dữ liệu lỗi (Data Quality Glitches)

Để kiểm thử độ bền bỉ và khả năng xử lý ngoại lệ của Spark Streaming pipeline, Simulator tự động inject ~2% events bị lỗi:
1. **Duplicate Events**: Gửi lại nguyên vẹn event đã từng phát sinh trong quá khứ để kiểm thử cơ chế Dedup.
2. **Missing Required Fields**: Bỏ khuyết các trường bắt buộc như `card_id`, `amount`, `merchant_id` hoặc `user_id` để kiểm thử Dead Letter Queue (DLQ).
3. **Corrupted Types**: Ép kiểu sai như `amount = "abc"` hoặc `"NaN"`.
4. **Late-arriving Events**: `event_time` lùi về quá khứ 2 giờ đến 24 giờ so với hiện tại để kiểm thử **Spark Watermarking**.
5. **Invalid Timestamp**: `event_time = "2026-99-99T99:99:99Z"` hoặc chuỗi lỗi.

---

## 🚀 Hướng dẫn vận hành

### 1. Khởi động Simulator với Docker Compose (Cloud Kafka):
Tạo `.env` từ `.env.example`, điền thông tin Aiven và tải CA PEM thành `ca.pem` vào repo root. Hai tệp này đã bị Git ignore; không commit credentials hoặc certificate riêng. Sau đó chạy:
```powershell
docker compose up -d
```
Hệ thống sẽ khởi chạy duy nhất container `fraudstream-simulator` (cực kỳ nhẹ, tốn chưa tới 100MB RAM), tự động stream dữ liệu liên tục lên Aiven Kafka Cloud.

#### Theo dõi logs của simulator:
```powershell
docker compose logs -f
```

#### Dừng container:
```powershell
docker compose down
```

---

### 2. Chạy trực tiếp trên Host (không cần Docker):
```powershell
# Chạy stream trực tiếp lên Aiven Kafka Cloud
python -m simulator.main

# Chạy Dry-Run (chạy kiểm thử không gửi Kafka)
python -m simulator.main --dry-run --rate 50 --max-events 200
```

---

### 3. Vận hành Pipeline trên Databricks (Orchestration & Workflows):

#### Cấu hình Databricks một lần
1. Đồng bộ toàn bộ repo vào Databricks Workspace/Git Folder để thư mục `fraudstream/` và `notebooks/` cùng nằm dưới project root.
2. Tạo Secret Scope `fraudstream` và key `aiven-kafka-password`; đặt mật khẩu Aiven trong secret này.
3. Cấu hình environment variables trên job cluster: `BOOTSTRAP_SERVERS`, `SASL_USERNAME`, `SASL_MECHANISM`, `DATABRICKS_SECRET_SCOPE`, `DATABRICKS_KAFKA_PASSWORD_KEY`, `KAFKA_CA_PATH`. CA PEM có thể đặt trong Unity Catalog Volume; `KAFKA_CA_PATH` phải là đường dẫn đọc được từ driver.
4. Với triển khai thủ công, có thể import `workflows/databricks_workflow_job.json`. Với CI/CD, dùng `databricks.yml` và resources trong `resources/`; Databricks Bundle là đường triển khai chính.

#### Chạy batch demo
Chạy Workflow với `run_mode=BATCH` (mặc định). Nó xử lý Kafka → Bronze → Silver → Gold → Analytics rồi kết thúc. Các checkpoint được lưu trong Volume và tái sử dụng khi chạy lại.

#### Chạy streaming liên tục
Chạy Workflow với `run_mode=STREAMING`. Job duy trì các query Bronze/Silver/Gold; dừng run của Job để dừng query một cách có kiểm soát. Không chạy đồng thời BATCH và STREAMING trên cùng catalog/schema. Giữ nguyên checkpoint để resume; để replay từ đầu, dùng schema/đích/checkpoint riêng.

Trong streaming mode, Gold đánh giá mỗi transaction khi nó vào Silver; chất lượng tương quan clickstream phụ thuộc vào việc session profile đã được cập nhật trước thời điểm chấm. Dashboard analytics là notebook batch riêng, có thể chạy định kỳ hoặc chạy thủ công.

#### Triển khai bằng Databricks Bundle
Đặt `workspace_host` và `existing_cluster_id` cho target, rồi chạy từ máy có Databricks CLI:

```powershell
databricks bundle validate -t dev --var="workspace_host=https://<workspace-host>" --var="existing_cluster_id=<cluster-id>" --var="notification_email=<email>" --var="job_owner_group=<workspace-group>"
databricks bundle deploy -t dev --var="workspace_host=https://<workspace-host>" --var="existing_cluster_id=<cluster-id>" --var="notification_email=<email>" --var="job_owner_group=<workspace-group>"
databricks bundle run fraudstream_streaming -t dev
```

Job `fraudstream_batch` và `fraudstream_streaming` được tách riêng để không chạy nhầm hai chế độ trên cùng checkpoint. Cấu hình notification, quyền Job và secret scope trong workspace/CI environment.

#### Smoke test trên Databricks/Aiven
1. Khởi động simulator bằng Docker Compose hoặc host; xác nhận log producer không có delivery/SSL errors.
2. Chạy `run_mode=BATCH`; kiểm tra Bronze giữ `raw_payload`, `kafka_topic`, `kafka_partition`, `kafka_offset`, Silver có dữ liệu hợp lệ và các bản ghi lỗi xuất hiện trong hai quarantine table.
3. Chạy lại batch; xác nhận Kafka/Silver checkpoint không đọc lại offset đã commit và số alert không bị nhân đôi.
4. Chạy `run_mode=STREAMING`; phát thêm simulator events, kiểm tra Bronze/Silver/Gold cập nhật, dừng Workflow, rồi chạy lại để xác nhận resume từ checkpoint.
5. Gửi một payload lỗi trong môi trường thử nghiệm và xác nhận raw payload cùng rejection reason có thể truy vết trong quarantine. Xác nhận Databricks Job notifications nhận cảnh báo khi cố ý làm query thất bại.

---

## 🧪 Chạy Kiểm thử (Unit Tests)

Các test bao gồm simulator, luật Spark, ngưỡng thời gian/giá trị, currency, JSON lỗi, dedup và timestamp trễ. Cần Java 17 để chạy local Spark:
```powershell
python -m pip install -r requirements-test.txt
python -m pytest tests/ -v
```
