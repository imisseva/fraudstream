# 🛡️ FraudStream: Real-time Financial & Clickstream Fraud Simulator

Hệ thống mô phỏng dữ liệu giao dịch tài chính & hành vi người dùng (Clickstream) theo thời gian thực chuẩn Enterprise, tích hợp **5 kịch bản gian lận thực tế (Scenarios A → E)**, cơ chế cấy lỗi kiểm thử khả năng chịu lỗi của pipeline (**Data Quality Glitches**), và đẩy dữ liệu đa topic vào **Apache Kafka**.

Hệ thống được thiết kế tối ưu làm nguồn dữ liệu mẫu (Data Source) cho các pipeline xử lý dòng (Stream Processing) bằng **Apache Spark Structured Streaming**, **Databricks**, hoặc **Apache Flink**.

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
                       |          Kafka UI (:8080)             |
                       +---------------------------------------+
```

---

## 📂 Cấu trúc thư mục mới

```
fraudstream/
├── Dockerfile                      # Build image Python 3.11-slim
├── .dockerignore
├── docker-compose.yml              # Zookeeper + Kafka + Kafka UI + Simulator
├── requirements.txt
├── README.md
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
└── tests/
    ├── __init__.py
    └── test_simulator.py           # 11 Unit tests bao phủ toàn bộ hệ thống
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

*Lưu ý:* Khi phát sinh gian lận, event có thể kèm thêm trường metadata `"fraud_scenario": "high_value"` (chỉ dùng để benchmark/đánh giá mô hình, không dùng làm input feature trong inference).

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
| **A** | **High Value Transaction** | Amount | Giao dịch số tiền đột biến $\ge 20.000.000$ VND tại các merchant vàng bạc, đá quý, đồ xa xỉ. |
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
- **[`00_run_all_pipeline.py`](file:///d:/my-projects/fraudstream/notebooks/00_run_all_pipeline.py):** Master Pipeline Orchestrator — Tự động kích hoạt toàn bộ luồng Bronze $\rightarrow$ Silver $\rightarrow$ Gold chỉ với **1 cú click**, báo cáo thời gian thực thi và trạng thái từng bước.
- **[`01_fraudstream_ingestion.py`](file:///d:/my-projects/fraudstream/notebooks/01_fraudstream_ingestion.py):** Tầng Bronze — Đọc song song 2 topics `transaction-events` và `clickstream-events` qua SASL/SSL với chứng chỉ PEM của Aiven Cloud, ghi vào Delta tables có checkpoint trên Unity Catalog Volume.
- **[`02_fraudstream_silver_quality.py`](file:///d:/my-projects/fraudstream/notebooks/02_fraudstream_silver_quality.py):** Tầng Silver — Chuẩn hóa timestamp đa định dạng, lọc lỗi vào Dead Letter Queue (`quarantine_transactions`), khử trùng lặp có Watermark (1 giờ), và tổng hợp hành vi duyệt web thành `silver_session_profiles`.
- **[`03_fraudstream_gold_detection.py`](file:///d:/my-projects/fraudstream/notebooks/03_fraudstream_gold_detection.py):** Tầng Gold — Stream-Join giao dịch với session profiles, chạy Window Functions phát hiện 7 kịch bản gian lận, tính `fraud_risk_score`, tích lũy cảnh báo bằng Delta `MERGE INTO`, và tự động xuất ma trận nhầm lẫn (Accuracy, Precision, Recall, F1).
- **[`04_fraud_dashboard_analytics.py`](file:///d:/my-projects/fraudstream/notebooks/04_fraud_dashboard_analytics.py):** Tầng Analytics — Bộ truy vấn SQL phân tích tổng thiệt hại tài chính được bảo vệ, phân bố gian lận theo kênh (Web/Mobile/POS) và quốc gia.

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
Toàn bộ thông tin kết nối Aiven Kafka Cloud đã được nạp tự động qua file `.env`. Bạn chỉ cần chạy:
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

#### Cách 1: Chạy trực tiếp qua Master Notebook (1-Click Run)
1. Mở notebook **[`notebooks/00_run_all_pipeline.py`](file:///d:/my-projects/fraudstream/notebooks/00_run_all_pipeline.py)** trên Databricks.
2. Nhập mật khẩu Kafka (nếu cần) vào widget `kafka_password`.
3. Bấm **Run All**. Notebook sẽ tự động kích hoạt tuần tự:
   - Bước 1: `01_fraudstream_ingestion` (Kafka $\rightarrow$ Bronze)
   - Bước 2: `02_fraudstream_silver_quality` (Bronze $\rightarrow$ Silver & DLQ)
   - Bước 3: `03_fraudstream_gold_detection` (Silver $\rightarrow$ 7 Fraud Scenarios Gold)
   - Bước 4: `04_fraud_dashboard_analytics` (Executive KPI Dashboard)
4. Xem bảng tổng kết thời gian thực thi (Latency) và số lượng bản ghi tại từng tầng.

#### Cách 2: Triển khai Databricks Workflows (Multi-Task Job Schedule)
1. Vào mục **Workflows** trên Databricks $\rightarrow$ Chọn **Create Job**.
2. Nhấn vào biểu tượng ba chấm $\rightarrow$ Chọn **Edit as JSON**.
3. Dán toàn bộ nội dung từ file **[`workflows/databricks_workflow_job.json`](file:///d:/my-projects/fraudstream/workflows/databricks_workflow_job.json)** vào và lưu lại.
4. Hệ thống sẽ tự động tạo sơ đồ Directed Acyclic Graph (DAG) gồm 4 tasks có ràng buộc phụ thuộc rõ ràng, hỗ trợ retry và tự động thông báo khi có lỗi.

---

## 🧪 Chạy Kiểm thử (Unit Tests)

Bộ 14 unit tests kiểm tra toàn diện schema, 5 scenarios, data quality, watermark timestamps và topic routing:
```powershell
python -m pytest tests/ -v
```
