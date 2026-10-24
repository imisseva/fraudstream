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

## 🚨 5 Kịch bản gian lận chi tiết (Fraud Scenarios)

| Scenario | Tên kịch bản | Chiều phân tích | Mô tả hành vi giả lập |
| :--- | :--- | :--- | :--- |
| **A** | **High Value Transaction** | Amount | Giao dịch số tiền đột biến gấp 8x - 25x mức tối đa thông thường của user tại các merchant vàng bạc, đá quý, đồ xa xỉ. |
| **B** | **Transaction Velocity** | Time Window | Cùng một thẻ phát sinh liên tiếp 8 - 12 giao dịch chỉ trong vòng dưới 60 giây (burst quẹt thẻ tự động). |
| **C** | **Geographic Anomaly** | Location / IP | Cùng một thẻ xuất hiện ở 2 quốc gia cách xa nhau trong khoảng thời gian không thể di chuyển ("Impossible Travel", ví dụ: vừa ở VN 5 phút sau đã ở US). |
| **D** | **Device / IP Sharing** | Device / Network | Một thiết bị (`device_id`) hoặc 1 IP duy nhất thực hiện thanh toán cho nhiều `user_id` và `card_id` hoàn toàn khác nhau (mô hình Fraud Ring / thiết bị nuôi clone). |
| **E** | **Repeated Attempts** | Behavior / Status | Một thẻ thực hiện liên tiếp 3 - 5 giao dịch bị từ chối (`status="failed"`), sau đó xuất hiện 1 giao dịch thành công giá trị lớn (`status="success"`). |

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

## 🧪 Chạy Kiểm thử (Unit Tests)

Bộ 14 unit tests kiểm tra toàn diện schema, 5 scenarios, data quality, watermark timestamps và topic routing:
```powershell
python -m pytest tests/ -v
```
