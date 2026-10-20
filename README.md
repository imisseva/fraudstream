# 🛡️ FraudStream: Real-time Clickstream & Fraud Simulator

Hệ thống giả lập luồng sự kiện clickstream và giao dịch thương mại điện tử theo thời gian thực (Real-time Clickstream & Transaction Simulator), tích hợp cơ chế inject các kịch bản gian lận (Fraud Patterns) và gửi dữ liệu vào Apache Kafka.

---

## 📌 Tổng quan kiến trúc (Architecture)

```
+-------------------------------------------------------------+
|                   FraudStream Simulator                     |
|                                                             |
|  +------------------+    +-------------------------------+  |
|  | User Pool (1000) |    | Session Lifecycle Management  |  |
|  | - 80% Normal     |--->| Landing -> Browsing ->        |  |
|  | - 15% Suspicious |    | Product View -> Cart -> Buy   |  |
|  | - 5% Fraudster   |    +---------------+---------------+  |
|  +------------------+                    |                  |
|                                          v                  |
|  +-------------------+        +----------------------+      |
|  | Fraud Generator   |        |   Event Generator    |      |
|  | - Card testing    |        | - page_view, search  |      |
|  | - Account takeover|        | - product_view       |      |
|  | - Bot attack      |        | - add_to_cart, buy   |      |
|  +---------+---------+        +----------+-----------+      |
|            |                             |                  |
|            +--------------+--------------+                  |
|                           |                                 |
|                           v                                 |
|             +----------------------------+                  |
|             | Traffic Pattern Controller |                  |
|             | (Hourly Dynamic Multiplier)|                  |
|             +-------------+--------------+                  |
|                           |                                 |
|                           v                                 |
|             +----------------------------+                  |
|             |    Kafka Event Producer    |                  |
|             +-------------+--------------+                  |
+---------------------------|---------------------------------+
                            |
                            v
               +---------------------------+
               | Apache Kafka (Port 9092)  |
               | Topic: clickstream-events |
               +-------------+-------------+
                             |
                             v
               +---------------------------+
               |   Kafka UI (Port 8080)    |
               +---------------------------+
```

---

## 📂 Cấu trúc thư mục

```
fraudstream/
├── docker-compose.yml              # Kafka + Zookeeper + Kafka UI
├── requirements.txt                # Python dependencies
├── README.md
│
├── simulator/
│   ├── __init__.py
│   ├── main.py                     # Entrypoint điều khiển simulator
│   ├── config.py                   # Cấu hình traffic, tỷ lệ user & Kafka
│   │
│   ├── models/
│   │   ├── __init__.py
│   │   ├── user.py                 # User profiles (normal, suspicious, fraudster)
│   │   ├── product.py              # Product catalog (50 items)
│   │   └── session.py              # Quản lý trạng thái user session
│   │
│   ├── generators/
│   │   ├── __init__.py
│   │   ├── event_generator.py      # Sinh event theo hành trình mua sắm
│   │   ├── fraud_generator.py      # Inject các hành vi gian lận
│   │   └── traffic_pattern.py      # Điều tiết tốc độ event theo giờ
│   │
│   ├── producers/
│   │   ├── __init__.py
│   │   └── kafka_producer.py       # Kết nối và gửi batch event tới Kafka
│   │
│   └── data/
│       ├── products.json           # 50 sản phẩm mẫu (electronics, gift cards, fashion...)
│       ├── cities.json             # 20 thành phố Việt Nam + dải IP
│       └── devices.json            # 13 profile thiết bị/trình duyệt
│
└── tests/
    ├── __init__.py
    └── test_simulator.py           # Unit tests kiểm tra schemas & logic
```

---

## 📜 Cấu trúc Event (Event Schemas)

Tất cả event được serialize dưới dạng JSON và gửi đến Kafka topic `clickstream-events` với partition key là `user_id`.

### 1. `page_view`
```json
{
  "event_id": "c1f1c6ad-62d0-4217-bf34-31518f8883e8",
  "timestamp": "2026-10-21T02:00:00.123456+00:00",
  "user_id": "user_a1b2c3d4",
  "session_id": "sess_e5f6g7h8i9j0",
  "ip_address": "103.12.44.87",
  "device": {"type": "desktop", "os": "Windows", "browser": "Chrome", "fingerprint": "fp_win_chrome_01"},
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "event_type": "page_view",
  "page": "/",
  "referrer": "google.com"
}
```

### 2. `search`
```json
{
  "event_id": "d2f2c6ad-62d0-4217-bf34-31518f8883e9",
  "timestamp": "2026-10-21T02:00:02.123456+00:00",
  "user_id": "user_a1b2c3d4",
  "session_id": "sess_e5f6g7h8i9j0",
  "ip_address": "103.12.44.87",
  "device": {"type": "desktop", "os": "Windows", "browser": "Chrome", "fingerprint": "fp_win_chrome_01"},
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "event_type": "search",
  "query": "electronics",
  "results_count": 28
}
```

### 3. `product_view`
```json
{
  "event_id": "e3f3c6ad-62d0-4217-bf34-31518f8883ea",
  "timestamp": "2026-10-21T02:00:05.123456+00:00",
  "user_id": "user_a1b2c3d4",
  "session_id": "sess_e5f6g7h8i9j0",
  "ip_address": "103.12.44.87",
  "device": {"type": "desktop", "os": "Windows", "browser": "Chrome", "fingerprint": "fp_win_chrome_01"},
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "event_type": "product_view",
  "product_id": "P001",
  "product_name": "iPhone 16 Pro Max 256GB",
  "category": "electronics",
  "price": 34990000.0
}
```

### 4. `add_to_cart`
```json
{
  "event_id": "f4f4c6ad-62d0-4217-bf34-31518f8883eb",
  "timestamp": "2026-10-21T02:00:08.123456+00:00",
  "user_id": "user_a1b2c3d4",
  "session_id": "sess_e5f6g7h8i9j0",
  "ip_address": "103.12.44.87",
  "device": {"type": "desktop", "os": "Windows", "browser": "Chrome", "fingerprint": "fp_win_chrome_01"},
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "event_type": "add_to_cart",
  "product_id": "P001",
  "product_name": "iPhone 16 Pro Max 256GB",
  "price": 34990000.0,
  "quantity": 1,
  "cart_total": 34990000.0
}
```

### 5. `purchase`
```json
{
  "event_id": "a5f5c6ad-62d0-4217-bf34-31518f8883ec",
  "timestamp": "2026-10-21T02:00:12.123456+00:00",
  "user_id": "user_a1b2c3d4",
  "session_id": "sess_e5f6g7h8i9j0",
  "ip_address": "103.12.44.87",
  "device": {"type": "desktop", "os": "Windows", "browser": "Chrome", "fingerprint": "fp_win_chrome_01"},
  "city": "Ho Chi Minh",
  "user_type": "normal",
  "event_type": "purchase",
  "order_id": "ORD_b4f910a2",
  "items": [
    {"product_id": "P001", "name": "iPhone 16 Pro Max 256GB", "price": 34990000.0, "qty": 1}
  ],
  "total_amount": 34990000.0,
  "payment_method": "credit_card"
}
```

---

## 🚨 Các kịch bản gian lận (Fraud Scenarios)

1. **Card Testing (`card_testing`)**: Fraudster thực hiện liên tiếp các đơn hàng thẻ quà tặng (`gift_cards`) với mệnh giá nhỏ đến lớn (10K - 1M) nhằm kiểm tra thẻ tín dụng đánh cắp còn hoạt động hay không.
2. **Account Takeover (`account_takeover`)**: Tài khoản đột ngột thay đổi IP từ tỉnh thành khác, fingerprint thiết bị lạ, sau đó mua ngay các sản phẩm giá trị cao (iPhone, MacBook, PlayStation...).
3. **Bot Attack (`bot_attack`)**: Bot gửi liên tục các request `product_view` với tần suất cao mà không qua phễu duyệt web thông thường.

---

## 🚀 Hướng dẫn cài đặt & Khởi chạy

### 1. Cài đặt Dependencies

Khuyên dùng Python 3.11+:
```powershell
pip install -r requirements.txt
```

### 2. Khởi động Kafka & Kafka UI bằng Docker Compose

```powershell
docker compose up -d
```
Sau khi khởi động:
- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: `http://localhost:8080` (giao diện web trực quan để xem topic, message, consumer groups)

### 3. Chạy Simulator

#### Chế độ mặc định (gửi vào Kafka):
```powershell
python -m simulator.main
```

#### Chế độ Dry-Run (chạy kiểm thử không cần Kafka Broker):
```powershell
python -m simulator.main --dry-run --rate 50 --max-events 200
```

#### Các tham số tuỳ chỉnh (CLI Options):
- `--bootstrap-servers`: Địa chỉ Kafka broker (mặc định: `localhost:9092`)
- `--topic`: Tên Kafka topic (mặc định: `clickstream-events`)
- `--rate`: Tốc độ sinh event cơ sở (events/giây, mặc định: `20.0`)
- `--users`: Số lượng user giả lập trong pool (mặc định: `1000`)
- `--dry-run`: Chạy thử nghiệm mà không kết nối Kafka
- `--max-events`: Tự động dừng sau N events (thích hợp cho test)

---

## 🧪 Chạy Kiểm thử (Unit Tests)

```powershell
pytest tests/ -v
```

