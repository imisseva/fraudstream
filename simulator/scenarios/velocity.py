import random
from datetime import datetime, timezone, timedelta
from typing import List
from simulator.models.user import User
from simulator.models.merchant import MerchantCatalog
from simulator.models.transaction import TransactionEvent
from simulator.utils.random_utils import random_id

class VelocityScenario:
    """Scenario B: Transaction Velocity — 8-12 giao dịch dồn dập trong vòng 30-60 giây trên cùng 1 thẻ."""

    def __init__(self, merchant_catalog: MerchantCatalog):
        self.catalog = merchant_catalog

    def generate_burst(self, user: User, burst_count: int = 10) -> List[TransactionEvent]:
        card = user.primary_card
        now = datetime.now(timezone.utc)
        events: List[TransactionEvent] = []

        # Tính toán offset thời gian lùi về quá khứ để sự kiện cuối cùng kết thúc đúng thời điểm 'now'
        # Tránh lỗi Watermark skew trên Spark/Flink do timestamp chạy trước vào tương lai
        time_offsets = [0.0]
        for _ in range(burst_count - 1):
            time_offsets.append(time_offsets[-1] + random.uniform(1.0, 3.5))
        total_duration = time_offsets[-1]
        start_time = now - timedelta(seconds=total_duration)

        # Các giao dịch velocity thường có giá trị nhỏ đến vừa (gift cards, gaming topup)
        for i in range(burst_count):
            merchant = self.catalog.get_random_merchant(category=random.choice(["gaming", "online_retail", "supermarket"]))
            txn_time = start_time + timedelta(seconds=time_offsets[i])
            amount = float(random.choice([50000, 100000, 200000, 500000]))

            event = TransactionEvent(
                event_id=random_id("evt_"),
                event_type="transaction",
                event_time=txn_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                transaction_id=random_id("txn_"),
                user_id=user.user_id,
                card_id=card.card_id,
                merchant_id=merchant.merchant_id,
                amount=amount,
                currency="VND",
                country=user.home_country,
                channel="web",
                payment_method="card",
                device_id=user.device_id,
                ip_address=user.home_ip,
                status="success",
                is_fraud=1,
                fraud_scenario="velocity",
            )
            events.append(event)

        return events

